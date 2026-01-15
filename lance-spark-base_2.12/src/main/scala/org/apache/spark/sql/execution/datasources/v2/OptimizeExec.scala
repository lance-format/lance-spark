/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.v2

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{NamedArgument, OptimizeOutputType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.SerializeUtil.{decode, encode}
import org.lance.Dataset
import org.lance.ReadOptions
import org.lance.compaction.{Compaction, CompactionOptions, CompactionTask, RewriteResult}
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceDataset, LanceRuntime, LanceSparkReadOptions}
import org.objenesis.strategy.StdInstantiatorStrategy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64

import scala.jdk.CollectionConverters._

case class OptimizeExec(
    catalog: TableCatalog,
    ident: Identifier,
    args: Seq[NamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = OptimizeOutputType.SCHEMA

  private def buildOptions(): CompactionOptions = {
    val builder = CompactionOptions.builder()
    val argsMap = args.map(t => (t.name, t)).toMap

    argsMap.get("target_rows_per_fragment").map(t =>
      builder.withTargetRowsPerFragment(t.value.asInstanceOf[Long]))
    argsMap.get("max_rows_per_group").map(t =>
      builder.withMaxRowsPerGroup(t.value.asInstanceOf[Long]))
    argsMap.get("max_bytes_per_file").map(t =>
      builder.withMaxBytesPerFile(t.value.asInstanceOf[Long]))
    argsMap.get("materialize_deletions").map(t =>
      builder.withMaterializeDeletions(t.value.asInstanceOf[Boolean]))
    argsMap.get("materialize_deletions_threshold").map(t =>
      builder.withMaterializeDeletionsThreshold(t.value.asInstanceOf[Float]))
    argsMap.get("num_threads").map(t => builder.withNumThreads(t.value.asInstanceOf[Long]))
    argsMap.get("batch_size").map(t => builder.withBatchSize(t.value.asInstanceOf[Long]))
    argsMap.get("defer_index_remap").map(t =>
      builder.withDeferIndexRemap(t.value.asInstanceOf[Boolean]))

    builder.build()
  }

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case lanceDataset: LanceDataset => lanceDataset
      case _ =>
        throw new UnsupportedOperationException("Optimize only supports LanceDataset")
    }

    // Build compaction options from arguments
    val options = buildOptions()
    val readOptions = lanceDataset.readOptions()

    // Get namespace info and initial storage options from catalog/dataset
    val (nsImpl, nsProps, tableId, initialStorageOpts): (
        Option[String],
        Option[Map[String, String]],
        Option[List[String]],
        Option[Map[String, String]]) = catalog match {
      case nsCatalog: BaseLanceNamespaceSparkCatalog =>
        (
          Option(nsCatalog.getNamespaceImpl),
          Option(nsCatalog.getNamespaceProperties).map(_.asScala.toMap),
          Option(readOptions.getTableId).map(_.asScala.toList),
          Option(lanceDataset.getInitialStorageOptions).map(_.asScala.toMap))
      case _ => (None, None, None, None)
    }

    // Plan compaction tasks
    val tasks = {
      val dataset = openDataset(readOptions, nsImpl, nsProps, tableId, initialStorageOpts)
      try {
        Compaction.planCompaction(dataset, options).getCompactionTasks
      } finally {
        dataset.close()
      }
    }

    // Need not to run compaction if there is no task
    if (tasks.isEmpty) {
      return Seq(new GenericInternalRow(Array[Any](0L, 0L, 0L, 0L)))
    }

    // Run compaction tasks in parallel
    val rdd: org.apache.spark.rdd.RDD[OptimizeTaskExecutor] = session.sparkContext.parallelize(
      tasks.asScala.toSeq.map(t =>
        OptimizeTaskExecutor.create(readOptions, t, nsImpl, nsProps, tableId, initialStorageOpts)),
      tasks.size)
    val result = rdd.map(f => f.execute())
      .collect()
      .map(t => decode[RewriteResult](t))
      .toList
      .asJava

    // Commit compaction results
    val metrics = {
      val dataset = openDataset(readOptions, nsImpl, nsProps, tableId, initialStorageOpts)
      try {
        Compaction.commitCompaction(dataset, result, options)
      } finally {
        dataset.close()
      }
    }

    Seq(new GenericInternalRow(
      Array[Any](
        metrics.getFragmentsRemoved,
        metrics.getFragmentsAdded,
        metrics.getFilesRemoved,
        metrics.getFilesAdded)))
  }

  private def openDataset(
      readOptions: LanceSparkReadOptions,
      nsImpl: Option[String],
      nsProps: Option[Map[String, String]],
      tableId: Option[List[String]],
      initialStorageOpts: Option[Map[String, String]]): Dataset = {
    // Build ReadOptions with merged storage options and credential refresh provider
    val merged = LanceRuntime.mergeStorageOptions(
      readOptions.getStorageOptions,
      initialStorageOpts.map(_.asJava).orNull)
    val provider = LanceRuntime.getOrCreateStorageOptionsProvider(
      nsImpl.orNull,
      nsProps.map(_.asJava).orNull,
      tableId.map(_.asJava).orNull)

    val builder = new ReadOptions.Builder().setStorageOptions(merged)
    if (provider != null) {
      builder.setStorageOptionsProvider(provider)
    }

    Dataset.open()
      .allocator(LanceRuntime.allocator())
      .uri(readOptions.getDatasetUri)
      .readOptions(builder.build())
      .build()
  }
}

case class OptimizeTaskExecutor(
    lanceConf: String,
    task: String,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]]) extends Serializable {

  def execute(): String = {
    val readOptions = decode[LanceSparkReadOptions](lanceConf)
    val compactionTask = decode[CompactionTask](task)

    // Build ReadOptions with merged storage options and credential refresh provider
    val merged = LanceRuntime.mergeStorageOptions(
      readOptions.getStorageOptions,
      initialStorageOptions.map(_.asJava).orNull)
    val provider = LanceRuntime.getOrCreateStorageOptionsProvider(
      namespaceImpl.orNull,
      namespaceProperties.map(_.asJava).orNull,
      tableId.map(_.asJava).orNull)

    val builder = new ReadOptions.Builder().setStorageOptions(merged)
    if (provider != null) {
      builder.setStorageOptionsProvider(provider)
    }

    val dataset = Dataset.open()
      .allocator(LanceRuntime.allocator())
      .uri(readOptions.getDatasetUri)
      .readOptions(builder.build())
      .build()

    try {
      val res = compactionTask.execute(dataset)
      encode(res)
    } finally {
      dataset.close()
    }
  }
}

object OptimizeTaskExecutor {
  def create(
      readOptions: LanceSparkReadOptions,
      task: CompactionTask,
      namespaceImpl: Option[String],
      namespaceProperties: Option[Map[String, String]],
      tableId: Option[List[String]],
      initialStorageOptions: Option[Map[String, String]]): OptimizeTaskExecutor = {
    OptimizeTaskExecutor(
      encode(readOptions),
      encode(task),
      namespaceImpl,
      namespaceProperties,
      tableId,
      initialStorageOptions)
  }
}

object SerializeUtil {
  private val kryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo] {
    override def initialValue(): Kryo = {
      val kryo = new Kryo()
      kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()))
      kryo
    }
  }

  def encode[T](obj: T): String = {
    val buffer = new ByteArrayOutputStream()
    val output = new Output(buffer)
    kryo.get.writeClassAndObject(output, obj)
    output.close()
    Base64.getEncoder.encodeToString(buffer.toByteArray)
  }

  def decode[T](obj: String): T = {
    val array = Base64.getDecoder.decode(obj)
    val input = new Input(new ByteArrayInputStream(array))
    val o = kryo.get.readClassAndObject(input)
    input.close()
    o.asInstanceOf[T]
  }
}
