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
import org.lance.compaction.{Compaction, CompactionOptions, CompactionTask, RewriteResult}
import org.lance.spark.{LanceDataset, LanceRuntime, LanceSparkReadOptions}
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

    // Plan compaction tasks
    val tasks = {
      val dataset = openDataset(readOptions)
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
      tasks.asScala.toSeq.map(t => OptimizeTaskExecutor.create(readOptions, t)),
      tasks.size)
    val result = rdd.map(f => f.execute())
      .collect()
      .map(t => decode[RewriteResult](t))
      .toList
      .asJava

    // Commit compaction results
    val metrics = {
      val dataset = openDataset(readOptions)
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

  private def openDataset(readOptions: LanceSparkReadOptions): Dataset = {
    if (readOptions.hasNamespace) {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .namespace(readOptions.getNamespace)
        .tableId(readOptions.getTableId)
        .build()
    } else {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(readOptions.getDatasetUri)
        .readOptions(readOptions.toReadOptions())
        .build()
    }
  }
}

case class OptimizeTaskExecutor(lanceConf: String, task: String) extends Serializable {
  def execute(): String = {
    val readOptions = decode[LanceSparkReadOptions](lanceConf)
    val compactionTask = decode[CompactionTask](task)
    val dataset = if (readOptions.hasNamespace) {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .namespace(readOptions.getNamespace)
        .tableId(readOptions.getTableId)
        .build()
    } else {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(readOptions.getDatasetUri)
        .readOptions(readOptions.toReadOptions())
        .build()
    }
    try {
      val res = compactionTask.execute(dataset)
      encode(res)
    } finally {
      dataset.close()
    }
  }
}

object OptimizeTaskExecutor {
  def create(readOptions: LanceSparkReadOptions, task: CompactionTask): OptimizeTaskExecutor = {
    OptimizeTaskExecutor(encode(readOptions), encode(task))
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
