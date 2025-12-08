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
import org.apache.spark.sql.catalyst.plans.logical.{CompactOutputType, NamedArgument}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.SerializeUtil.{decode, encode}
import org.lance.compaction.{CompactionOptions, CompactionTask, RewriteResult}
import org.lance.spark.{LanceConfig, LanceDataset}
import org.lance.spark.internal.LanceDatasetAdapter
import org.objenesis.strategy.StdInstantiatorStrategy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64

import scala.jdk.CollectionConverters._

case class CompactExec(
    catalog: TableCatalog,
    ident: Identifier,
    args: Seq[NamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = CompactOutputType.SCHEMA

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
        throw new UnsupportedOperationException("Compact only supports for LanceDataset")
    }

    // Build compaction options from arguments
    val options = buildOptions()

    // Plan compaction tasks
    val tasks =
      LanceDatasetAdapter.planCompaction(lanceDataset.config(), options).getCompactionTasks

    // Need not to run compaction if there is no task
    if (tasks.isEmpty) {
      return Seq(new GenericInternalRow(Array[Any](0L, 0L, 0L, 0L)))
    }

    // Run compaction tasks in parallel
    val rdd: org.apache.spark.rdd.RDD[CompactionTaskExecutor] = session.sparkContext.parallelize(
      tasks.asScala.toSeq.map(t => CompactionTaskExecutor.create(lanceDataset.config(), t)),
      tasks.size)
    val result = rdd.map(f => f.execute())
      .collect()
      .map(t => decode[RewriteResult](t))
      .toList
      .asJava

    // Commit compaction results
    val metrics = LanceDatasetAdapter.commitCompaction(lanceDataset.config(), result, options)

    Seq(new GenericInternalRow(
      Array[Any](
        metrics.getFragmentsRemoved,
        metrics.getFragmentsAdded,
        metrics.getFilesRemoved,
        metrics.getFilesAdded)))
  }
}

case class CompactionTaskExecutor(lanceConf: String, task: String) extends Serializable {
  def execute(): String = {
    val res = LanceDatasetAdapter.executeCompaction(
      decode[LanceConfig](lanceConf),
      decode[CompactionTask](task))
    encode(res)
  }
}

object CompactionTaskExecutor {
  def create(lanceConf: LanceConfig, task: CompactionTask): CompactionTaskExecutor = {
    CompactionTaskExecutor(encode(lanceConf), encode(task))
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
