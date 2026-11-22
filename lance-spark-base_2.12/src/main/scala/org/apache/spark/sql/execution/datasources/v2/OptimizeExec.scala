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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{LanceNamedArgument, OptimizeOutputType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.util.LanceSerializeUtil.{decode, encode}
import org.lance.{Dataset, DataStorageVersion}
import org.lance.compaction.{Compaction, CompactionMode, CompactionOptions, CompactionTask, RewriteResult}
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceDataset, LanceSparkReadOptions}
import org.lance.spark.utils.{FileFormatVersionResolver, Utils}

import java.util.Locale

import scala.collection.JavaConverters._

case class OptimizeExec(
    catalog: TableCatalog,
    ident: Identifier,
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = OptimizeOutputType.SCHEMA

  // Lance fragment IDs are unsigned 32-bit integers.
  private val MaxFragmentId = 0xFFFFFFFFL

  private def buildOptions(dataset: Dataset): CompactionOptions = {
    val builder = CompactionOptions.builder()
    val argsMap = args.map(t => (t.name, t)).toMap

    argsMap.get("target_rows_per_fragment").map(t =>
      builder.withTargetRowsPerFragment(longArg(t)))
    argsMap.get("max_rows_per_group").map(t => builder.withMaxRowsPerGroup(longArg(t)))
    argsMap.get("max_bytes_per_file").map(t => builder.withMaxBytesPerFile(longArg(t)))
    argsMap.get("materialize_deletions").map(t =>
      builder.withMaterializeDeletions(booleanArg(t)))
    argsMap.get("materialize_deletions_threshold").map(t =>
      builder.withMaterializeDeletionsThreshold(floatArg(t)))
    argsMap.get("num_threads").map(t => builder.withNumThreads(longArg(t)))
    argsMap.get("batch_size").map(t => builder.withBatchSize(longArg(t)))
    argsMap.get("defer_index_remap").map(t => builder.withDeferIndexRemap(booleanArg(t)))
    argsMap.get("max_source_fragments").map(t => builder.withMaxSourceFragments(longArg(t)))
    argsMap.get("compaction_mode").map(t => builder.withCompactionMode(compactionModeArg(t)))
    argsMap.get("binary_copy_read_batch_bytes").map(t =>
      builder.withBinaryCopyReadBatchBytes(positiveLongArg(t)))
    argsMap.get("max_source_rows").map(t => builder.withMaxSourceRows(positiveLongArg(t)))
    argsMap.get("max_source_bytes").map(t => builder.withMaxSourceBytes(positiveLongArg(t)))
    argsMap.get("excluded_fragment_ids").map(t =>
      builder.withExcludedFragmentIds(fragmentIdsArg(t)))
    // An explicit data_storage_version wins over the version inferred from the dataset.
    argsMap.get("data_storage_version") match {
      case Some(t) => builder.withDataStorageVersion(dataStorageVersionArg(t))
      case None =>
        FileFormatVersionResolver.resolveForCompaction(dataset).ifPresent(v =>
          builder.withDataStorageVersion(v))
    }

    builder.build()
  }

  private def floatArg(arg: LanceNamedArgument): Float = arg.value match {
    case n: java.lang.Number => n.floatValue()
    case other =>
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be a numeric literal, got: $other")
  }

  // Long-only: reject a fractional literal rather than truncating it (longValue would make 2.5 a 2).
  private def longArg(arg: LanceNamedArgument): Long = arg.value match {
    case l: java.lang.Long => l.longValue()
    case other =>
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be an integer literal, got: $other")
  }

  private def positiveLongArg(arg: LanceNamedArgument): Long = {
    val value = longArg(arg)
    if (value <= 0) {
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be greater than 0, got: $value")
    }
    value
  }

  private def stringArg(arg: LanceNamedArgument): String = arg.value match {
    case s: String => s
    case other =>
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be a string literal, got: $other")
  }

  private def compactionModeArg(arg: LanceNamedArgument): CompactionMode = {
    val value = stringArg(arg).trim.toLowerCase(Locale.ROOT)
    CompactionMode.values().find(_.getValue == value).getOrElse(
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be one of " +
          CompactionMode.values().map(m => s"'${m.getValue}'").mkString(", ") +
          s", got: '${stringArg(arg)}'"))
  }

  private def dataStorageVersionArg(arg: LanceNamedArgument): DataStorageVersion = {
    val value = stringArg(arg).trim.toLowerCase(Locale.ROOT)
    DataStorageVersion.values().find(_.toRustString == value).getOrElse(
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be one of " +
          DataStorageVersion.values().map(v => s"'${v.toRustString}'").mkString(", ") +
          s", got: '${stringArg(arg)}'"))
  }

  // Fragment IDs are given as a comma-separated string literal, e.g. '1,2,3', since the
  // OPTIMIZE argument grammar only accepts scalar constants. A single integer is also accepted.
  private def fragmentIdsArg(arg: LanceNamedArgument): java.util.List[java.lang.Long] = {
    def invalid(): Nothing =
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be a comma-separated list of non-negative fragment IDs " +
          s"(e.g. '1,2,3'), got: ${arg.value}")
    val ids = arg.value match {
      case l: java.lang.Long => Seq(l.longValue())
      case s: String =>
        s.split(",", -1).toSeq.map(_.trim).map { id =>
          if (id.isEmpty || !id.forall(Character.isDigit)) invalid()
          try id.toLong
          catch { case _: NumberFormatException => invalid() }
        }
      case _ => invalid()
    }
    if (ids.exists(id => id < 0 || id > MaxFragmentId)) invalid()
    ids.map(java.lang.Long.valueOf).asJava
  }

  private def booleanArg(arg: LanceNamedArgument): Boolean = arg.value match {
    case b: java.lang.Boolean => b.booleanValue()
    case other =>
      throw new IllegalArgumentException(
        s"'${arg.name}' option must be a boolean literal (true/false), got: $other")
  }

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = LanceDataset.requireWritable(catalog.loadTable(ident), "Optimize")

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
    val (options, tasks) = {
      val dataset = Utils.openDatasetBuilder(readOptions)
        .initialStorageOptions(initialStorageOpts.map(_.asJava).orNull)
        .build()
      try {
        val resolvedOptions = buildOptions(dataset)
        (resolvedOptions, Compaction.planCompaction(dataset, resolvedOptions).getCompactionTasks)
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
      val dataset = Utils.openDatasetBuilder(readOptions)
        .initialStorageOptions(initialStorageOpts.map(_.asJava).orNull)
        .build()
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

    val dataset = Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
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
