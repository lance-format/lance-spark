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
import org.apache.spark.sql.catalyst.plans.logical.{LanceNamedArgument, LanceOptimizeIndexOutputType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.Dataset
import org.lance.index.{Index, OptimizeOptions}
import org.lance.operation.CreateIndex
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

import java.util.{Collections, Locale}

import scala.collection.JavaConverters._

object LanceOptimizeIndexExec {
  private val SystemIndexNames = Set("__lance_frag_reuse", "__lance_mem_wal")

  private def isSystemIndex(indexName: String): Boolean =
    indexName != null && SystemIndexNames.exists(_.equalsIgnoreCase(indexName))
}

/** Driver-side execution of ALTER TABLE ... OPTIMIZE INDEX. */
case class LanceOptimizeIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: String,
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = LanceOptimizeIndexOutputType.SCHEMA

  private case class IndexState(segmentCount: Long)

  private case class IndexDelta(
      fragmentsIndexed: Long,
      segmentsAdded: Long,
      segmentsRemoved: Long)

  private def buildOptions(): OptimizeOptions = {
    val normalizedArgs = args.map(arg => arg.name.toLowerCase(Locale.ROOT) -> arg)
    val duplicateArgs = normalizedArgs.groupBy(_._1).collect {
      case (name, values) if values.size > 1 => name
    }.toSeq.sorted
    if (duplicateArgs.nonEmpty) {
      throw new IllegalArgumentException(
        s"Duplicate OPTIMIZE INDEX options: ${duplicateArgs.mkString(", ")}")
    }

    val argsMap = normalizedArgs.toMap
    val supported = Set("num_indices_to_merge")
    val unsupported = argsMap.keySet.diff(supported).toSeq.sorted
    if (unsupported.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unsupported OPTIMIZE INDEX options: ${unsupported.mkString(", ")}")
    }

    val builder = OptimizeOptions.builder()
      .indexNames(Collections.singletonList(indexName))

    argsMap.get("num_indices_to_merge").foreach { arg =>
      val value = arg.value match {
        case number: java.lang.Long => number.longValue()
        case other =>
          throw new IllegalArgumentException(
            s"num_indices_to_merge must be a non-negative integer, got: $other")
      }
      if (value < 0 || value > Int.MaxValue) {
        throw new IllegalArgumentException(
          s"num_indices_to_merge must be between 0 and ${Int.MaxValue}, got: $value")
      }
      builder.numIndicesToMerge(value.toInt)
    }

    builder.build()
  }

  private def indexState(dataset: Dataset): IndexState = {
    val description = dataset.describeIndices().asScala
      .find(_.getName == indexName)
      .getOrElse(throw new IllegalArgumentException(s"Index '$indexName' does not exist"))
    IndexState(description.getSegments.size().toLong)
  }

  private def requiredFragments(index: Index): Set[Integer] = {
    val fragments = index.fragments()
    if (!fragments.isPresent) {
      throw new IllegalStateException(
        s"Lance index segment '${index.uuid()}' for '$indexName' has no fragment metadata")
    }
    fragments.get().asScala.toSet
  }

  private def indexDelta(dataset: Dataset, beforeVersion: Long, afterVersion: Long): IndexDelta = {
    if (afterVersion <= beforeVersion) {
      throw new IllegalStateException(
        s"OPTIMIZE INDEX '$indexName' produced invalid dataset version change: " +
          s"$beforeVersion -> $afterVersion")
    }

    val maybeTransaction = dataset.readTransaction()
    if (!maybeTransaction.isPresent) {
      throw new IllegalStateException(
        s"Dataset version $afterVersion has no transaction for OPTIMIZE INDEX '$indexName'")
    }

    val transaction = maybeTransaction.get()
    try {
      transaction.operation() match {
        case operation: CreateIndex =>
          val added = operation.getNewIndices.asScala.filter(_.name() == indexName)
          val removed = operation.getRemovedIndices.asScala.filter(_.name() == indexName)
          if (added.isEmpty && removed.isEmpty) {
            throw new IllegalStateException(
              s"Dataset version $afterVersion did not change index '$indexName'")
          }

          val addedFragments = added.flatMap(requiredFragments).toSet
          val removedFragments = removed.flatMap(requiredFragments).toSet
          IndexDelta(
            addedFragments.diff(removedFragments).size.toLong,
            added.size.toLong,
            removed.size.toLong)
        case operation =>
          throw new IllegalStateException(
            s"Dataset version $afterVersion was created by '${operation.name()}', " +
              s"not OPTIMIZE INDEX '$indexName'")
      }
    } finally {
      transaction.close()
    }
  }

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = LanceDataset.requireWritable(catalog.loadTable(ident), "OptimizeIndex")
    if (LanceOptimizeIndexExec.isSystemIndex(indexName)) {
      throw new IllegalArgumentException(s"Cannot optimize system index '$indexName'")
    }
    val options = buildOptions()

    val dataset = Utils.openDatasetBuilder(lanceDataset.readOptions())
      .initialStorageOptions(lanceDataset.getInitialStorageOptions)
      .build()
    try {
      val before = indexState(dataset)
      val beforeVersion = dataset.version()
      dataset.optimizeIndices(options)
      val afterVersion = dataset.version()
      val after = indexState(dataset)

      val delta = if (afterVersion == beforeVersion) {
        if (after != before) {
          throw new IllegalStateException(
            s"Index '$indexName' changed without a new dataset version")
        }
        IndexDelta(0L, 0L, 0L)
      } else {
        indexDelta(dataset, beforeVersion, afterVersion)
      }
      val expectedSegmentsAfter =
        before.segmentCount - delta.segmentsRemoved + delta.segmentsAdded
      if (expectedSegmentsAfter != after.segmentCount) {
        throw new IllegalStateException(
          s"OPTIMIZE INDEX '$indexName' reported an inconsistent segment change: " +
            s"${before.segmentCount} - ${delta.segmentsRemoved} + ${delta.segmentsAdded} != " +
            s"${after.segmentCount}")
      }

      Seq(new GenericInternalRow(Array[Any](
        UTF8String.fromString(indexName),
        delta.fragmentsIndexed,
        before.segmentCount,
        after.segmentCount)))
    } finally {
      dataset.close()
    }
  }
}
