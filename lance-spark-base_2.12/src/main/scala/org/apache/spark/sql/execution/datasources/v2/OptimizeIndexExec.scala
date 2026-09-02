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
import org.lance.index.{Index, OptimizeOptions}
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

import java.util.{Collections, Locale}

import scala.collection.JavaConverters._

/** Driver-side execution of ALTER TABLE ... OPTIMIZE INDEX. */
case class LanceOptimizeIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: String,
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = LanceOptimizeIndexOutputType.SCHEMA

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

  private def indexesByName(indexes: Seq[Index]): Seq[Index] = {
    indexes.filter(_.name() == indexName)
  }

  private def coveredLiveFragments(indexes: Seq[Index], liveFragments: Set[Int]): Set[Int] = {
    indexes
      .flatMap(index => index.fragments().orElse(Collections.emptyList[Integer]()).asScala)
      .map(_.intValue())
      .toSet
      .intersect(liveFragments)
  }

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case dataset: LanceDataset => dataset
      case _ =>
        throw new UnsupportedOperationException("OptimizeIndex only supports LanceDataset")
    }

    val dataset = Utils.openDatasetBuilder(lanceDataset.readOptions()).build()
    try {
      val liveFragments = dataset.getFragments.asScala.map(_.getId).toSet
      val before = indexesByName(dataset.getIndexes.asScala.toSeq)
      if (before.isEmpty) {
        throw new IllegalArgumentException(s"Index '$indexName' does not exist")
      }

      val coveredBefore = coveredLiveFragments(before, liveFragments)
      dataset.optimizeIndices(buildOptions())

      val after = indexesByName(dataset.getIndexes.asScala.toSeq)
      val coveredAfter = coveredLiveFragments(after, liveFragments)
      val fragmentsIndexed = (coveredAfter -- coveredBefore).size.toLong

      Seq(new GenericInternalRow(Array[Any](
        UTF8String.fromString(indexName),
        fragmentsIndexed,
        before.size.toLong,
        after.size.toLong)))
    } finally {
      dataset.close()
    }
  }
}
