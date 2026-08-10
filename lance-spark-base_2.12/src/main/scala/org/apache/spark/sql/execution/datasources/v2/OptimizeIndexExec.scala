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
import org.apache.spark.sql.catalyst.plans.logical.{LanceNamedArgument, OptimizeIndexOutputType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.index.OptimizeOptions
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

import scala.collection.JavaConverters._

/**
 * Physical execution of ALTER TABLE ... OPTIMIZE INDEX for Lance datasets.
 *
 * Incrementally merges unindexed (newly-appended) fragments into existing indexes via lance-core's
 * optimizeIndices API. This is distinct from CREATE INDEX, which performs a full distributed
 * rebuild over all fragments. When indexName is empty, all indexes are optimized. This runs on the
 * driver (single node).
 *
 * Supported WITH options (option names are case-insensitive):
 * <ul>
 *   <li>{@code retrain} (boolean, default false): rebuild the index from its source data instead
 *       of an incremental merge. Applies to whichever index is targeted; useful when the data
 *       distribution has shifted. This is still cheaper than dropping and re-creating the index.
 *   <li>{@code num_indices_to_merge} (integer &gt;= 0, default core-defined): number of delta
 *       indices to merge per index; 0 creates a new delta index instead of merging into the base.
 * </ul>
 */
case class OptimizeIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: Option[String],
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = OptimizeIndexOutputType.SCHEMA

  private val RETRAIN = "retrain"
  private val NUM_INDICES_TO_MERGE = "num_indices_to_merge"
  private val ALLOWED_OPTIONS = Set(RETRAIN, NUM_INDICES_TO_MERGE)

  /**
   * Validates the WITH options and builds the core OptimizeOptions. Option names are matched
   * case-insensitively; unknown names, duplicates, wrong value types, and out-of-range integers are
   * rejected rather than silently ignored or reinterpreted.
   */
  private def buildOptions(): OptimizeOptions = {
    val normalized = args.map(arg => (arg.name.toLowerCase, arg))

    val unknown = normalized.map(_._1).filterNot(ALLOWED_OPTIONS.contains)
    if (unknown.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unsupported OPTIMIZE INDEX option(s): ${unknown.distinct.mkString(", ")}. "
          + s"Supported options: ${ALLOWED_OPTIONS.toSeq.sorted.mkString(", ")}")
    }

    val byName = normalized.groupBy(_._1)
    byName.collectFirst {
      case (name, occurrences) if occurrences.size > 1 =>
        throw new IllegalArgumentException(s"Duplicate OPTIMIZE INDEX option: $name")
    }

    val builder = OptimizeOptions.builder()
    indexName.foreach(name => builder.indexNames(List(name).asJava))

    byName.get(RETRAIN).foreach { occurrences =>
      occurrences.head._2.value match {
        case b: java.lang.Boolean => builder.retrain(b)
        case other =>
          throw new IllegalArgumentException(
            s"OPTIMIZE INDEX option '$RETRAIN' expects a boolean, got: $other")
      }
    }

    byName.get(NUM_INDICES_TO_MERGE).foreach { occurrences =>
      occurrences.head._2.value match {
        case n: java.lang.Long =>
          if (n < 0 || n > Int.MaxValue) {
            throw new IllegalArgumentException(
              s"OPTIMIZE INDEX option '$NUM_INDICES_TO_MERGE' must be between 0 and "
                + s"${Int.MaxValue}, got: $n")
          }
          builder.numIndicesToMerge(n.intValue())
        case other =>
          throw new IllegalArgumentException(
            s"OPTIMIZE INDEX option '$NUM_INDICES_TO_MERGE' expects an integer, got: $other")
      }
    }

    builder.build()
  }

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case ds: LanceDataset => ds
      case _ =>
        throw new UnsupportedOperationException("OptimizeIndex only supports LanceDataset")
    }

    val options = buildOptions()
    val readOptions = lanceDataset.readOptions()

    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      // lance-core exact-matches indexNames and treats an empty match set as a successful no-op,
      // so a typo would otherwise be reported as completed maintenance. Validate up front that an
      // explicitly named index exists before optimizing.
      indexName.foreach { name =>
        val existing = dataset.listIndexes().asScala
        if (!existing.contains(name)) {
          throw new IllegalArgumentException(
            s"Index '$name' does not exist on table ${ident.name()}. "
              + s"Existing indexes: ${existing.toSeq.sorted.mkString(", ")}")
        }
      }
      dataset.optimizeIndices(options)
    } finally {
      dataset.close()
    }

    Seq(new GenericInternalRow(Array[Any](
      UTF8String.fromString(indexName.getOrElse("<all>")),
      UTF8String.fromString("optimized"))))
  }
}
