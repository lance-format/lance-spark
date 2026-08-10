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
 * Supported WITH options:
 * <ul>
 *   <li>{@code retrain} (boolean): retrain the index from scratch instead of an incremental merge.
 *   <li>{@code num_indices_to_merge} (long): number of delta indices to merge per index; 0 creates
 *       a new delta index.
 * </ul>
 */
case class OptimizeIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: Option[String],
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = OptimizeIndexOutputType.SCHEMA

  private def buildOptions(): OptimizeOptions = {
    val builder = OptimizeOptions.builder()
    val argsMap = args.map(t => (t.name, t)).toMap

    indexName.foreach(name => builder.indexNames(List(name).asJava))
    argsMap.get("retrain").foreach(t => builder.retrain(t.value.asInstanceOf[Boolean]))
    argsMap.get("num_indices_to_merge").foreach(t =>
      builder.numIndicesToMerge(t.value.asInstanceOf[Long].toInt))

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
      dataset.optimizeIndices(options)
    } finally {
      dataset.close()
    }

    Seq(new GenericInternalRow(Array[Any](
      UTF8String.fromString(indexName.getOrElse("<all>")),
      UTF8String.fromString("optimized"))))
  }
}
