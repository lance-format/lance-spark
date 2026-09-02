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
import org.apache.spark.sql.catalyst.plans.logical.ShowIndexesOutputType
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.spark.LanceDataset
import org.lance.spark.utils.{FieldPathUtils, Utils}

import scala.collection.JavaConverters._

object ShowIndexesExec {
  private val SystemIndexNames = Set("__lance_frag_reuse", "__lance_mem_wal")

  private def isSystemIndex(indexName: String): Boolean =
    indexName != null && SystemIndexNames.exists(_.equalsIgnoreCase(indexName))
}

/**
 * Physical execution of SHOW INDEXES for Lance datasets.
 *
 * This command lists all indexes defined on the underlying Lance table.
 */
case class ShowIndexesExec(
    catalog: TableCatalog,
    ident: Identifier) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = ShowIndexesOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case ds: LanceDataset => ds
      case _ =>
        throw new UnsupportedOperationException("ShowIndexes only supports LanceDataset")
    }

    val readOptions = lanceDataset.readOptions()

    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      // Group by logical index: one row per name, with every physical segment kept so segment-level
      // metadata can be aggregated.
      val indexes = dataset.getIndexes.asScala.toSeq
        .filterNot(idx => ShowIndexesExec.isSystemIndex(idx.name()))
        .groupBy(_.name())
        .toSeq
        .sortBy(_._1)
      val lanceSchema = dataset.getLanceSchema()

      indexes.map { case (_, indexSegments) =>
        val idx = indexSegments.head
        val fieldIds = idx.fields()
        val fieldNamesArray =
          if (fieldIds == null) {
            null
          } else {
            val names = fieldIds.asScala.map { id =>
              val colName = Option(FieldPathUtils.pathByFieldId(lanceSchema, id))
                .getOrElse(id.toString)
              UTF8String.fromString(colName)
            }
            new GenericArrayData(names.toArray[AnyRef])
          }

        val name = idx.name()
        val stats = dataset.getIndexStatistics(name)
        val indexTypeValue = stats.get("index_type")
        val indexTypeUtf8 =
          if (indexTypeValue == null) {
            null
          } else {
            UTF8String.fromString(indexTypeValue.toString.toLowerCase(java.util.Locale.ROOT))
          }

        def getLong(key: String): java.lang.Long = {
          val value = stats.get(key)
          value match {
            case n: java.lang.Number => java.lang.Long.valueOf(n.longValue())
            case _ => null
          }
        }

        val numIndexedFragments = getLong("num_indexed_fragments")
        val numIndexedRows = getLong("num_indexed_rows")
        val numUnindexedFragments = getLong("num_unindexed_fragments")
        val numUnindexedRows = getLong("num_unindexed_rows")

        // Truncated (not rounded) so it never overstates coverage. Null for empty tables.
        val indexedPercent: java.lang.Double =
          if (numIndexedRows == null || numUnindexedRows == null) {
            null
          } else {
            val total = numIndexedRows.longValue() + numUnindexedRows.longValue()
            if (total <= 0L) {
              null
            } else {
              val percent = 100.0 * numIndexedRows.longValue() / total
              java.lang.Double.valueOf(math.floor(percent * 100.0) / 100.0)
            }
          }

        // Physical segments backing this logical index. Older cores report only `num_indices`.
        val numSegments = {
          val reported = getLong("num_segments")
          if (reported != null) reported else getLong("num_indices")
        }

        // Total across segments, or null when any segment predates index file size tracking: a
        // partial sum would understate the index rather than admit it is unknown.
        val sizeBytes: java.lang.Long = {
          val perSegment = indexSegments.map(segment => segment.getSizeBytes)
          if (perSegment.exists(!_.isPresent)) {
            null
          } else {
            java.lang.Long.valueOf(perSegment.map(_.get.longValue()).sum)
          }
        }

        new GenericInternalRow(Array[Any](
          UTF8String.fromString(name),
          fieldNamesArray,
          indexTypeUtf8,
          numIndexedFragments,
          numIndexedRows,
          numUnindexedFragments,
          numUnindexedRows,
          indexedPercent,
          numSegments,
          sizeBytes))
      }
    } finally {
      dataset.close()
    }
  }
}
