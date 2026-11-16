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
import org.lance.Dataset
import org.lance.index.{Index, IndexCriteria, IndexType}
import org.lance.spark.LanceDataset
import org.lance.spark.utils.{FieldPathUtils, Utils}

import java.util.{Locale, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

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
      val indexes = dataset.getIndexes.asScala.toSeq
        .filterNot(idx => IndexUtils.isSystemIndex(idx.name()))
        .groupBy(_.name())
        .toSeq
        .sortBy(_._1)
      val lanceSchema = dataset.getLanceSchema()
      lazy val fragmentIds = dataset.getFragments.asScala.iterator.map(_.getId).toSet
      lazy val totalRows = dataset.countRows()

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
        val stats = metadataStatistics(dataset, indexSegments, fragmentIds, totalRows)
          .getOrElse(dataset.getIndexStatistics(name))
        val indexTypeValue = stats.get("index_type")
        val indexTypeUtf8 =
          if (indexTypeValue == null) {
            null
          } else {
            UTF8String.fromString(indexTypeValue.toString.toLowerCase(Locale.ROOT))
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

        val indexedPercent: java.lang.Double =
          if (numIndexedRows == null || numUnindexedRows == null) {
            null
          } else {
            val total = numIndexedRows.longValue() + numUnindexedRows.longValue()
            if (total <= 0L) {
              null
            } else {
              val percent = 100.0 * numIndexedRows.longValue() / total
              math.floor(percent * 100.0) / 100.0
            }
          }

        val numSegments = {
          val reported = getLong("num_segments")
          if (reported != null) reported else getLong("num_indices")
        }

        val sizeBytes: java.lang.Long = {
          val perSegment = indexSegments.map(segment => segment.getSizeBytes)
          if (perSegment.exists(!_.isPresent)) {
            null
          } else {
            perSegment.map(_.get.longValue()).sum
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

  private def metadataStatistics(
      dataset: Dataset,
      segments: Seq[Index],
      fragmentIds: => Set[Int],
      totalRows: => Long): Option[JMap[String, Object]] = {
    val coveredFragments = mutable.HashSet.empty[Int]
    val knownCoverage = segments.forall { segment =>
      segment.indexDetails().isPresent && segment.fragments().isPresent &&
      segment.fragments().get().asScala.iterator
        .map(_.intValue())
        .filter(fragmentIds.contains)
        .forall(coveredFragments.add)
    }
    if (!knownCoverage) {
      return None
    }

    val name = segments.head.name()
    val descriptions =
      try {
        dataset.describeIndices(new IndexCriteria.Builder().hasName(name).build()).asScala.toSeq
      } catch {
        case error: RuntimeException =>
          logDebug(s"Falling back to native statistics for index $name", error)
          return None
      }
    descriptions.find(_.getName == name).flatMap { description =>
      val indexType = description.getIndexType
      val concreteType = indexType != null &&
        !indexType.equalsIgnoreCase("Vector") &&
        !indexType.equalsIgnoreCase("Scalar") &&
        IndexType.values().exists(_.name().replace("_", "").equalsIgnoreCase(
          indexType.replace("_", "")))
      val sameSegments = description.getMetadata.asScala.map(_.uuid()).toSet ==
        segments.map(_.uuid()).toSet
      val indexedRows = description.getRowsIndexed
      if (!concreteType || !sameSegments || indexedRows < 0 || indexedRows > totalRows) {
        None
      } else {
        Some(Map[String, Object](
          "index_type" -> indexType,
          "num_indexed_fragments" -> Long.box(coveredFragments.size.toLong),
          "num_indexed_rows" -> Long.box(indexedRows),
          "num_unindexed_fragments" -> Long.box(
            fragmentIds.size.toLong - coveredFragments.size),
          "num_unindexed_rows" -> Long.box(totalRows - indexedRows),
          "num_segments" -> Long.box(segments.size.toLong)).asJava)
      }
    }
  }
}
