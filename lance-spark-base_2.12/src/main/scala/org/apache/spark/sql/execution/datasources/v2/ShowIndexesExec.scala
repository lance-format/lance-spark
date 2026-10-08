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
import org.lance.index.IndexSegmentStatistics
import org.lance.spark.{LanceDataset, LanceSparkReadOptions}
import org.lance.spark.internal.ExecutorNamespace
import org.lance.spark.utils.{FieldPathUtils, Utils}

import java.util.UUID

import scala.collection.JavaConverters._

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
    val distributed = session.conf
      .get(ShowIndexesExec.DISTRIBUTED_STATISTICS_ENABLED, "false")
      .toBoolean

    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      val indexes = dataset.getIndexes.asScala.toSeq
        .filterNot(idx => IndexUtils.isSystemIndex(idx.name()))
        .groupBy(_.name())
        .toSeq
        .sortBy(_._1)
      val lanceSchema = dataset.getLanceSchema()
      val statisticsByName: Map[String, Seq[IndexSegmentStatistics]] =
        if (distributed && indexes.nonEmpty) {
          val pinnedReadOptions = IndexUtils.pinVersion(readOptions, dataset)
          val (namespaceImpl, namespaceProperties, _, initialStorageOptions) =
            IndexUtils.extractNamespaceInfo(catalog, lanceDataset, readOptions)
          val segments = indexes.flatMap { case (name, indexSegments) =>
            indexSegments.map(segment => (name, segment.uuid()))
          }
          val partitions = math.min(segments.size, session.sparkContext.defaultParallelism)
          session.sparkContext.parallelize(segments, partitions)
            .mapPartitions { work =>
              ShowIndexesExec.collectSegmentStatistics(
                work,
                pinnedReadOptions,
                namespaceImpl,
                namespaceProperties,
                initialStorageOptions)
            }
            .collect()
            .groupBy(_._1)
            .map { case (name, results) => name -> results.map(_._2).toSeq }
        } else {
          Map.empty
        }

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
        val stats =
          if (distributed) {
            dataset.getIndexStatisticsFromSegments(name, statisticsByName(name).asJava)
          } else {
            dataset.getIndexStatistics(name)
          }
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
}

object ShowIndexesExec {
  val DISTRIBUTED_STATISTICS_ENABLED = "spark.lance.indexStatistics.distributed.enabled"

  private def collectSegmentStatistics(
      segments: Iterator[(String, UUID)],
      readOptions: LanceSparkReadOptions,
      namespaceImpl: Option[String],
      namespaceProperties: Option[Map[String, String]],
      initialStorageOptions: Option[Map[String, String]])
      : Iterator[(String, IndexSegmentStatistics)] = {
    if (!segments.hasNext) {
      return Iterator.empty
    }
    val namespace = ExecutorNamespace.acquire(
      readOptions,
      namespaceImpl.orNull,
      namespaceProperties.map(_.asJava).orNull)
    try {
      val dataset = Utils.openDatasetBuilder(readOptions)
        .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
        .build()
      try {
        segments.toVector.groupBy(_._1).iterator.flatMap { case (name, work) =>
          dataset.getIndexSegmentStatistics(name, work.map(_._2).asJava).asScala
            .map(statistics => name -> statistics)
        }.toVector.iterator
      } finally {
        dataset.close()
      }
    } finally {
      namespace.close()
    }
  }
}
