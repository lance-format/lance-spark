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
import org.lance.spark.{LanceDataset, LanceRuntime, LanceSparkReadOptions}

import scala.jdk.CollectionConverters._

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

    val dataset = openDataset(readOptions)
    try {
      val indexes = dataset.getIndexes().asScala.toSeq
      val fieldIdToName = dataset.getLanceSchema().fields().asScala
        .map(f => (java.lang.Integer.valueOf(f.getId), f.getName))
        .toMap

      indexes.map { idx =>
        val fieldIds = idx.fields()
        val fieldNamesArray =
          if (fieldIds == null) {
            null
          } else {
            val names = fieldIds.asScala.map { id =>
              val colName = fieldIdToName.getOrElse(id, id.toString)
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

        new GenericInternalRow(Array[Any](
          UTF8String.fromString(name),
          fieldNamesArray,
          indexTypeUtf8,
          numIndexedFragments,
          numIndexedRows,
          numUnindexedFragments,
          numUnindexedRows))
      }
    } finally {
      dataset.close()
    }
  }

  private def openDataset(readOptions: LanceSparkReadOptions): Dataset = {
    if (readOptions.hasNamespace) {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .namespace(readOptions.getNamespace)
        .readOptions(readOptions.toReadOptions())
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
