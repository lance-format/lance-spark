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
import org.apache.spark.sql.catalyst.plans.logical.DropTagOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.Dataset
import org.lance.spark.{LanceDataset, LanceRuntime, LanceSparkReadOptions}

case class DropTagExec(
    catalog: TableCatalog,
    ident: Identifier,
    tagName: String) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = DropTagOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case lanceDataset: LanceDataset => lanceDataset
      case _ =>
        throw new UnsupportedOperationException("DROP TAG only supports LanceDataset")
    }

    val readOptions = lanceDataset.readOptions()

    val dataset = openDataset(readOptions)
    try {
      dataset.tags().delete(tagName)
    } finally {
      dataset.close()
    }

    Seq(new GenericInternalRow(Array[Any](UTF8String.fromString(tagName))))
  }

  private def openDataset(readOptions: LanceSparkReadOptions): Dataset = {
    if (readOptions.hasNamespace) {
      Dataset.open()
        .allocator(LanceRuntime.allocator())
        .namespace(readOptions.getNamespace)
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
