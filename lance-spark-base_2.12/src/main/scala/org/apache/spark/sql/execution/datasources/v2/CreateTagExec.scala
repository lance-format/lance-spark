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
import org.apache.spark.sql.catalyst.plans.logical.CreateTagOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.{Dataset, Ref}
import org.lance.spark.{LanceDataset, LanceRuntime, LanceSparkReadOptions}

case class CreateTagExec(
    catalog: TableCatalog,
    ident: Identifier,
    tagName: String,
    version: Option[Any]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = CreateTagOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case lanceDataset: LanceDataset => lanceDataset
      case _ =>
        throw new UnsupportedOperationException("CREATE TAG only supports LanceDataset")
    }

    val readOptions = lanceDataset.readOptions()

    val taggedVersion = {
      val dataset = openDataset(readOptions)
      try {
        version match {
          case Some(v: java.lang.Long) =>
            dataset.tags().create(tagName, Ref.ofMain(v.longValue()))
            v.longValue()
          case Some(v: String) =>
            dataset.tags().create(tagName, Ref.ofTag(v))
            dataset.version()
          case None =>
            val latestVersion = dataset.latestVersion()
            dataset.tags().create(tagName, Ref.ofMain(latestVersion))
            latestVersion
          case Some(other) =>
            throw new IllegalArgumentException(
              s"Unsupported version type: ${other.getClass.getName}")
        }
      } finally {
        dataset.close()
      }
    }

    Seq(new GenericInternalRow(Array[Any](UTF8String.fromString(tagName), taggedVersion)))
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
