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
import org.apache.spark.sql.catalyst.plans.logical.LanceShowTagsOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

import scala.collection.JavaConverters._

case class LanceShowTagsExec(
    catalog: TableCatalog,
    ident: Identifier) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = LanceShowTagsOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case d: LanceDataset => d
      case _ => throw new UnsupportedOperationException("ShowTags only supports LanceDataset")
    }

    val dataset = Utils.openDatasetBuilder(lanceDataset.readOptions())
      .initialStorageOptions(lanceDataset.getInitialStorageOptions)
      .build()

    try {
      dataset.tags().list().asScala.map { tag =>
        val branch = if (tag.getBranch.isPresent) {
          UTF8String.fromString(tag.getBranch.get())
        } else {
          null
        }
        val createdAt = if (tag.getCreatedAt.isPresent) {
          tag.getCreatedAt.get().getEpochSecond
        } else {
          null
        }
        val updatedAt = if (tag.getUpdatedAt.isPresent) {
          tag.getUpdatedAt.get().getEpochSecond
        } else {
          null
        }
        new GenericInternalRow(Array[Any](
          UTF8String.fromString(tag.getName),
          branch,
          tag.getVersion,
          createdAt,
          updatedAt,
          tag.getManifestSize))
      }.toSeq
    } finally {
      dataset.close()
    }
  }
}
