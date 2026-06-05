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
import org.apache.spark.sql.catalyst.plans.logical.ShowBranchesOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._

case class ShowBranchesExec(
    catalog: TableCatalog,
    ident: Identifier) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = ShowBranchesOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case d: LanceDataset => d
      case _ => throw new UnsupportedOperationException("ShowBranches only supports LanceDataset")
    }

    val formatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ZoneId.systemDefault())

    val dataset = Utils.openDatasetBuilder(lanceDataset.readOptions()).build()
    try {
      dataset.branches().list().asScala
        .sortBy(_.getCreateAt)
        .map { branch =>
          new GenericInternalRow(Array[Any](
            UTF8String.fromString(branch.getName),
            UTF8String.fromString(branch.getParentBranch.orElse("")),
            branch.getParentVersion,
            UTF8String.fromString(Instant.ofEpochMilli(branch.getCreateAt * 1000).atZone(
              ZoneId.systemDefault()).format(formatter)),
            branch.getManifestSize))
        }.toSeq
    } finally {
      dataset.close()
    }
  }
}
