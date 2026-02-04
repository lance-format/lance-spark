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
import org.apache.spark.sql.catalyst.plans.logical.{NamedArgument, VacuumOutputType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.lance.Dataset
import org.lance.cleanup.CleanupPolicy
import org.lance.spark.{LanceDataset, LanceRuntime, LanceSparkReadOptions}

import scala.jdk.CollectionConverters._

case class VacuumExec(
    catalog: TableCatalog,
    ident: Identifier,
    args: Seq[NamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = VacuumOutputType.SCHEMA

  private def buildPolicy(): CleanupPolicy = {
    val builder = CleanupPolicy.builder()
    val argsMap = args.map(t => (t.name, t)).toMap

    argsMap.get("before_version").map(t => builder.withBeforeVersion(t.value.asInstanceOf[Long]))
    argsMap
      .get("before_timestamp_millis")
      .map(t => builder.withBeforeTimestampMillis(t.value.asInstanceOf[Long]))
    argsMap.get("delete_unverified").map(t =>
      builder.withDeleteUnverified(t.value.asInstanceOf[Boolean]))
    argsMap
      .get("error_if_tagged_old_versions")
      .map(t => builder.withErrorIfTaggedOldVersions(t.value.asInstanceOf[Boolean]))

    builder.build()
  }

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case lanceDataset: LanceDataset => lanceDataset
      case _ =>
        throw new UnsupportedOperationException("Vacuum only supports LanceDataset")
    }

    val policy = buildPolicy()
    val readOptions = lanceDataset.readOptions()

    val stats = {
      val dataset = openDataset(readOptions)
      try {
        dataset.cleanupWithPolicy(policy)
      } finally {
        dataset.close()
      }
    }

    Seq(new GenericInternalRow(Array[Any](stats.getBytesRemoved, stats.getOldVersions)))
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
