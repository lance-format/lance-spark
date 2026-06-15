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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan, MergeIntoTable, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, SupportsRead, SupportsRowLevelOperations, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperationBuilder, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.lance.spark.LanceDataset
import org.lance.spark.utils.BlobUtils

import java.util

/**
 * Masks ACCEPT_ANY_SCHEMA on MERGE/UPDATE/DELETE targets so row-level resolution runs on blob v2
 * tables. Lowered plans are rewritten by [[LanceBlobV2RowLevelCopyRule]].
 */
case class LanceBlobV2RowLevelResolutionRule() extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case m: MergeIntoTable => m.copy(targetTable = mask(m.targetTable))
    case u: UpdateTable => u.copy(table = mask(u.table))
    case d: DeleteFromTable => d.copy(table = mask(d.table))
  }

  private def mask(target: LogicalPlan): LogicalPlan = target.resolveOperators {
    case r: DataSourceV2Relation =>
      r.table match {
        case _: RowLevelMaskedLanceTable => r
        case ds: LanceDataset with SupportsRowLevelOperations
            if BlobUtils.hasBlobV2Fields(ds.schema()) =>
          r.copy(table = new RowLevelMaskedLanceTable(ds))
        case _ => r
      }
  }
}

/** Delegates everything to the wrapped Lance dataset, minus ACCEPT_ANY_SCHEMA. */
class RowLevelMaskedLanceTable(val delegate: LanceDataset with SupportsRowLevelOperations)
  extends Table
  with SupportsRead
  with SupportsWrite
  with SupportsMetadataColumns
  with SupportsRowLevelOperations {

  override def name(): String = delegate.name()

  override def schema(): StructType = delegate.schema()

  override def capabilities(): util.Set[TableCapability] = {
    val filtered = new util.HashSet[TableCapability](delegate.capabilities())
    filtered.remove(TableCapability.ACCEPT_ANY_SCHEMA)
    filtered
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    delegate.newScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    delegate.newWriteBuilder(info)

  override def metadataColumns(): Array[MetadataColumn] = delegate.metadataColumns()

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder =
    delegate.newRowLevelOperationBuilder(info)
}
