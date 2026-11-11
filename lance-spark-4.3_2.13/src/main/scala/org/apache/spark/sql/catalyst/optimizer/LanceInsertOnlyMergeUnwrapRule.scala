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

import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertOnlyMerge, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Spark 4.2 lowers an insert-only MERGE (only {@code WHEN NOT MATCHED THEN INSERT}) to an
 * [[InsertOnlyMerge]] node instead of the [[AppendData]] used through Spark 4.1. That node bypasses
 * [[LanceBlobV2RowLevelCopyRule]], which hooks {@code AppendData}/{@code WriteDelta} to turn blob v2
 * descriptors into copy tokens. Without the rewrite the masked blob-v2 target reaches {@code
 * newWriteBuilder} with struct-typed descriptors and the write is rejected.
 *
 * For a blob-v2 target (wrapped in [[RowLevelMaskedLanceTable]] by
 * [[LanceBlobV2RowLevelResolutionRule]]) rewrite the node back to the pre-4.2 {@code AppendData}
 * shape so the shared copy rule runs. Other targets keep the {@code InsertOnlyMerge} fast path.
 *
 * Must be injected before [[LanceBlobV2RowLevelCopyRule]] so the unwrapped {@code AppendData} is
 * visible to the copy rewrite within the operator-optimization batch.
 */
case class LanceInsertOnlyMergeUnwrapRule() extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case InsertOnlyMerge(r: DataSourceV2Relation, query, None, _)
        if r.table.isInstanceOf[RowLevelMaskedLanceTable] =>
      AppendData.byPosition(r, query)
  }
}
