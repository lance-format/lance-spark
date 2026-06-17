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

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.lance.spark.LanceDataset

private[optimizer] object BlobPlanUtils {

  final class V2BlobWrite private[BlobPlanUtils] (
      val table: NamedRelation,
      val query: LogicalPlan,
      val writeOptions: Map[String, String],
      command: LogicalPlan) {

    def withQuery(newQuery: LogicalPlan): LogicalPlan = command match {
      case a: AppendData => a.copy(query = newQuery)
      case o: OverwriteByExpression => o.copy(query = newQuery)
    }

    def withTableAndOptions(
        newTable: NamedRelation,
        newWriteOptions: Map[String, String]): LogicalPlan = command match {
      case a: AppendData => a.copy(table = newTable, writeOptions = newWriteOptions)
      case o: OverwriteByExpression => o.copy(table = newTable, writeOptions = newWriteOptions)
    }
  }

  object V2BlobWrite {
    def unapply(plan: LogicalPlan): Option[V2BlobWrite] = plan match {
      case a: AppendData => Some(new V2BlobWrite(a.table, a.query, a.writeOptions, a))
      case o: OverwriteByExpression => Some(new V2BlobWrite(o.table, o.query, o.writeOptions, o))
      case _ => None
    }
  }

  object LanceRelation {
    def unapply(plan: LogicalPlan): Option[(DataSourceV2Relation, LanceDataset)] = plan match {
      case relation: DataSourceV2Relation =>
        relation.table match {
          case ds: LanceDataset => Some((relation, ds))
          case _ => None
        }
      case _ => None
    }
  }

  def collectLanceRelationsWithSubqueries(
      plan: LogicalPlan): Seq[(DataSourceV2Relation, LanceDataset)] =
    plan.collectWithSubqueries { case LanceRelation(relation, ds) => (relation, ds) }
}
