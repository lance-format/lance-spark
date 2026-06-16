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

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.lance.LanceMatch
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.lance.spark.LanceConstant
import org.lance.spark.LanceDataset

import java.util.{HashMap => JHashMap}

/**
 * Optimizer rule that pushes `lance_match(column, 'query')` predicates into Lance scans as FTS
 * queries, by injecting the FTS column and query text into the table's scan options before the
 * V2 pushdown batch builds the physical scan.
 *
 * This rule runs in the standard operator-optimization batch, which fires BEFORE
 * {@code V2ScanRelationPushDown}. At this point the plan still contains {@code DataSourceV2Relation}
 * (not yet a {@code DataSourceV2ScanRelation}); we mutate that relation's options map so that
 * {@code LanceDataset.newScanBuilder} will see the FTS spec and configure the scan builder
 * accordingly. The {@code LanceMatch} expression is then removed from the {@code Filter} node.
 *
 * MVP supports two shapes:
 *   - `Filter(LanceMatch(col, 'q'), rel)` → FTS only, filter node removed
 *   - `Filter(And(LanceMatch(col, 'q'), rest), rel)` → FTS extracted, `rest` kept in Filter
 *
 * Deeper nesting (OR, NOT, multiple LanceMatch) falls back to Catalyst evaluation of LanceMatch,
 * which is correct but does not use the FTS index.
 */
case class LanceFtsPushdownRule() extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case f @ Filter(condition, rel: DataSourceV2Relation)
        if isLanceTableWithoutFts(rel) =>
      extractTopLevelLanceMatch(condition) match {
        case Some((column, query, remainingOpt)) =>
          val newOpts = new JHashMap[String, String]()
          newOpts.putAll(rel.options.asCaseSensitiveMap())
          newOpts.put(LanceConstant.LANCE_FTS_COLUMN_OPT, column)
          newOpts.put(LanceConstant.LANCE_FTS_QUERY_OPT, query)
          val newRel = rel.copy(options = new CaseInsensitiveStringMap(newOpts))
          remainingOpt match {
            case Some(cond) => Filter(cond, newRel)
            case None => newRel
          }
        case None => f
      }
  }

  private def isLanceTableWithoutFts(rel: DataSourceV2Relation): Boolean =
    rel.table.isInstanceOf[LanceDataset] &&
      !rel.options.containsKey(LanceConstant.LANCE_FTS_COLUMN_OPT)

  private def extractTopLevelLanceMatch(
      condition: Expression): Option[(String, String, Option[Expression])] = condition match {
    case m: LanceMatch => specOf(m).map { case (c, q) => (c, q, None) }
    case And(l: LanceMatch, r) => specOf(l).map { case (c, q) => (c, q, Some(r)) }
    case And(l, r: LanceMatch) => specOf(r).map { case (c, q) => (c, q, Some(l)) }
    case _ => None
  }

  private def specOf(m: LanceMatch): Option[(String, String)] = (m.left, m.right) match {
    case (col: AttributeReference, Literal(q: UTF8String, _)) => Some((col.name, q.toString))
    case _ => None
  }
}
