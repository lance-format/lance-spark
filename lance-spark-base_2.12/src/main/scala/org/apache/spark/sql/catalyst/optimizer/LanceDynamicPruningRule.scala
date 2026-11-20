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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.lance.spark.{LanceConstant, LanceDataset}

import scala.collection.JavaConverters._

/**
 * Lets Spark's PartitionPruning rule prune a Lance table by a join with a filtered Lance table.
 * PartitionPruning looks for a selective Filter on the side of the join that filters the other,
 * but a filter on a Lance table is pushed into its scan, which removes the Filter before that rule
 * runs. Before the scan push-down, this rule marks such a filtering side's Lance relation with
 * [[LanceConstant.KEEP_PUSHED_FILTERS_KEY]], so its scan also returns the predicates it pushes
 * as ones Spark still evaluates, and the Filter stays for PartitionPruning to find.
 *
 * Only a relation read through projections and filters straight under the join is marked: such a
 * scan has no limit, sort or aggregate above it to push down, which a Filter left above it would
 * prevent. Spark evaluates the kept predicates again on the rows the scan returns.
 */
case class LanceDynamicPruningRule() extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }
    plan.transformUp {
      case j @ Join(left, right, joinType, Some(condition), _) =>
        lazy val keys = equiJoinKeys(condition, left, right)
        // As PartitionPruning: a side can be pruned when the join keeps its rows only if they
        // match, and here only a Lance scan that takes a runtime filter on the key. The filtering
        // side is checked first, since looking up the other side's indexes opens its table.
        val newRight =
          if (canPruneLeft(joinType) && canKeepFilters(right) &&
            keys.exists { case (l, _) => prunableBy(left, l) }) {
            mark(right)
          } else {
            right
          }
        val newLeft =
          if (canPruneRight(joinType) && canKeepFilters(left) &&
            keys.exists { case (_, r) => prunableBy(right, r) }) {
            mark(left)
          } else {
            left
          }
        if ((newLeft ne left) || (newRight ne right)) j.copy(left = newLeft, right = newRight)
        else j
    }
  }

  /** The (left, right) attribute pairs of the equalities in `condition`. */
  private def equiJoinKeys(
      condition: Expression,
      left: LogicalPlan,
      right: LogicalPlan): Seq[(Attribute, Attribute)] =
    splitConjunctivePredicates(condition).collect {
      case EqualTo(a: AttributeReference, b: AttributeReference)
          if left.outputSet.contains(a) && right.outputSet.contains(b) => (a, b)
      case EqualTo(a: AttributeReference, b: AttributeReference)
          if left.outputSet.contains(b) && right.outputSet.contains(a) => (b, a)
    }

  /**
   * Whether `plan` reads column `key` of a Lance table through projections and filters, and the
   * table's scan would take a runtime filter on it.
   */
  private def prunableBy(plan: LogicalPlan, key: Attribute): Boolean =
    lanceRelation(plan).exists { r =>
      r.outputSet.contains(key) &&
      r.table.asInstanceOf[LanceDataset].runtimeFilterColumns(r.options).contains(key.name)
    }

  private def lanceRelation(plan: LogicalPlan): Option[DataSourceV2Relation] = plan match {
    case r: DataSourceV2Relation if r.table.isInstanceOf[LanceDataset] => Some(r)
    case Project(projectList, child) if projectList.forall(_.deterministic) => lanceRelation(child)
    case Filter(c, child) if c.deterministic => lanceRelation(child)
    case _ => None
  }

  /**
   * Whether `plan` reads a Lance relation through projections and filters, at least one of them
   * selective, so that marking the relation keeps a Filter for PartitionPruning.
   */
  private def canKeepFilters(plan: LogicalPlan): Boolean =
    lanceRelation(plan).isDefined && plan.exists {
      case Filter(c, _) => isLikelySelective(c)
      case _ => false
    }

  /** `plan` with its Lance relation marked to keep its pushed filters. */
  private def mark(plan: LogicalPlan): LogicalPlan = plan match {
    case r: DataSourceV2Relation =>
      val options = r.options.asCaseSensitiveMap().asScala
      if (options.get(LanceConstant.KEEP_PUSHED_FILTERS_KEY).contains("true")) {
        r
      } else {
        r.copy(options = new CaseInsensitiveStringMap(
          (options + (LanceConstant.KEEP_PUSHED_FILTERS_KEY -> "true")).asJava))
      }
    case p @ Project(_, child) => p.copy(child = mark(child))
    case f @ Filter(_, child) => f.copy(child = mark(child))
    case other => other
  }

  private def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  private def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter => true
    case _ => false
  }
}
