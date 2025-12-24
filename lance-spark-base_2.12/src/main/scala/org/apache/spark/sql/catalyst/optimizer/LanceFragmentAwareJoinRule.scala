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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.IntegerType
import org.lance.spark.join.FragmentAwareJoinUtils

import scala.collection.mutable

/**
 * Optimizer rule for fragment-aware joins on Lance tables.
 *
 * This rule detects joins on `_rowid` columns and optimizes them by:
 * 1. Extracting fragment IDs from row IDs (rowid >>> 32)
 * 2. Repartitioning both sides by fragment ID
 * 3. Enabling co-located, shuffle-free joins
 *
 * Target join condition: `A.origin_row_id = B._rowid`
 *
 * Two scenarios are supported:
 *
 * 1. Non-stable rowid (default, enable_stable_row_ids=false):
 *    - `_rowid` equals `_rowaddr`, fragment ID can be extracted directly via `rowid >>> 32`
 *    - This optimization is fully supported
 *
 * 2. Stable rowid (enable_stable_row_ids=true):
 *    - `_rowid` is a logical ID, different from `_rowaddr`
 *    - Requires RowIdIndex lookup to map `_rowid` to `_rowaddr`
 *    - TODO: Not yet implemented, requires Lance Java API enhancement
 *
 * Note: This optimization assumes non-stable rowid mode. For datasets with
 * stable rowid enabled, additional RowIdIndex lookup would be needed.
 */
case class LanceFragmentAwareJoinRule() extends Rule[LogicalPlan] {

  private val FRAGMENT_ID_COL = "_lance_frag_id"

  // Track which joins have been optimized using object identity
  private val optimizedJoins = mutable.WeakHashMap[Join, Boolean]()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      case join @ Join(left, right, joinType, condition, hint) =>
        // Skip if already processed
        if (optimizedJoins.getOrElse(join, false)) {
          join
        } else if (isChildRepartitioned(left) || isChildRepartitioned(right)) {
          // Skip if children are already repartitioned by fragment ID
          join
        } else {
          condition match {
            case Some(expr) if canOptimizeJoin(expr, hint) =>
              val result = optimizeFragmentAwareJoin(join, left, right, joinType, expr, hint)
              result match {
                case Project(_, newJoin: Join) =>
                  optimizedJoins.put(newJoin, true)
                case _ =>
              }
              result
            case _ => join
          }
        }
    }
  }

  /**
   * Check if a child plan is already repartitioned by fragment ID.
   */
  private def isChildRepartitioned(plan: LogicalPlan): Boolean = {
    plan match {
      case r: RepartitionByExpression =>
        r.partitionExpressions.exists { expr =>
          expr.find {
            case a: AttributeReference => a.name == FRAGMENT_ID_COL
            case _ => false
          }.isDefined
        }
      case Project(_, child) => isChildRepartitioned(child)
      case _ => false
    }
  }

  /**
   * Check if a join can be optimized using fragment-aware strategy.
   */
  private def canOptimizeJoin(condition: Expression, hint: JoinHint): Boolean = {
    // Check if hint explicitly enables fragment-aware join
    val hasFragmentAwareHint = hint.leftHint.exists(h =>
      h.strategy.exists(
        _.toString.equalsIgnoreCase("FRAGMENT_AWARE_JOIN"))) || hint.rightHint.exists(h =>
      h.strategy.exists(_.toString.equalsIgnoreCase("FRAGMENT_AWARE_JOIN")))

    // Check if join condition involves _rowid or _rowaddr metadata column
    val hasRowIdJoin = condition.exists {
      case attr: AttributeReference =>
        FragmentAwareJoinUtils.isRowAddressOrIdColumn(attr.name)
      case _ => false
    }

    hasFragmentAwareHint || hasRowIdJoin
  }

  /**
   * Transform the join to use fragment-aware optimization.
   */
  private def optimizeFragmentAwareJoin(
      originalJoin: Join,
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: org.apache.spark.sql.catalyst.plans.JoinType,
      condition: Expression,
      hint: JoinHint): LogicalPlan = {

    val joinKeys = findJoinKeyColumns(condition, left, right)

    if (joinKeys.isEmpty) {
      return originalJoin
    }

    val (leftKey, rightKey) = joinKeys.head

    // Add fragment ID extraction to both sides
    val leftWithFragId = addFragmentIdColumn(left, leftKey)
    val rightWithFragId = addFragmentIdColumn(right, rightKey)

    // Create repartitioned plans
    val leftFragIdExpr = leftWithFragId.output.find(_.name == FRAGMENT_ID_COL).get
    val leftRepartitioned = RepartitionByExpression(Seq(leftFragIdExpr), leftWithFragId, None)

    val rightFragIdExpr = rightWithFragId.output.find(_.name == FRAGMENT_ID_COL).get
    val rightRepartitioned = RepartitionByExpression(Seq(rightFragIdExpr), rightWithFragId, None)

    // Create new join with repartitioned inputs
    val newJoin = Join(leftRepartitioned, rightRepartitioned, joinType, Some(condition), hint)

    // Project to remove the temporary fragment ID column
    Project(originalJoin.output, newJoin)
  }

  /**
   * Find join key columns that can be used for fragment-aware optimization.
   */
  private def findJoinKeyColumns(
      condition: Expression,
      left: LogicalPlan,
      right: LogicalPlan): Seq[(AttributeReference, AttributeReference)] = {

    val leftAttrs = left.outputSet
    val rightAttrs = right.outputSet

    condition.collect {
      case EqualTo(l: AttributeReference, r: AttributeReference) =>
        val (leftAttr, rightAttr) =
          if (leftAttrs.contains(l)) (l, r)
          else (r, l)
        if (FragmentAwareJoinUtils.isRowAddressOrIdColumn(rightAttr.name) ||
          FragmentAwareJoinUtils.isRowAddressOrIdColumn(leftAttr.name)) {
          Some((leftAttr, rightAttr))
        } else {
          None
        }
      case EqualNullSafe(l: AttributeReference, r: AttributeReference) =>
        val (leftAttr, rightAttr) =
          if (leftAttrs.contains(l)) (l, r)
          else (r, l)
        if (FragmentAwareJoinUtils.isRowAddressOrIdColumn(rightAttr.name) ||
          FragmentAwareJoinUtils.isRowAddressOrIdColumn(leftAttr.name)) {
          Some((leftAttr, rightAttr))
        } else {
          None
        }
    }.flatten
  }

  /**
   * Add a virtual column that extracts fragment ID from a row ID column.
   */
  private def addFragmentIdColumn(
      plan: LogicalPlan,
      rowIdCol: AttributeReference): LogicalPlan = {

    val fragmentIdExpr = ShiftRight(rowIdCol, Literal(32, IntegerType))
    val fragmentIdAlias = Alias(fragmentIdExpr, FRAGMENT_ID_COL)()

    Project(plan.output :+ fragmentIdAlias, plan)
  }
}
