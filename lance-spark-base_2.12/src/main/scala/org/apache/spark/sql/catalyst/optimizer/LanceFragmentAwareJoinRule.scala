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

/**
 * Optimizer rule for fragment-aware joins on Lance tables.
 *
 * This rule detects joins on _rowaddr or _rowid columns and optimizes them by:
 * 1. Extracting fragment IDs from row addresses
 * 2. Repartitioning both sides by fragment ID
 * 3. Enabling co-located, shuffle-free joins
 *
 * Example transformation:
 * {{{
 *   SELECT * FROM table_a A JOIN table_b B ON A.origin_row_id = B._rowaddr
 *   =>
 *   SELECT * FROM
 *     (SELECT *, (_rowaddr >>> 32) AS _frag_id FROM table_a) A
 *   JOIN
 *     (SELECT *, (_rowaddr >>> 32) AS _frag_id FROM table_b) B
 *   ON A.origin_row_id = B._rowaddr
 *   CLUSTER BY (_frag_id)
 * }}}
 */
case class LanceFragmentAwareJoinRule() extends Rule[LogicalPlan] {

  private val FRAGMENT_ID_COL = "_lance_frag_id"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case join @ Join(left, right, joinType, condition, hint) =>
        // Check if this join can benefit from fragment-aware optimization
        condition match {
          case Some(expr) if canOptimizeJoin(expr, left, right, hint) =>
            optimizeFragmentAwareJoin(join, left, right, joinType, expr, hint)
          case _ => join
        }
    }
  }

  /**
   * Check if a join can be optimized using fragment-aware strategy.
   *
   * Conditions:
   * 1. Join condition involves _rowaddr or _rowid columns
   * 2. At least one side is a Lance table
   * 3. User hint allows fragment-aware join (or no conflicting hints)
   */
  private def canOptimizeJoin(
      condition: Expression,
      left: LogicalPlan,
      right: LogicalPlan,
      hint: JoinHint): Boolean = {

    // Check if hint explicitly enables fragment-aware join
    val hasFragmentAwareHint = hint.leftHint.exists(h =>
      h.strategy.exists(
        _.toString.equalsIgnoreCase("FRAGMENT_AWARE_JOIN"))) || hint.rightHint.exists(h =>
      h.strategy.exists(_.toString.equalsIgnoreCase("FRAGMENT_AWARE_JOIN")))

    // Check if join condition involves row address or row ID columns
    val hasRowAddrJoin = condition.exists {
      case attr: AttributeReference =>
        FragmentAwareJoinUtils.isRowAddressOrIdColumn(attr.name)
      case _ => false
    }

    // Enable optimization if:
    // 1. Explicit hint is provided, OR
    // 2. Join involves rowaddr/rowid columns (auto-detection)
    hasFragmentAwareHint || hasRowAddrJoin
  }

  /**
   * Transform the join to use fragment-aware optimization.
   *
   * Steps:
   * 1. Add fragment ID extraction as a virtual column to both sides
   * 2. Add RepartitionByExpression to cluster by fragment ID
   * 3. Preserve original join condition
   */
  private def optimizeFragmentAwareJoin(
      originalJoin: Join,
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: org.apache.spark.sql.catalyst.plans.JoinType,
      condition: Expression,
      hint: JoinHint): LogicalPlan = {

    // Find row address columns in the join condition
    val (leftRowAddrCols, rightRowAddrCols) = findRowAddressColumns(condition, left, right)

    if (leftRowAddrCols.isEmpty && rightRowAddrCols.isEmpty) {
      // No row address columns found, return original join
      return originalJoin
    }

    // Add fragment ID extraction to left side if it has row address columns
    val leftWithFragId = if (leftRowAddrCols.nonEmpty) {
      addFragmentIdColumn(left, leftRowAddrCols.head)
    } else {
      left
    }

    // Add fragment ID extraction to right side if it has row address columns
    val rightWithFragId = if (rightRowAddrCols.nonEmpty) {
      addFragmentIdColumn(right, rightRowAddrCols.head)
    } else {
      right
    }

    // Create repartitioned plans if both sides have fragment IDs
    val leftRepartitioned = if (leftRowAddrCols.nonEmpty) {
      val fragIdExpr = leftWithFragId.output.find(_.name == FRAGMENT_ID_COL).get
      RepartitionByExpression(Seq(fragIdExpr), leftWithFragId, None)
    } else {
      leftWithFragId
    }

    val rightRepartitioned = if (rightRowAddrCols.nonEmpty) {
      val fragIdExpr = rightWithFragId.output.find(_.name == FRAGMENT_ID_COL).get
      RepartitionByExpression(Seq(fragIdExpr), rightWithFragId, None)
    } else {
      rightWithFragId
    }

    // Create new join with repartitioned inputs
    val newJoin = Join(leftRepartitioned, rightRepartitioned, joinType, Some(condition), hint)

    // Project to remove the temporary fragment ID column
    val outputAttrs = originalJoin.output
    Project(outputAttrs, newJoin)
  }

  /**
   * Find row address or row ID columns in the join condition.
   *
   * @return Tuple of (left row addr columns, right row addr columns)
   */
  private def findRowAddressColumns(
      condition: Expression,
      left: LogicalPlan,
      right: LogicalPlan): (Seq[AttributeReference], Seq[AttributeReference]) = {

    val leftAttrs = left.outputSet
    val rightAttrs = right.outputSet

    val rowAddrColumns = condition.collect {
      case attr: AttributeReference
          if FragmentAwareJoinUtils.isRowAddressOrIdColumn(attr.name) =>
        attr
    }

    val leftRowAddrCols = rowAddrColumns.filter(leftAttrs.contains)
    val rightRowAddrCols = rowAddrColumns.filter(rightAttrs.contains)

    (leftRowAddrCols, rightRowAddrCols)
  }

  /**
   * Add a virtual column that extracts fragment ID from row address.
   *
   * The fragment ID is computed as: rowaddr >>> 32
   */
  private def addFragmentIdColumn(
      plan: LogicalPlan,
      rowAddrCol: AttributeReference): LogicalPlan = {

    // Create expression: rowaddr >>> 32
    val fragmentIdExpr = ShiftRight(rowAddrCol, Literal(32, IntegerType))

    // Create alias for the fragment ID
    val fragmentIdAlias = Alias(fragmentIdExpr, FRAGMENT_ID_COL)()

    // Project with all original columns plus fragment ID
    Project(plan.output :+ fragmentIdAlias, plan)
  }
}
