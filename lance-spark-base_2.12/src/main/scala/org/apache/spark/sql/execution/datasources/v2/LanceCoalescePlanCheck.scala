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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Coalesce, Expression, ExprId, NamedExpression, PlanExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.lance.spark.internal.CoalesceArrowReader
import org.lance.spark.read.LanceScan

import java.util.{ArrayList, Collections, LinkedHashMap, List => JList, Map => JMap}

import scala.collection.mutable

/**
 * Verifies in the optimized Catalyst logical plan that a configured `coalesce_columns` fallback
 * column is only consumed as a fallback argument of `COALESCE(primary, fallback)` and is not
 * referenced by residual filters or projected expressions outside that `COALESCE`.
 */
object LanceCoalescePlanCheck {

  def registerIfNeeded(): Unit = {
    SparkSession.getActiveSession.foreach { session =>
      val current = session.experimental.extraStrategies
      if (!current.contains(Strategy)) {
        session.experimental.extraStrategies = Strategy +: current
      }
    }
  }

  private object Strategy extends SparkStrategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case PhysicalOperation(projectList, filters, rel: DataSourceV2ScanRelation) =>
          rel.scan match {
            case lanceScan: LanceScan =>
              val configured = lanceScan.readOptions().getCoalesceColumns
              if (!configured.isEmpty) {
                val verified = resolveVerifiedCoalesce(projectList, filters, rel.output, configured)
                lanceScan.setVerifiedCoalesceColumns(verified)
              }
            case _ =>
          }
        case _ =>
      }
      Nil
    }
  }

  private def resolveVerifiedCoalesce(
      projectList: Seq[NamedExpression],
      filters: Seq[Expression],
      scanOutput: Seq[AttributeReference],
      configured: JMap[String, String]): JMap[String, String] = {
    val outputNames: JList[String] = new ArrayList[String](scanOutput.size)
    val attrByName = mutable.HashMap.empty[String, AttributeReference]
    scanOutput.foreach { attr =>
      outputNames.add(attr.name)
      attrByName.put(attr.name, attr)
    }

    val active = CoalesceArrowReader.resolveActiveCoalesce(outputNames, configured)
    if (active.isEmpty) {
      return Collections.emptyMap()
    }

    val fallbackToPrimaries = mutable.LinkedHashMap.empty[String, mutable.ArrayBuffer[String]]
    val iter = active.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val list = fallbackToPrimaries.getOrElseUpdate(
        entry.getValue,
        mutable.ArrayBuffer.empty[String])
      list += entry.getKey
    }

    val verified: JMap[String, String] = new LinkedHashMap[String, String]()
    fallbackToPrimaries.foreach { case (fallbackCol, candidatePrimaries) =>
      val fbAttr = attrByName(fallbackCol).exprId
      val referencedInFilters = filters.exists(referencesAttr(_, fbAttr))
      if (!referencedInFilters) {
        val validPrimaryByExprId: Map[ExprId, String] =
          candidatePrimaries.map(p => attrByName(p).exprId -> p).toMap
        val guardingPrimaries = mutable.LinkedHashSet.empty[String]
        var allGuarded = true
        val projIter = projectList.iterator
        while (projIter.hasNext && allGuarded) {
          collectGuardingPrimaries(projIter.next(), fbAttr, validPrimaryByExprId) match {
            case Some(primaries) => guardingPrimaries ++= primaries
            case None => allGuarded = false
          }
        }
        if (allGuarded && guardingPrimaries.nonEmpty) {
          candidatePrimaries.foreach { p =>
            if (guardingPrimaries.contains(p)) {
              verified.put(p, fallbackCol)
            }
          }
        }
      }
    }
    Collections.unmodifiableMap(verified)
  }

  private def referencesAttr(expr: Expression, targetId: ExprId): Boolean = {
    if (expr.references.exists(_.exprId == targetId)) {
      true
    } else {
      expr.exists {
        case p: PlanExpression[_] =>
          p.plan match {
            case lp: LogicalPlan =>
              lp.references.exists(_.exprId == targetId) ||
              lp.expressions.exists(_.references.exists(_.exprId == targetId))
            case _ => false
          }
        case _ => false
      }
    }
  }

  private def collectGuardingPrimaries(
      expr: Expression,
      fbAttr: ExprId,
      validPrimaryByExprId: Map[ExprId, String]): Option[Seq[String]] = {
    if (!referencesAttr(expr, fbAttr)) {
      return Some(Seq.empty)
    }
    expr match {
      case Coalesce(children) =>
        val primaries = mutable.LinkedHashSet.empty[String]
        var i = 0
        while (i < children.length) {
          children(i) match {
            case attr: AttributeReference if attr.exprId == fbAttr =>
              val precedingPrimaries = children.slice(0, i).collect {
                case prev: AttributeReference if validPrimaryByExprId.contains(prev.exprId) =>
                  validPrimaryByExprId(prev.exprId)
              }
              if (precedingPrimaries.isEmpty) {
                return None
              }
              primaries ++= precedingPrimaries
            case child =>
              collectGuardingPrimaries(child, fbAttr, validPrimaryByExprId) match {
                case Some(childPrimaries) => primaries ++= childPrimaries
                case None => return None
              }
          }
          i += 1
        }
        Some(primaries.toSeq)

      case attr: AttributeReference if attr.exprId == fbAttr =>
        None

      case _: PlanExpression[_] =>
        None

      case other =>
        val primaries = mutable.LinkedHashSet.empty[String]
        val iter = other.children.iterator
        while (iter.hasNext) {
          collectGuardingPrimaries(iter.next(), fbAttr, validPrimaryByExprId) match {
            case Some(childPrimaries) => primaries ++= childPrimaries
            case None => return None
          }
        }
        Some(primaries.toSeq)
    }
  }
}
