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
package org.lance.spark

import org.apache.spark.sql.catalyst.expressions.{Expression, LanceBlobV2CopyRef}
import org.apache.spark.sql.catalyst.optimizer.LanceBlobSourceContextRule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.util.LanceSerializeUtil
import org.lance.spark.utils.BlobSourceContext

object BlobPlanProbe {

  def countCopyRefs(plan: LogicalPlan): Int =
    allExpressions(plan).flatMap(copyRefsInExpr).size

  def rowAddressColumnNames(plan: LogicalPlan): java.util.List[String] = {
    val names = new java.util.ArrayList[String]()
    allExpressions(plan).flatMap(copyRefsInExpr).foreach { ref =>
      ref.rowAddress.references.foreach(attr => names.add(attr.name))
    }
    names
  }

  /** The dataset version each encoded blob source context would resolve against; null = latest. */
  def blobSourceContextVersions(plan: LogicalPlan): java.util.Map[String, java.lang.Long] = {
    val versions = new java.util.HashMap[String, java.lang.Long]()
    decodedContexts(plan).forEach((uri, ctx) => versions.put(uri, ctx.getReadOptions.getVersion))
    versions
  }

  /** The namespace impl each encoded blob source context would reopen its dataset through. */
  def blobSourceContextNamespaceImpls(plan: LogicalPlan): java.util.Map[String, String] = {
    val impls = new java.util.HashMap[String, String]()
    decodedContexts(plan).forEach((uri, ctx) => impls.put(uri, ctx.getNamespaceImpl))
    impls
  }

  private def decodedContexts(plan: LogicalPlan): java.util.Map[String, BlobSourceContext] =
    LanceBlobSourceContextRule
      .encodeBlobSourceContexts(plan)
      .map(encoded =>
        LanceSerializeUtil.decode[java.util.HashMap[String, BlobSourceContext]](encoded))
      .getOrElse(new java.util.HashMap[String, BlobSourceContext]())

  private def allExpressions(plan: LogicalPlan): Seq[Expression] =
    allNodes(plan).flatMap(_.expressions)

  // CTAS/RTAS are AnalysisOnlyCommands: their rewritten query lives in innerChildren, not children,
  // so a plain children walk misses the injected copyref. Traverse both.
  private def allNodes(plan: LogicalPlan): Seq[LogicalPlan] = {
    val inner = plan.innerChildren.collect { case child: LogicalPlan => child }
    plan +: (plan.children ++ inner).flatMap(allNodes)
  }

  private def copyRefsInExpr(e: Expression): Seq[LanceBlobV2CopyRef] = {
    val self = e match {
      case ref: LanceBlobV2CopyRef => Seq(ref)
      case _ => Seq.empty
    }
    self ++ e.children.flatMap(copyRefsInExpr)
  }
}
