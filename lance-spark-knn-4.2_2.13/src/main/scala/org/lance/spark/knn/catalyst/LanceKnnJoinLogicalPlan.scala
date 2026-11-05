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
package org.lance.spark.knn.catalyst

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.StructType
import org.lance.spark.knn.internal.LanceKnnJoinStage

/**
 * The single logical node the SQL rewrite ([[IndexedNearestByJoinRule]]) emits for an indexed
 * `APPROX NEAREST K` join. Its one child is the LEFT input; the right (Lance) side is captured in
 * `stageConf` (URI + version + probe parameters), NOT as a plan child — the join runs entirely
 * inside one `mapPartitions` over the left rows, with NO shuffle. The matching
 * [[LanceKnnJoinStrategy]] lowers this to [[LanceKnnJoinExec]], which drives the
 * [[LanceKnnJoinStage.runPartition]] per-partition probe.
 *
 * `output` is `left ++ right ++ __score` — the right-side and score attributes are synthesised
 * here from the probe results, so `producedAttributes = output -- child.outputSet` marks them as
 * introduced by this node (Catalyst's `missingInput` check would otherwise flag them).
 *
 * The `references = child.outputSet` override is load-bearing: the matching exec decodes the WHOLE
 * left row per partition to feed the probe, so no left column can be pruned. Without this override
 * Catalyst's `ColumnPruning` sees a downstream consumer that references only a subset (or nothing,
 * e.g. `count(*)`) and wraps the child in a narrowing `Project`, which would change the row shape
 * the executor's left-side encoder expects.
 */
case class LanceKnnJoinLogicalPlan(
    override val child: LogicalPlan,
    stageConf: LanceKnnJoinStage.Conf,
    leftSchema: StructType,
    finalSchema: StructType,
    finalOutput: Seq[Attribute])
  extends UnaryNode {

  override def output: Seq[Attribute] = finalOutput

  override def producedAttributes: AttributeSet = AttributeSet(output) -- child.outputSet

  override lazy val references: AttributeSet = child.outputSet

  override protected def withNewChildInternal(newChild: LogicalPlan): LanceKnnJoinLogicalPlan =
    copy(child = newChild)
}
