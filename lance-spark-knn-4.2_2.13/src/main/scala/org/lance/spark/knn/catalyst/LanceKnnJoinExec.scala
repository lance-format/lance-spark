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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.StructType
import org.lance.spark.knn.internal.LanceKnnJoinStage

/**
 * Physical operator for the indexed nearest-by join. The whole join is one no-shuffle
 * `mapPartitions` over the left input — [[requiredChildDistribution]] is intentionally NOT
 * overridden, so Catalyst inserts NO `Exchange` above this node. Each task:
 *
 *  1. decodes the child's `RDD[InternalRow]` (the left input) into typed `Row`s,
 *  2. drives [[LanceKnnJoinStage.runPartition]], which opens R's index once, probes + trims +
 *     late-materializes per left row (see that object's doc for why a shuffle/merge pipeline only
 *     adds cost), and
 *  3. re-encodes the assembled `left ++ right ++ __score` rows back to `RDD[InternalRow]`.
 *
 * The decode/encode uses `ExpressionEncoder`; `.copy()` on both sides because Spark reuses the
 * `InternalRow` buffer across iterations of the upstream/downstream operators.
 */
case class LanceKnnJoinExec(
    override val child: SparkPlan,
    stageConf: LanceKnnJoinStage.Conf,
    leftSchema: StructType,
    finalSchema: StructType,
    finalOutput: Seq[Attribute])
  extends UnaryExecNode {

  override def output: Seq[Attribute] = finalOutput

  override def nodeName: String = "LanceKnnJoin"

  // The right-side + score attrs in `output` are synthesised per row from the probe results;
  // they do not appear in `child.output`. Declare them produced so Spark's `missingInput` check
  // (and the `!` marker in tree-string output) doesn't flag this node.
  override def producedAttributes: AttributeSet = AttributeSet(output) -- child.outputSet

  override protected def doExecute(): RDD[InternalRow] = {
    val childRdd = child.execute()
    val leftSchemaCaptured = leftSchema
    val finalSchemaCaptured = finalSchema
    val confCaptured = stageConf

    // Encoders are created on the driver and captured into the closure (ExpressionEncoder is
    // serializable). The deserializer/serializer instances are NOT thread-safe, so build them
    // per partition inside `mapPartitions`.
    val leftEnc = ExpressionEncoder(leftSchemaCaptured).resolveAndBind()
    val finalEnc = ExpressionEncoder(finalSchemaCaptured).resolveAndBind()

    childRdd.mapPartitions { iter =>
      val deser = leftEnc.createDeserializer()
      val ser = finalEnc.createSerializer()
      val leftRows: Iterator[Row] = iter.map(ir => deser(ir.copy()))
      LanceKnnJoinStage.runPartition(leftRows, confCaptured).map(row => ser(row).copy())
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): LanceKnnJoinExec =
    copy(child = newChild)
}
