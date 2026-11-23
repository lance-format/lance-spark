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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LocalRelation, LogicalPlan, OverwriteByExpression, Project}
import org.apache.spark.sql.execution.datasources.v2.AppendDataCompat
import org.apache.spark.sql.types.IntegerType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class BlobPlanUtilsTest {

  private val table = UnresolvedRelation(Seq("target"))
  private val query = LocalRelation(AttributeReference("id", IntegerType)())
  private val options = Map("marker" -> "original")
  private val deletePredicate = EqualTo(query.output.head, Literal(7))
  private val writes: Seq[LogicalPlan] = Seq(
    AppendDataCompat.byPosition(table, query, options),
    AppendData.byName(table, query, options),
    OverwriteByExpression.byPosition(table, query, deletePredicate, options),
    OverwriteByExpression.byName(table, query, deletePredicate, options))

  @Test
  def queryRewritePreservesTheTargetAndWriteOptions(): Unit = {
    val newQuery = Project(query.output, query)
    for (original <- writes) {
      val updated = BlobPlanUtils.V2BlobWrite.unapply(original).get.withQuery(newQuery)
      val write = BlobPlanUtils.V2BlobWrite.unapply(updated).get
      assertSame(newQuery, write.query)
      assertSame(table, write.table)
      assertSame(options, write.writeOptions)
      assertWriteFlagsPreserved(original, updated)
    }
  }

  @Test
  def targetRewritePreservesTheQueryAndWriteFlags(): Unit = {
    val newTable = UnresolvedRelation(Seq("replacement"))
    val newOptions = Map("marker" -> "updated")
    for (original <- writes) {
      val updated =
        BlobPlanUtils.V2BlobWrite.unapply(original).get.withTableAndOptions(newTable, newOptions)
      val write = BlobPlanUtils.V2BlobWrite.unapply(updated).get
      assertSame(newTable, write.table)
      assertSame(newOptions, write.writeOptions)
      assertSame(query, write.query)
      assertWriteFlagsPreserved(original, updated)
    }
  }

  private def assertWriteFlagsPreserved(original: LogicalPlan, updated: LogicalPlan): Unit = {
    assertEquals(original.getClass, updated.getClass)
    original match {
      case append: AppendData =>
        assertEquals(append.isByName, updated.asInstanceOf[AppendData].isByName)
      case overwrite: OverwriteByExpression =>
        val rewritten = updated.asInstanceOf[OverwriteByExpression]
        assertEquals(overwrite.isByName, rewritten.isByName)
        assertSame(overwrite.deleteExpr, rewritten.deleteExpr)
    }
  }
}
