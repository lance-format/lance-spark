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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * Logical plan for `ANALYZE TABLE <t> COMPUTE STATISTICS [FOR ALL COLUMNS | FOR COLUMNS c1, ...]`.
 * The executor computes per-column min/max/nullCount/distinctCount (+ avgLen/maxLen) via Spark's
 * own `CommandUtils.computeColumnStats` and persists them into Lance table properties under
 * `lance.stats.*` keys, tagged with the expected post-write manifest version (read version + 1).
 *
 * <p>This plan is produced by [[org.apache.spark.sql.execution.datasources.v2.LanceAnalyzeTableResolution]],
 * which rewrites Spark's native `AnalyzeColumn` / `AnalyzeTable` into it when the target is a Lance
 * table — there is no custom ANALYZE grammar. {@code columns} is empty when the user specified
 * {@code FOR ALL COLUMNS} (or omitted the clause entirely); the executor expands it to the full
 * table schema. NDV is always HLL-approximate (Spark's native ANALYZE behavior).
 */
case class LanceAnalyzeTable(
    table: LogicalPlan,
    columns: Seq[String],
    forAllColumns: Boolean) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = LanceAnalyzeTableOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = {
    val target = if (forAllColumns) "ALL COLUMNS" else columns.mkString(", ")
    s"AnalyzeLanceTable [for $target]"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan])
      : LanceAnalyzeTable = {
    copy(table = newChildren(0))
  }
}

object LanceAnalyzeTableOutputType {
  val SCHEMA = StructType(
    Array(
      StructField("columns_analyzed", DataTypes.IntegerType, nullable = false),
      StructField("rows_scanned", DataTypes.LongType, nullable = false),
      StructField("manifest_version", DataTypes.LongType, nullable = false)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
