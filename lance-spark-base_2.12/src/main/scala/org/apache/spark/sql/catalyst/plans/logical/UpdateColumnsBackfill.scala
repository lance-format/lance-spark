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

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Logical plan node representing the ALTER TABLE UPDATE COLUMNS FROM TABLE/VIEW command.
 *
 * This command updates existing columns in a table by computing their values
 * from a TABLE/VIEW. The TABLE/VIEW is executed and the results are used
 * to update the columns. Only rows that match (by _rowaddr) are updated;
 * rows in source that don't exist in target are ignored.
 *
 * @param table The target table to update columns in
 * @param columnNames The names of the columns to update
 * @param source The TABLE/VIEW that provides the new values for the columns
 */
case class UpdateColumnsBackfill(
    table: LogicalPlan,
    columnNames: Seq[String],
    source: LogicalPlan) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table, source)

  override def output: Seq[Attribute] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): UpdateColumnsBackfill = {
    copy(table = newChildren(0), columnNames, source = newChildren(1))
  }

  override def simpleString(maxFields: Int): String = {
    s"UpdateColumnsBackfill columns=[${columnNames.mkString(", ")}]"
  }
}
