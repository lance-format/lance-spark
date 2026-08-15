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
 * Logical plan node for the Lance `REPLACE <table> WHERE <predicate> AS <query>` command.
 *
 * The command atomically replaces the rows of the target table matching {@code predicate} with the
 * result of {@code query}, in a single table version (an atomic delete + append). It is the
 * partition-overwrite analogue of Iceberg's `INSERT OVERWRITE ... PARTITION(...)`: the rows to drop
 * are chosen by the predicate rather than by a declared partition spec, which Lance does not have.
 *
 * @param table The target Lance table whose matching rows are replaced.
 * @param predicate The row filter, as raw SQL text captured verbatim from the original statement.
 *     It is handed to Lance to select the rows to delete, so its semantics match Lance's own filter
 *     evaluation rather than being resolved as a Catalyst expression here.
 * @param query The source query whose result becomes the new rows for the matched region.
 */
case class ReplaceWhere(
    table: LogicalPlan,
    predicate: String,
    query: LogicalPlan) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table, query)

  override def output: Seq[Attribute] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): ReplaceWhere = {
    copy(table = newChildren(0), predicate = predicate, query = newChildren(1))
  }

  override def simpleString(maxFields: Int): String = {
    s"ReplaceWhere predicate=[$predicate]"
  }
}
