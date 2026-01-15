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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog._
import org.lance.spark.{LanceConstant, LanceDataset}

case class AddColumnsBackfillExec(
    catalog: TableCatalog,
    ident: Identifier,
    columnNames: Seq[String],
    query: LogicalPlan)
  extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val originalTable = catalog.loadTable(ident) match {
      case lanceTable: LanceDataset => lanceTable
      case _ =>
        throw new UnsupportedOperationException("AddColumnsBackfill only supports for LanceDataset")
    }

    // Check the added columns must not exist
    val originalFields = originalTable.schema().fieldNames.toSet
    val existedFields = columnNames.filter(p => originalFields.contains(p))
    if (existedFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"Can't add existed columns: ${existedFields.toList.mkString(", ")}")
    }

    // Add Project if source relation has more fields
    val needFields = query.output.filter(p =>
      columnNames.contains(p.name)
        || LanceDataset.ROW_ADDRESS_COLUMN.name().equals(p.name)
        || LanceDataset.FRAGMENT_ID_COLUMN.name().equals(p.name))

    val actualQuery = if (needFields.length != query.output.length) {
      Project(needFields, query)
    } else {
      query
    }

    val relation = DataSourceV2Relation.create(
      new LanceDataset(originalTable.readOptions(), actualQuery.schema, null, null, null),
      Some(catalog),
      Some(ident))

    val append =
      AppendData.byPosition(
        relation,
        actualQuery,
        Map(LanceConstant.BACKFILL_COLUMNS_KEY -> columnNames.mkString(",")))
    val qe = session.sessionState.executePlan(append)
    qe.assertCommandExecuted()

    Nil
  }
}
