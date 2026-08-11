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
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.connector.catalog._
import org.lance.spark.{LanceConstant, LanceDataset}

/**
 * Physical plan for `REPLACE <table> WHERE <predicate> AS <query>`.
 *
 * The command reuses the ordinary distributed write pipeline to materialize the query result into
 * new Lance fragments, and carries the row filter through as an internal write option
 * ([[LanceConstant.REPLACE_WHERE_KEY]]). The batch-write commit then turns the append into a single
 * atomic `Update` that deletes the existing rows matching the predicate and adds the new fragments,
 * so the replacement is one table version (an atomic delete + append) rather than two commits.
 */
case class ReplaceWhereExec(
    catalog: TableCatalog,
    ident: Identifier,
    predicate: String,
    query: LogicalPlan)
  extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val originalTable = catalog.loadTable(ident) match {
      case lanceTable: LanceDataset => lanceTable
      case other =>
        throw new UnsupportedOperationException(
          s"REPLACE ... WHERE is only supported for Lance tables, but got: ${other.getClass}")
    }

    // Write through a relation built on the target table's schema so the query is validated and
    // written exactly like a normal INSERT. The predicate rides along as an internal write option;
    // it is consumed at commit time to compute the rows to delete.
    val relation = DataSourceV2Relation.create(
      new LanceDataset(
        originalTable.readOptions(),
        originalTable.schema(),
        originalTable.getInitialStorageOptions,
        originalTable.getNamespaceImpl,
        originalTable.getNamespaceProperties,
        originalTable.getManagedVersioning,
        originalTable.getFileFormatVersion),
      Some(catalog),
      Some(ident))

    val append =
      AppendData.byPosition(relation, query, Map(LanceConstant.REPLACE_WHERE_KEY -> predicate))
    val qe = session.sessionState.executePlan(append)
    qe.assertCommandExecuted()

    Nil
  }
}
