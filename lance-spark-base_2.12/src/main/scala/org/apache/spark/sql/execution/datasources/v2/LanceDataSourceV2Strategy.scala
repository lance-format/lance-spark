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
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

case class LanceDataSourceV2Strategy(session: SparkSession) extends SparkStrategy
  with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case AddColumnsBackfill(ResolvedIdentifier(catalog, ident), columnNames, source) =>
      AddColumnsBackfillExec(asTableCatalog(catalog), ident, columnNames, source) :: Nil

    case UpdateColumnsBackfill(ResolvedIdentifier(catalog, ident), columnNames, source) =>
      UpdateColumnsBackfillExec(asTableCatalog(catalog), ident, columnNames, source) :: Nil

    case Optimize(ResolvedIdentifier(catalog, ident), args) =>
      OptimizeExec(asTableCatalog(catalog), ident, args) :: Nil

    case Vacuum(ResolvedIdentifier(catalog, ident), args) =>
      VacuumExec(asTableCatalog(catalog), ident, args) :: Nil

    case AddIndex(ResolvedIdentifier(catalog, ident), indexName, method, columns, args) =>
      AddIndexExec(
        asTableCatalog(catalog),
        ident,
        indexName.toLowerCase,
        method,
        columns,
        args) :: Nil

    case ShowIndexes(ResolvedIdentifier(catalog, ident)) =>
      ShowIndexesExec(asTableCatalog(catalog), ident) :: Nil

    case LanceDropIndex(ResolvedIdentifier(catalog, ident), indexName) =>
      LanceDropIndexExec(asTableCatalog(catalog), ident, indexName.toLowerCase) :: Nil

    case LanceCreateBranch(ResolvedIdentifier(catalog, ident), branchName, ref, ifNotExists) =>
      LanceCreateBranchExec(asTableCatalog(catalog), ident, branchName, ref, ifNotExists) :: Nil

    case LanceDropBranch(ResolvedIdentifier(catalog, ident), branchName, ifExists) =>
      LanceDropBranchExec(asTableCatalog(catalog), ident, branchName, ifExists) :: Nil

    case LanceShowBranches(ResolvedIdentifier(catalog, ident)) =>
      LanceShowBranchesExec(asTableCatalog(catalog), ident) :: Nil

    case LanceCreateTag(ResolvedIdentifier(catalog, ident), tagName, ref, ifNotExists) =>
      LanceCreateTagExec(asTableCatalog(catalog), ident, tagName, ref, ifNotExists) :: Nil

    case LanceDropTag(ResolvedIdentifier(catalog, ident), tagName, ifExists) =>
      LanceDropTagExec(asTableCatalog(catalog), ident, tagName, ifExists) :: Nil

    case LanceShowTags(ResolvedIdentifier(catalog, ident)) =>
      LanceShowTagsExec(asTableCatalog(catalog), ident) :: Nil

    case SetUnenforcedPrimaryKey(ResolvedIdentifier(catalog, ident), columns) =>
      SetUnenforcedPrimaryKeyExec(asTableCatalog(catalog), ident, columns) :: Nil

    case _ => Nil
  }

  private def asTableCatalog(plugin: CatalogPlugin): TableCatalog = {
    plugin match {
      case t: TableCatalog => t
      case _ => throw new IllegalArgumentException(s"Catalog $plugin is not a TableCatalog")
    }
  }

}
