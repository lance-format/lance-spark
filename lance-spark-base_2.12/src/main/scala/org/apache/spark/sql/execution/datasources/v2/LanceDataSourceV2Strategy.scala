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
        indexName,
        method,
        columns,
        args) :: Nil

    case _ => Nil
  }

  private def asTableCatalog(plugin: CatalogPlugin): TableCatalog = {
    plugin match {
      case t: TableCatalog => t
      case _ => throw new IllegalArgumentException(s"Catalog $plugin is not a TableCatalog")
    }
  }

}
