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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistryBase, Resolver}
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, ExpressionInfo, Literal, SortOrder}
import org.apache.spark.sql.connector.catalog.{Identifier, LookupCatalog, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.lance.ipc.Query
import org.lance.spark.{LanceConstant, LanceDataset, LanceSparkReadOptions}
import org.lance.spark.utils.{QueryUtils, VectorUtils}

import scala.collection.JavaConverters._

object LanceTableValuedFunctions {

  val VECTOR_SEARCH = "vector_search"

  val supportedFnNames: Seq[String] = Seq(VECTOR_SEARCH)

  private type TableFunctionDescription = (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)

  def getTableValueFunctionInjection(fnName: String): TableFunctionDescription = {
    val (info, builder) = fnName match {
      case VECTOR_SEARCH =>
        FunctionRegistryBase.build[VectorSearchQuery](fnName, since = None)
      case _ =>
        throw new IllegalArgumentException(
          s"Function $fnName is not a supported Lance table-valued function.")
    }
    (FunctionIdentifier(fnName), info, builder)
  }

  def resolveLanceTableValuedFunction(
      spark: SparkSession,
      tvf: LanceTableValueFunction): LogicalPlan = {
    tvf match {
      case VectorSearchQuery(args) => resolveVectorSearch(spark, args)
      case _ =>
        throw new IllegalArgumentException(
          s"Function ${tvf.fnName} is not a supported Lance table-valued function.")
    }
  }

  private def resolveVectorSearch(spark: SparkSession, args: Seq[Expression]): LogicalPlan = {
    if (args.size != 2) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH needs two parameters: table, nearest.")
    }

    val tableName = extractString(args(0), "table")
    val nearestJson = extractString(args(1), "nearest")
    val nearestQuery = extractNearestQuery(nearestJson)
    val limit = extractLimit(nearestQuery)

    val resolvedTable = resolveCatalogTable(spark, tableName)

    val resolver = spark.sessionState.conf.resolver
    val sourceSchema = resolvedTable.table.schema()
    if (sourceSchema.fields.exists(field => resolver(field.name, LanceConstant.VECTOR_DISTANCE))) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH cannot read a table that already contains "
          + s"${LanceConstant.VECTOR_DISTANCE}; the name is reserved for vector search distance.")
    }
    validateVectorColumn(sourceSchema, nearestQuery, resolver)

    val scanOptions =
      new CaseInsensitiveStringMap(
        Map(LanceSparkReadOptions.CONFIG_NEAREST -> nearestJson).asJava)
    val relation =
      DataSourceV2Relation.create(
        withDistanceColumn(resolvedTable.table),
        resolvedTable.catalog,
        resolvedTable.identifier,
        scanOptions)

    val distanceAttr =
      relation.output.find(attr => resolver(attr.name, LanceConstant.VECTOR_DISTANCE))
        .getOrElse {
          throw new IllegalStateException(
            s"Internal column ${LanceConstant.VECTOR_DISTANCE} is missing from vector_search plan.")
        }

    val sorted = Sort(Seq(SortOrder(distanceAttr, Ascending)), global = true, relation)
    val limited = GlobalLimit(Literal(limit), LocalLimit(Literal(limit), sorted))
    Project(
      limited.output.filterNot(attr => resolver(attr.name, LanceConstant.VECTOR_DISTANCE)),
      limited)
  }

  private def extractNearestQuery(nearestJson: String): Query = {
    try {
      val query = QueryUtils.stringToQuery(nearestJson)
      if (query == null) {
        throw new IllegalArgumentException(s"$VECTOR_SEARCH nearest argument cannot be null.")
      }
      query
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH nearest argument must be a valid Lance nearest query json.",
          e)
    }
  }

  private def extractLimit(query: Query): Int = {
    val limit = query.getK
    if (limit <= 0) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH nearest query k must be a positive integer, but got $limit.")
    }
    limit
  }

  private def validateVectorColumn(
      schema: StructType,
      query: Query,
      resolver: Resolver): Unit = {
    val columnName = query.getColumn
    if (columnName == null || columnName.isEmpty) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH nearest query must specify a 'column'.")
    }
    val field = schema.fields.find(f => resolver(f.name, columnName)).getOrElse {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH column '$columnName' does not exist in table schema. "
          + s"Available columns: ${schema.fieldNames.mkString(", ")}")
    }
    if (!VectorUtils.isVectorField(field)) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH column '$columnName' is not a vector column "
          + s"(FixedSizeList of Float/Double); got ${field.dataType.simpleString}.")
    }
    val key = query.getKey
    if (key != null) {
      val dim = VectorUtils.getVectorDimension(field)
      if (dim > 0 && key.length != dim) {
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH query vector length ${key.length} does not match "
            + s"column '$columnName' dimension $dim.")
      }
    }
  }

  private def withDistanceColumn(table: LanceDataset): LanceDataset = {
    table.withSchema(table.schema().add(LanceConstant.VECTOR_DISTANCE, DataTypes.FloatType, true))
  }

  private case class ResolvedLanceTable(
      table: LanceDataset,
      catalog: Option[TableCatalog],
      identifier: Option[Identifier])

  private def resolveCatalogTable(spark: SparkSession, tableName: String): ResolvedLanceTable = {
    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val lookup = new LookupCatalog {
      override protected val catalogManager = spark.sessionState.catalogManager
    }
    val (catalog, ident) = parts match {
      case lookup.CatalogAndIdentifier(resolvedCatalog, resolvedIdent) =>
        (resolvedCatalog, resolvedIdent)
      case _ =>
        throw new IllegalArgumentException(s"Invalid Lance table identifier: $tableName")
    }
    val tableCatalog = catalog match {
      case catalog: TableCatalog => catalog
      case other =>
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH only supports table catalogs, but $tableName resolved to "
            + other.getClass.getName)
    }

    tableCatalog.loadTable(ident) match {
      case lanceTable: LanceDataset =>
        ResolvedLanceTable(lanceTable, Some(tableCatalog), Some(ident))
      case other =>
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH only supports Lance tables, but $tableName resolved to "
            + other.getClass.getName)
    }
  }

  private def extractString(expr: Expression, argName: String): String = {
    val value = evalFoldable(expr, argName)
    if (value == null) {
      throw new IllegalArgumentException(s"$VECTOR_SEARCH $argName argument cannot be null.")
    }
    value.toString
  }

  private def evalFoldable(expr: Expression, argName: String): Any = {
    if (!expr.foldable) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH $argName argument must be a foldable literal expression.")
    }
    expr.eval()
  }

}

/**
 * Represents an unresolved Lance table-valued function.
 *
 * @param fnName
 *   one of [[LanceTableValuedFunctions.supportedFnNames]].
 */
abstract class LanceTableValueFunction(val fnName: String) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

  val args: Seq[Expression]
}

/** Plan for vector_search(table, nearest). */
case class VectorSearchQuery(override val args: Seq[Expression])
  extends LanceTableValueFunction(LanceTableValuedFunctions.VECTOR_SEARCH)
