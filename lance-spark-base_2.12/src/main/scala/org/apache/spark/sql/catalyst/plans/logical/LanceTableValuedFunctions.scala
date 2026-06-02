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
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, ExpressionInfo, Literal, SortOrder}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, LanceArrowUtils}
import org.lance.ipc.Query
import org.lance.spark.{LanceDataset, LanceSparkReadOptions}
import org.lance.spark.utils.{QueryUtils, Utils}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object LanceTableValuedFunctions {

  val VECTOR_SEARCH = "vector_search"
  private val VECTOR_DISTANCE = "_distance"

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
    if (args.size != 4) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH needs four parameters: table, column, query_vector, limit.")
    }

    val tableName = extractString(args(0), "table")
    val requestedColumn = extractString(args(1), "column")
    val queryVector = extractQueryVector(args(2))
    val limit = extractLimit(args(3))

    val resolvedTable =
      if (looksLikePath(tableName)) {
        resolvePathTable(tableName)
      } else {
        resolveCatalogTable(spark, tableName)
      }

    val resolver = spark.sessionState.conf.resolver
    val sourceSchema = resolvedTable.table.schema()
    val vectorField = sourceSchema.fields.find(field => resolver(field.name, requestedColumn))
      .getOrElse {
        throw new IllegalArgumentException(
          s"Column $requestedColumn does not exist in Lance table $tableName.")
      }
    if (sourceSchema.fields.exists(field => resolver(field.name, VECTOR_DISTANCE))) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH cannot read a table that already contains "
          + s"$VECTOR_DISTANCE; the name is reserved for vector search distance.")
    }

    val query = createNearestQuery(vectorField.name, queryVector, limit)
    val scanOptions =
      new CaseInsensitiveStringMap(
        Map(LanceSparkReadOptions.CONFIG_NEAREST -> QueryUtils.queryToString(query)).asJava)
    val relation =
      DataSourceV2Relation.create(
        withDistanceColumn(resolvedTable.table),
        resolvedTable.catalog,
        resolvedTable.identifier,
        scanOptions)

    val distanceAttr = relation.output.find(attr => resolver(attr.name, VECTOR_DISTANCE))
      .getOrElse {
        throw new IllegalStateException(
          s"Internal column $VECTOR_DISTANCE is missing from vector_search plan.")
      }

    val sorted = Sort(Seq(SortOrder(distanceAttr, Ascending)), global = true, relation)
    val limited = GlobalLimit(Literal(limit), LocalLimit(Literal(limit), sorted))
    Project(
      limited.output.filterNot(attr => resolver(attr.name, VECTOR_DISTANCE)),
      limited)
  }

  private def createNearestQuery(column: String, queryVector: Array[Float], limit: Int): Query = {
    val builder = new Query.Builder()
    builder.setColumn(column)
    builder.setKey(queryVector)
    builder.setK(limit)
    builder.setUseIndex(true)
    builder.build()
  }

  private def withDistanceColumn(table: LanceDataset): LanceDataset = {
    new LanceDataset(
      table.readOptions(),
      table.schema().add(VECTOR_DISTANCE, DataTypes.FloatType, true),
      table.getInitialStorageOptions,
      table.getNamespaceImpl,
      table.getNamespaceProperties,
      table.getManagedVersioning,
      table.getFileFormatVersion)
  }

  private case class ResolvedLanceTable(
      table: LanceDataset,
      catalog: Option[TableCatalog],
      identifier: Option[Identifier])

  private def resolvePathTable(path: String): ResolvedLanceTable = {
    val readOptions = LanceSparkReadOptions.from(path)
    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      val schema = LanceArrowUtils.fromArrowSchema(dataset.getSchema)
      ResolvedLanceTable(
        new LanceDataset(
          readOptions,
          schema,
          null,
          null,
          null,
          false,
          dataset.getLanceFileFormatVersion),
        None,
        None)
    } finally {
      dataset.close()
    }
  }

  private def resolveCatalogTable(spark: SparkSession, tableName: String): ResolvedLanceTable = {
    val catalogManager = spark.sessionState.catalogManager
    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val currentCatalog = catalogManager.currentCatalog.asInstanceOf[TableCatalog]

    val (catalog, ident) = parts match {
      case Seq(table) =>
        (currentCatalog, Identifier.of(catalogManager.currentNamespace, table))
      case Seq(namespace, table) =>
        (currentCatalog, Identifier.of(Array(namespace), table))
      case multipart if multipart.size >= 3 =>
        tableCatalog(spark, multipart.head) match {
          case Some(namedCatalog) =>
            (
              namedCatalog,
              Identifier.of(multipart.slice(1, multipart.size - 1).toArray, multipart.last))
          case None =>
            (currentCatalog, Identifier.of(multipart.dropRight(1).toArray, multipart.last))
        }
      case _ =>
        throw new IllegalArgumentException(s"Invalid Lance table identifier: $tableName")
    }

    catalog.loadTable(ident) match {
      case lanceTable: LanceDataset =>
        ResolvedLanceTable(lanceTable, Some(catalog), Some(ident))
      case other =>
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH only supports Lance tables, but $tableName resolved to "
            + other.getClass.getName)
    }
  }

  private def tableCatalog(spark: SparkSession, catalogName: String): Option[TableCatalog] = {
    try {
      spark.sessionState.catalogManager.catalog(catalogName) match {
        case catalog: TableCatalog => Some(catalog)
        case _ => None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def looksLikePath(value: String): Boolean = {
    value.startsWith("/") ||
    value.contains("/") ||
    value.startsWith("s3://") ||
    value.startsWith("gs://") ||
    value.startsWith("az://") ||
    value.startsWith("abfss://") ||
    value.startsWith("file://") ||
    value.startsWith("hdfs://")
  }

  private def extractString(expr: Expression, argName: String): String = {
    val value = evalFoldable(expr, argName)
    if (value == null) {
      throw new IllegalArgumentException(s"$VECTOR_SEARCH $argName argument cannot be null.")
    }
    value.toString
  }

  private def extractLimit(expr: Expression): Int = {
    val value = evalFoldable(expr, "limit")
    val limit = value match {
      case n: java.lang.Byte => n.longValue()
      case n: java.lang.Short => n.longValue()
      case n: java.lang.Integer => n.longValue()
      case n: java.lang.Long => n.longValue()
      case n: java.lang.Number => n.longValue()
      case other =>
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH limit must be a positive integer, but got ${typeName(other)}.")
    }
    if (limit <= 0 || limit > Int.MaxValue) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH limit must be a positive integer no larger than ${Int.MaxValue}, "
          + s"but got $limit.")
    }
    limit.toInt
  }

  private def extractQueryVector(expr: Expression): Array[Float] = {
    val value = evalFoldable(expr, "query_vector")
    if (value == null) {
      throw new IllegalArgumentException(s"$VECTOR_SEARCH query_vector argument cannot be null.")
    }
    val arrayData = value match {
      case data: ArrayData => data
      case other =>
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH query_vector must be an array of numeric values, "
            + s"but got ${typeName(other)}.")
    }
    if (arrayData.numElements() == 0) {
      throw new IllegalArgumentException(s"$VECTOR_SEARCH query_vector cannot be empty.")
    }
    expr.dataType match {
      case ArrayType(elementType, _) =>
        Array.tabulate(arrayData.numElements()) { index =>
          if (arrayData.isNullAt(index)) {
            throw new IllegalArgumentException(
              s"$VECTOR_SEARCH query_vector cannot contain null elements.")
          }
          elementType match {
            case FloatType => arrayData.getFloat(index)
            case DoubleType => arrayData.getDouble(index).toFloat
            case ByteType => arrayData.getByte(index).toFloat
            case ShortType => arrayData.getShort(index).toFloat
            case IntegerType => arrayData.getInt(index).toFloat
            case LongType => arrayData.getLong(index).toFloat
            case other =>
              throw new IllegalArgumentException(
                s"$VECTOR_SEARCH query_vector must contain numeric values convertible to FLOAT, "
                  + s"but got element type ${other.simpleString}.")
          }
        }
      case other =>
        throw new IllegalArgumentException(
          s"$VECTOR_SEARCH query_vector must be an array, but got ${other.simpleString}.")
    }
  }

  private def evalFoldable(expr: Expression, argName: String): Any = {
    if (!expr.foldable) {
      throw new IllegalArgumentException(
        s"$VECTOR_SEARCH $argName argument must be a foldable literal expression.")
    }
    expr.eval()
  }

  private def typeName(value: Any): String = {
    if (value == null) {
      "null"
    } else {
      value.getClass.getName
    }
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

/** Plan for vector_search(table, column, query_vector, limit). */
case class VectorSearchQuery(override val args: Seq[Expression])
  extends LanceTableValueFunction(LanceTableValuedFunctions.VECTOR_SEARCH)
