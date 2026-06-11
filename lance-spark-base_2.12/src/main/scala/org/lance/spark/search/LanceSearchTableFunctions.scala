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
package org.lance.spark.search

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.lance.spark.LanceDataset
import org.lance.spark.search.LanceSearchQuery.SearchType

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object LanceSearchTableFunctions {
  private val DefaultK: Integer = Integer.valueOf(10)
  private val TableArg = "table"
  private val QueryVectorArg = "query_vector"
  private val QueryArg = "query"
  private val SearchQueryArg = "search_query"
  private val ColumnsArg = "columns"
  private val SearchColumnsArg = "search_columns"
  private val DistanceMetricColumn = "_distance"
  private val FtsScoreColumn = "_score"
  private val HybridScoreColumn = "_relevance_score"
  private val RowIdColumn = "_rowid"
  private val NamedArgumentExpressionClass =
    "org.apache.spark.sql.catalyst.expressions.NamedArgumentExpression"

  def vectorSearch(args: Seq[Expression]): LogicalPlan = {
    val parsed = parseArgs("VECTOR_SEARCH", args, Seq(TableArg, QueryVectorArg, "k"))
    val tableName = requiredString(parsed, TableArg)
    val queryVector = requiredFloatArray(parsed, QueryVectorArg)
    val k = optionalInt(parsed, "num_results")
      .orElse(optionalInt(parsed, "limit"))
      .orElse(optionalInt(parsed, "k"))
      .getOrElse(DefaultK)
    val outputColumns = optionalStringArray(parsed, ColumnsArg).getOrElse(Seq.empty)
    val filter = optionalString(parsed, "filter").orNull
    val withRowId = effectiveWithRowId(parsed, outputColumns)
    val offset = optionalInt(parsed, "offset")
    val version = optionalLong(parsed, "version")
    val requestK = offset
      .map(value => Integer.valueOf(Math.addExact(k.intValue(), value.intValue())))
      .getOrElse(k)
    val resolved = resolveLanceTable(tableName, version)
    val schemaColumns =
      requestOutputColumns(resolved.table.schema(), outputColumns, DistanceMetricColumn)
    val requestColumns = namespaceOutputColumns(schemaColumns)
    val schema =
      outputSchema(resolved.table.schema(), schemaColumns, DistanceMetricColumn, withRowId)

    val builder = LanceSearchQuery
      .builder(SearchType.VECTOR)
      .tableId(resolved.table.readOptions().getTableId)
      .namespaceImpl(resolved.table.getNamespaceImpl)
      .namespaceProperties(resolved.table.getNamespaceProperties)
      .outputColumns(requestColumns.asJava)
      .vector(queryVector.asJava)
      .topK(requestK)
      .vectorColumn(optionalString(parsed, "vector_column").orNull)
      .distanceType(optionalString(parsed, "distance_type").orNull)
      .filter(filter)
      .offset(offset.orNull)
      .version(version.orNull)
      .withRowId(withRowId.orNull)
      .nprobes(optionalInt(parsed, "nprobes").orNull)
      .ef(optionalInt(parsed, "ef").orNull)
      .refineFactor(optionalInt(parsed, "refine_factor").orNull)
      .lowerBound(optionalFloat(parsed, "lower_bound").orNull)
      .upperBound(optionalFloat(parsed, "upper_bound").orNull)
      .bypassVectorIndex(optionalBoolean(parsed, "bypass_vector_index").orNull)
      .fastSearch(optionalBoolean(parsed, "fast_search").orNull)
      .prefilter(optionalBoolean(parsed, "prefilter").orNull)

    relation("VECTOR_SEARCH", schema, builder.build(), resolved)
  }

  def search(args: Seq[Expression]): LogicalPlan = {
    val parsed = parseArgs("SEARCH", args, Seq(TableArg, QueryArg, "k"))
    val tableName = requiredString(parsed, TableArg)
    val queryText = optionalString(parsed, QueryArg)
      .orElse(optionalString(parsed, SearchQueryArg))
      .getOrElse(throw new IllegalArgumentException("SEARCH requires query"))
    val k = optionalInt(parsed, "num_results")
      .orElse(optionalInt(parsed, "limit"))
      .orElse(optionalInt(parsed, "k"))
      .getOrElse(DefaultK)
    val outputColumns = optionalStringArray(parsed, ColumnsArg).getOrElse(Seq.empty)
    val filter = optionalString(parsed, "filter").orNull
    val withRowId = effectiveWithRowId(parsed, outputColumns)
    val version = optionalLong(parsed, "version")
    val resolved = resolveLanceTable(tableName, version)
    val schemaColumns = requestOutputColumns(resolved.table.schema(), outputColumns, FtsScoreColumn)
    val requestColumns = namespaceOutputColumns(schemaColumns)
    val schema = outputSchema(resolved.table.schema(), schemaColumns, FtsScoreColumn, withRowId)

    val builder = LanceSearchQuery
      .builder(SearchType.FULL_TEXT)
      .tableId(resolved.table.readOptions().getTableId)
      .namespaceImpl(resolved.table.getNamespaceImpl)
      .namespaceProperties(resolved.table.getNamespaceProperties)
      .outputColumns(requestColumns.asJava)
      .textQuery(queryText)
      .searchColumns(optionalStringArray(parsed, SearchColumnsArg).getOrElse(Seq.empty).asJava)
      .topK(k)
      .filter(filter)
      .offset(optionalInt(parsed, "offset").orNull)
      .version(version.orNull)
      .withRowId(withRowId.orNull)

    relation("SEARCH", schema, builder.build(), resolved)
  }

  def hybridSearch(args: Seq[Expression]): LogicalPlan = {
    val parsed = parseArgs("HYBRID_SEARCH", args, Seq(TableArg, QueryVectorArg, QueryArg, "k"))
    val tableName = requiredString(parsed, TableArg)
    val queryVector = requiredFloatArray(parsed, QueryVectorArg)
    val queryText = optionalString(parsed, QueryArg)
      .orElse(optionalString(parsed, SearchQueryArg))
      .getOrElse(throw new IllegalArgumentException("HYBRID_SEARCH requires query"))
    val k = optionalInt(parsed, "num_results")
      .orElse(optionalInt(parsed, "limit"))
      .orElse(optionalInt(parsed, "k"))
      .getOrElse(DefaultK)
    val offset = optionalInt(parsed, "offset").getOrElse(Integer.valueOf(0))
    val candidateK = optionalInt(parsed, "candidates")
      .orElse(optionalInt(parsed, "num_candidates"))
      .orElse(optionalInt(parsed, "candidate_count"))
      .map(value => Integer.valueOf(math.max(value.intValue(), k.intValue() + offset.intValue())))
      .getOrElse(Integer.valueOf(k.intValue() + offset.intValue()))
    val outputColumns = optionalStringArray(parsed, ColumnsArg).getOrElse(Seq.empty)
    val filter = optionalString(parsed, "filter").orNull
    val withRowId = effectiveWithRowId(parsed, outputColumns)
    val rrfK = optionalFloat(parsed, "rrf_k").getOrElse(java.lang.Float.valueOf(60.0f))
    val version = optionalLong(parsed, "version")

    val resolved = resolveLanceTable(tableName, version)
    val vectorSchemaColumns =
      hybridSideOutputColumns(resolved.table.schema(), outputColumns, DistanceMetricColumn)
    val ftsSchemaColumns =
      hybridSideOutputColumns(resolved.table.schema(), outputColumns, FtsScoreColumn)
    val vectorSchema =
      outputSchema(
        resolved.table.schema(),
        vectorSchemaColumns,
        DistanceMetricColumn,
        Some(java.lang.Boolean.TRUE))
    val ftsSchema =
      outputSchema(
        resolved.table.schema(),
        ftsSchemaColumns,
        FtsScoreColumn,
        Some(java.lang.Boolean.TRUE))
    val schema = hybridOutputSchema(resolved.table.schema(), outputColumns, withRowId)

    val vectorRequestColumns = namespaceOutputColumns(vectorSchemaColumns)
    val ftsRequestColumns = namespaceOutputColumns(ftsSchemaColumns)

    val vectorQuery = LanceSearchQuery
      .builder(SearchType.VECTOR)
      .tableId(resolved.table.readOptions().getTableId)
      .namespaceImpl(resolved.table.getNamespaceImpl)
      .namespaceProperties(resolved.table.getNamespaceProperties)
      .outputColumns(vectorRequestColumns.asJava)
      .vector(queryVector.asJava)
      .topK(candidateK)
      .vectorColumn(optionalString(parsed, "vector_column").orNull)
      .distanceType(optionalString(parsed, "distance_type").orNull)
      .filter(filter)
      .version(version.orNull)
      .withRowId(java.lang.Boolean.TRUE)
      .nprobes(optionalInt(parsed, "nprobes").orNull)
      .ef(optionalInt(parsed, "ef").orNull)
      .refineFactor(optionalInt(parsed, "refine_factor").orNull)
      .lowerBound(optionalFloat(parsed, "lower_bound").orNull)
      .upperBound(optionalFloat(parsed, "upper_bound").orNull)
      .bypassVectorIndex(optionalBoolean(parsed, "bypass_vector_index").orNull)
      .fastSearch(optionalBoolean(parsed, "fast_search").orNull)
      .prefilter(optionalBoolean(parsed, "prefilter").orNull)
      .build()

    val ftsQuery = LanceSearchQuery
      .builder(SearchType.FULL_TEXT)
      .tableId(resolved.table.readOptions().getTableId)
      .namespaceImpl(resolved.table.getNamespaceImpl)
      .namespaceProperties(resolved.table.getNamespaceProperties)
      .outputColumns(ftsRequestColumns.asJava)
      .textQuery(queryText)
      .searchColumns(optionalStringArray(parsed, SearchColumnsArg).getOrElse(Seq.empty).asJava)
      .topK(candidateK)
      .filter(filter)
      .version(version.orNull)
      .withRowId(java.lang.Boolean.TRUE)
      .build()

    val hybridQuery =
      new LanceHybridSearchQuery(vectorQuery, ftsQuery, vectorSchema, ftsSchema, k, offset, rrfK)
    hybridRelation("HYBRID_SEARCH", schema, hybridQuery, resolved)
  }

  private def relation(
      functionName: String,
      schema: StructType,
      query: LanceSearchQuery,
      resolved: ResolvedLanceTable): LogicalPlan = {
    val table = new LanceSearchTable(functionName, schema, query)
    DataSourceV2Relation.create(
      table,
      Some(resolved.catalog),
      Some(resolved.identifier),
      CaseInsensitiveStringMap.empty())
  }

  private def hybridRelation(
      functionName: String,
      schema: StructType,
      query: LanceHybridSearchQuery,
      resolved: ResolvedLanceTable): LogicalPlan = {
    val table = new LanceHybridSearchTable(functionName, schema, query)
    DataSourceV2Relation.create(
      table,
      Some(resolved.catalog),
      Some(resolved.identifier),
      CaseInsensitiveStringMap.empty())
  }

  private def parseArgs(
      functionName: String,
      args: Seq[Expression],
      positionalNames: Seq[String]): ParsedArgs = {
    val named = scala.collection.mutable.LinkedHashMap.empty[String, Expression]
    val positional = ArrayBuffer.empty[Expression]

    args.foreach { expr =>
      namedArgument(expr) match {
        case Some((key, value)) =>
          named.put(normalizeName(key), value)
        case None =>
          positional += expr
      }
    }

    if (named.nonEmpty && positional.nonEmpty) {
      throw new IllegalArgumentException(
        s"$functionName does not support mixing named and positional arguments")
    }
    if (named.nonEmpty) {
      ParsedArgs(named.toMap)
    } else {
      if (positional.size > positionalNames.size) {
        throw new IllegalArgumentException(s"$functionName received too many positional arguments")
      }
      ParsedArgs(positionalNames.zip(positional).map { case (name, expr) => name -> expr }.toMap)
    }
  }

  private def requiredString(parsed: ParsedArgs, name: String): String =
    optionalString(parsed, name).getOrElse(throw new IllegalArgumentException(s"$name is required"))

  private def requiredFloatArray(parsed: ParsedArgs, name: String): Seq[java.lang.Float] =
    optionalFloatArray(parsed, name).getOrElse(
      throw new IllegalArgumentException(s"$name is required"))

  private def optionalString(parsed: ParsedArgs, name: String): Option[String] =
    parsed.get(name).map {
      case attr: UnresolvedAttribute => attr.name
      case expr => literalValue(expr) match {
          case value: org.apache.spark.unsafe.types.UTF8String => value.toString
          case value: String => value
          case null => null
          case other => other.toString
        }
    }

  private def optionalStringArray(parsed: ParsedArgs, name: String): Option[Seq[String]] =
    parsed.get(name).map(expr =>
      literalArray(expr).map {
        case value: org.apache.spark.unsafe.types.UTF8String => value.toString
        case value: String => value
        case other => other.toString
      })

  private def optionalFloatArray(parsed: ParsedArgs, name: String): Option[Seq[java.lang.Float]] =
    parsed.get(name).map(expr => literalArray(expr).map(toFloat))

  private def optionalInt(parsed: ParsedArgs, name: String): Option[Integer] =
    parsed.get(name).map(value => Integer.valueOf(toNumber(literalValue(value)).intValue()))

  private def optionalLong(parsed: ParsedArgs, name: String): Option[java.lang.Long] =
    parsed.get(name).map(value => java.lang.Long.valueOf(toNumber(literalValue(value)).longValue()))

  private def optionalFloat(parsed: ParsedArgs, name: String): Option[java.lang.Float] =
    parsed.get(name).map(value => toFloat(literalValue(value)))

  private def optionalBoolean(parsed: ParsedArgs, name: String): Option[java.lang.Boolean] =
    parsed.get(name).map(value =>
      java.lang.Boolean.valueOf(literalValue(value).asInstanceOf[Boolean]))

  private def literalArray(expr: Expression): Seq[Any] = expr match {
    case array: CreateArray => array.children.map(literalValue)
    case literal: Literal if literal.value == null => Seq.empty
    case other =>
      literalValue(other) match {
        case values: Seq[_] => values
        case values: Array[_] => values.toSeq
        case value =>
          throw new IllegalArgumentException(s"Expected array literal, got $value")
      }
  }

  private def literalValue(expr: Expression): Any = expr match {
    case literal: Literal => literal.value
    case array: CreateArray => array.children.map(literalValue)
    case other if other.foldable => other.eval(InternalRow.empty)
    case other =>
      throw new IllegalArgumentException(s"Argument must be a foldable literal: ${other.sql}")
  }

  private def toNumber(value: Any): Number = value match {
    case number: Number => number
    case decimal: org.apache.spark.sql.types.Decimal => decimal.toJavaBigDecimal
    case decimal: scala.math.BigDecimal => decimal.bigDecimal
    case other => throw new IllegalArgumentException(s"Expected numeric literal, got $other")
  }

  private def toFloat(value: Any): java.lang.Float =
    java.lang.Float.valueOf(toNumber(value).floatValue())

  private def namedArgument(expr: Expression): Option[(String, Expression)] = {
    if (expr.getClass.getName != NamedArgumentExpressionClass) {
      None
    } else {
      val key = expr.getClass.getMethod("key").invoke(expr).asInstanceOf[String]
      val value = expr.getClass.getMethod("value").invoke(expr).asInstanceOf[Expression]
      Some((key, value))
    }
  }

  private def normalizeName(name: String): String =
    name.toLowerCase(Locale.ROOT)

  private def normalizeOutputColumns(columns: Seq[String]): Seq[String] =
    if (columns.exists(_ == "*")) {
      Seq.empty
    } else {
      columns
    }

  private def requestOutputColumns(
      baseSchema: StructType,
      columns: Seq[String],
      metricName: String): Seq[String] = {
    val normalizedColumns = normalizeOutputColumns(columns)
    if (normalizedColumns.isEmpty) {
      normalizedColumns
    } else {
      val resolvedColumns = normalizedColumns.map { column =>
        if (column.equalsIgnoreCase(metricName)) {
          metricName
        } else if (column.equalsIgnoreCase(RowIdColumn)) {
          RowIdColumn
        } else {
          findField(baseSchema, column).name
        }
      }
      if (resolvedColumns.exists(_.equalsIgnoreCase(metricName))) {
        resolvedColumns
      } else {
        resolvedColumns :+ metricName
      }
    }
  }

  private def hybridSideOutputColumns(
      baseSchema: StructType,
      columns: Seq[String],
      metricName: String): Seq[String] = {
    val normalizedColumns = normalizeOutputColumns(columns)
    if (normalizedColumns.isEmpty) {
      normalizedColumns
    } else {
      val resolvedColumns = normalizedColumns.flatMap { column =>
        if (isHybridMetricColumn(column) || column.equalsIgnoreCase(RowIdColumn)) {
          None
        } else {
          Some(findField(baseSchema, column).name)
        }
      }
      if (resolvedColumns.exists(_.equalsIgnoreCase(metricName))) {
        resolvedColumns
      } else {
        resolvedColumns :+ metricName
      }
    }
  }

  private def namespaceOutputColumns(columns: Seq[String]): Seq[String] =
    normalizeOutputColumns(columns).filterNot(_.equalsIgnoreCase(RowIdColumn))

  private def effectiveWithRowId(
      parsed: ParsedArgs,
      outputColumns: Seq[String]): Option[java.lang.Boolean] =
    if (normalizeOutputColumns(outputColumns).exists(_.equalsIgnoreCase(RowIdColumn))) {
      Some(java.lang.Boolean.TRUE)
    } else {
      optionalBoolean(parsed, "with_row_id")
    }

  private def outputSchema(
      baseSchema: StructType,
      columns: Seq[String],
      metricName: String,
      withRowId: Option[java.lang.Boolean]): StructType = {
    val normalizedColumns = normalizeOutputColumns(columns)
    val fields =
      if (normalizedColumns.isEmpty) {
        baseSchema.fields.toSeq
      } else {
        normalizedColumns.map { column =>
          if (column.equalsIgnoreCase(metricName)) {
            StructField(metricName, DataTypes.FloatType, nullable = true)
          } else if (column.equalsIgnoreCase(RowIdColumn)) {
            StructField(RowIdColumn, DataTypes.LongType, nullable = true)
          } else {
            findField(baseSchema, column)
          }
        }
      }
    val result = ArrayBuffer(fields: _*)
    if (!result.exists(_.name.equalsIgnoreCase(metricName))) {
      result += StructField(metricName, DataTypes.FloatType, nullable = true)
    }
    if (withRowId.contains(java.lang.Boolean.TRUE) &&
      !result.exists(_.name.equalsIgnoreCase(RowIdColumn))) {
      result += StructField(RowIdColumn, DataTypes.LongType, nullable = true)
    }
    new StructType(result.toArray)
  }

  private def hybridOutputSchema(
      baseSchema: StructType,
      columns: Seq[String],
      withRowId: Option[java.lang.Boolean]): StructType = {
    val normalizedColumns = normalizeOutputColumns(columns)
    val fields =
      if (normalizedColumns.isEmpty) {
        baseSchema.fields.toSeq
      } else {
        normalizedColumns.map { column =>
          if (column.equalsIgnoreCase(DistanceMetricColumn)) {
            StructField(DistanceMetricColumn, DataTypes.FloatType, nullable = true)
          } else if (column.equalsIgnoreCase(FtsScoreColumn)) {
            StructField(FtsScoreColumn, DataTypes.FloatType, nullable = true)
          } else if (column.equalsIgnoreCase(HybridScoreColumn)) {
            StructField(HybridScoreColumn, DataTypes.FloatType, nullable = false)
          } else if (column.equalsIgnoreCase(RowIdColumn)) {
            StructField(RowIdColumn, DataTypes.LongType, nullable = true)
          } else {
            findField(baseSchema, column)
          }
        }
      }
    val result = ArrayBuffer(fields: _*)
    if (!result.exists(_.name.equalsIgnoreCase(DistanceMetricColumn))) {
      result += StructField(DistanceMetricColumn, DataTypes.FloatType, nullable = true)
    }
    if (!result.exists(_.name.equalsIgnoreCase(FtsScoreColumn))) {
      result += StructField(FtsScoreColumn, DataTypes.FloatType, nullable = true)
    }
    if (!result.exists(_.name.equalsIgnoreCase(HybridScoreColumn))) {
      result += StructField(HybridScoreColumn, DataTypes.FloatType, nullable = false)
    }
    if (withRowId.contains(java.lang.Boolean.TRUE) &&
      !result.exists(_.name.equalsIgnoreCase(RowIdColumn))) {
      result += StructField(RowIdColumn, DataTypes.LongType, nullable = true)
    }
    new StructType(result.toArray)
  }

  private def isHybridMetricColumn(column: String): Boolean =
    column.equalsIgnoreCase(DistanceMetricColumn) ||
      column.equalsIgnoreCase(FtsScoreColumn) ||
      column.equalsIgnoreCase(HybridScoreColumn)

  private def findField(schema: StructType, column: String): StructField =
    schema.fields
      .find(_.name.equalsIgnoreCase(column))
      .getOrElse(throw new IllegalArgumentException(s"Unknown column '$column'"))

  private def resolveLanceTable(
      tableName: String,
      version: Option[java.lang.Long]): ResolvedLanceTable = {
    val spark = SparkSession.active
    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName).toArray
    if (parts.isEmpty) {
      throw new IllegalArgumentException("table is required")
    }
    val catalogManager = spark.sessionState.catalogManager
    val (catalog, identifier) =
      if (parts.length >= 2 && isConfiguredCatalog(spark, parts.head)) {
        val catalog = catalogManager.catalog(parts.head)
        val identParts = parts.tail
        (catalog, Identifier.of(identParts.dropRight(1), identParts.last))
      } else {
        val namespace =
          if (parts.length > 1) parts.dropRight(1) else catalogManager.currentNamespace
        (catalogManager.currentCatalog, Identifier.of(namespace, parts.last))
      }

    val tableCatalog = catalog match {
      case tableCatalog: TableCatalog => tableCatalog
      case other =>
        throw new IllegalArgumentException(s"Catalog '${other.name()}' is not a table catalog")
    }

    val table =
      version
        .map(value => tableCatalog.loadTable(identifier, value.toString))
        .getOrElse(tableCatalog.loadTable(identifier))
    table match {
      case table: LanceDataset =>
        if (table.getNamespaceImpl == null || table.readOptions().getTableId == null) {
          throw new IllegalArgumentException(
            "Lance search table functions require namespace-backed Lance tables")
        }
        ResolvedLanceTable(tableCatalog, identifier, table)
      case other =>
        throw new IllegalArgumentException(
          s"Table '$tableName' is not a Lance table: ${other.getClass.getName}")
    }
  }

  private def isConfiguredCatalog(spark: SparkSession, name: String): Boolean =
    spark.sessionState.catalogManager.isCatalogRegistered(name) ||
      spark.conf.getOption(s"spark.sql.catalog.$name").isDefined

  private case class ParsedArgs(args: Map[String, Expression]) {
    def get(name: String): Option[Expression] = args.get(normalizeName(name))
  }

  private case class ResolvedLanceTable(
      catalog: TableCatalog,
      identifier: Identifier,
      table: LanceDataset)
}
