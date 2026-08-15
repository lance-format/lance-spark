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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType, StringType}
import org.lance.spark.{LanceConstant, LanceDataset}

import scala.collection.mutable.ArrayBuffer

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

    val options = Map(LanceConstant.REPLACE_WHERE_KEY -> predicate) ++
      equalityTermsJson(predicate).map(LanceConstant.REPLACE_WHERE_EQUALITY_KEY -> _)

    val append = AppendData.byPosition(relation, query, options)
    val qe = session.sessionState.executePlan(append)
    qe.assertCommandExecuted()

    Nil
  }

  /**
   * If the predicate is a pure conjunction of `column = literal` equality terms on string or
   * integral columns, returns their JSON encoding for the metadata-only fragment-drop fast path at
   * commit time. Returns `None` for any other predicate shape (ranges, OR, functions, other types),
   * in which case commit falls back to the exact scan-based deletion — so this only ever enables an
   * optimization, never changes which rows are replaced.
   */
  private def equalityTermsJson(predicate: String): Option[String] = {
    val parsed =
      try {
        session.sessionState.sqlParser.parseExpression(predicate)
      } catch {
        case _: Throwable => return None
      }

    val terms = ArrayBuffer.empty[(String, String)]
    if (!collectEqualities(parsed, terms)) {
      return None
    }
    // A column appearing twice with different required values can never match; let the scan path
    // handle that (it will simply find no rows). Only emit when each column maps to one value.
    val byColumn = terms.groupBy(_._1)
    if (terms.isEmpty || byColumn.exists(_._2.map(_._2).distinct.size > 1)) {
      return None
    }
    val json =
      byColumn
        .map { case (col, pairs) => (col, pairs.head._2) }
        .map { case (col, value) => s"""{"column":${quote(col)},"value":${quote(value)}}""" }
        .mkString("[", ",", "]")
    Some(json)
  }

  /**
   * Walks a conjunction, collecting `column = literal` pairs into `out`. Returns false (disabling
   * the fast path) as soon as any node is not an AND or a supported equality on a simple column
   * reference and string/integral literal.
   */
  private def collectEqualities(expr: Expression, out: ArrayBuffer[(String, String)]): Boolean =
    expr match {
      case And(left, right) => collectEqualities(left, out) && collectEqualities(right, out)
      case EqualTo(col, lit: Literal) if columnName(col).isDefined && lit.value != null =>
        supportedLiteral(lit) match {
          case Some(value) =>
            out += ((columnName(col).get, value))
            true
          case None => false
        }
      case EqualTo(lit: Literal, col) if columnName(col).isDefined && lit.value != null =>
        supportedLiteral(lit) match {
          case Some(value) =>
            out += ((columnName(col).get, value))
            true
          case None => false
        }
      case _ => false
    }

  private def columnName(expr: Expression): Option[String] = expr match {
    case u: UnresolvedAttribute if u.nameParts.size == 1 => Some(u.nameParts.head)
    case _ => None
  }

  /** Canonical string form for a literal the zonemap comparison can reproduce, else None. */
  private def supportedLiteral(lit: Literal): Option[String] = lit.dataType match {
    case StringType => Some(lit.value.toString)
    case ByteType | ShortType | IntegerType | LongType => Some(lit.value.toString)
    case _ => None
  }

  private def quote(s: String): String = {
    val escaped = s.replace("\\", "\\\\").replace("\"", "\\\"")
    "\"" + escaped + "\""
  }
}
