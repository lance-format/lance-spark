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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{And, ApplyFunctionExpression, AttributeReference, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.optimizer.BlobPlanUtils.LanceRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.lance.ipc.FullTextQuery
import org.lance.ipc.FullTextQuery.Operator
import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.function.{LanceMatchFunction, LanceMultiMatchFunction, LancePhraseFunction}
import org.lance.spark.utils.FullTextQueryUtils

import java.util.{ArrayList, HashMap, Locale, Optional}

/**
 * Catalyst optimizer rule that intercepts Lance FTS predicates ({@code lance_match},
 * {@code lance_match_phrase}, {@code lance_multi_match}) in a Filter condition and pushes the FTS
 * predicate down to the Lance scanner via {@link LanceSparkReadOptions#CONFIG_FULL_TEXT_QUERY}
 * in the relation options map.
 *
 * <p>Validation rules enforced at planning time:
 * <ul>
 *   <li>No FTS predicate may appear as an operand of an OR node anywhere in the condition tree —
 *       OR semantics cannot be preserved when a single FTS query is pushed to the scanner.</li>
 *   <li>At most one FTS predicate per Filter scope — Lance {@code ScanOptions} carries a single
 *       {@code FullTextQuery}; multi-column search requires {@code lance_multi_match}.</li>
 *   <li>The column argument must be a column reference or string literal; the query text
 *       argument must be a string literal (it is pushed to the index at planning time and
 *       cannot be a runtime-valued expression).</li>
 *   <li>For {@code lance_match} with options string: all keys must be recognized and values
 *       must be valid at planning time to fail fast with a clear message.</li>
 * </ul>
 *
 * <p>When a valid FTS predicate is found, it is extracted from the condition, the serialized
 * {@code FullTextQuery} is injected into the relation options map, and any remaining non-FTS
 * sub-expressions are retained as a residual {@code Filter} node.
 *
 * <p>Subclasses implement {@link #rewriteRelation} to supply the version-specific
 * {@code DataSourceV2Relation.copy()} call, which differs in arity between Spark 3.x/4.0
 * (5-arg) and Spark 4.1 (6-arg).
 */
abstract class AbstractLanceFtsPredicateRule extends Rule[LogicalPlan] {

  /** Rewrites the relation with the new options map. Version-specific copy() arity lives here. */
  protected def rewriteRelation(
      rel: DataSourceV2Relation,
      newOptions: CaseInsensitiveStringMap): DataSourceV2Relation

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case filter @ Filter(condition, LanceRelation(rel, _)) =>
      val ftsExprs = collectFtsExpressions(condition)
      if (ftsExprs.isEmpty) {
        filter
      } else {
        validateNoOrWithFts(condition)
        if (ftsExprs.size > 1) {
          throw new IllegalArgumentException(
            "At most one Lance FTS predicate is allowed per WHERE clause. " +
              "Lance ScanOptions carries a single FullTextQuery; for multi-column search use " +
              "lance_multi_match instead.")
        }
        val ftsExpr = ftsExprs.head
        if (!isTopLevelConjunct(condition, ftsExpr)) {
          throw new IllegalArgumentException(
            "Lance FTS predicates must be top-level AND conjuncts in the WHERE clause. " +
              "Wrapping an FTS predicate in NOT, CASE, or other expressions is not supported " +
              "because the predicate is pushed to the FTS index at planning time and cannot " +
              "be evaluated as a row-level expression.")
        }
        if (rel.options.containsKey(LanceSparkReadOptions.CONFIG_NEAREST)) {
          throw new IllegalArgumentException(
            "Lance FTS predicates cannot be combined with vector search (nearest) in the " +
              "same query. Lance ScanOptions supports either a full-text query or a nearest " +
              "vector query, but not both simultaneously.")
        }

        val ftsQuery = buildFullTextQuery(ftsExpr)
        val ftsJson = FullTextQueryUtils.fullTextQueryToString(ftsQuery)

        val newOptionsMap = new HashMap[String, String](rel.options.asCaseSensitiveMap())
        newOptionsMap.put(LanceSparkReadOptions.CONFIG_FULL_TEXT_QUERY, ftsJson)
        val newOptions = new CaseInsensitiveStringMap(newOptionsMap)

        val newRel = rewriteRelation(rel, newOptions)

        val residual = removeExpression(condition, ftsExpr)
        residual match {
          case None => newRel
          case Some(remaining) => Filter(remaining, newRel)
        }
      }
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private val knownMatchOptionKeys: Set[String] =
    Set("fuzziness", "operator", "boost", "prefix_length", "max_expansions")

  private val ftsNames: Set[String] = Set(
    LanceMatchFunction.NAME,
    LancePhraseFunction.NAME,
    LanceMultiMatchFunction.NAME)

  private def collectFtsExpressions(expr: Expression): Seq[ApplyFunctionExpression] =
    expr.collect {
      case afe: ApplyFunctionExpression if ftsNames.contains(afe.function.name()) => afe
    }

  /**
   * Returns true if `target` is reachable from `root` only via And nodes (i.e. it is a
   * direct top-level conjunct). Returns false if `target` is wrapped in NOT, CASE, or any
   * other non-And expression.
   */
  private def isTopLevelConjunct(root: Expression, target: Expression): Boolean = root match {
    case e if e eq target => true
    case And(left, right) => isTopLevelConjunct(left, target) || isTopLevelConjunct(right, target)
    case _ => false
  }

  /**
   * Traverses the full condition tree (not just top-level operands) and raises
   * IllegalArgumentException if any FTS expression is reachable via an OR node.
   * OR(FTS, FTS) gets a dedicated message naming lance_multi_match.
   */
  private def validateNoOrWithFts(expr: Expression): Unit = {
    expr match {
      case or: Or =>
        val leftFts = collectFtsExpressions(or.left)
        val rightFts = collectFtsExpressions(or.right)
        if (leftFts.nonEmpty && rightFts.nonEmpty) {
          throw new IllegalArgumentException(
            "Lance FTS predicates cannot be combined with OR — " +
              "OR semantics cannot be pushed to the FTS index. " +
              "For multi-column search, use a single lance_multi_match call with all desired columns.")
        }
        if (leftFts.nonEmpty || rightFts.nonEmpty) {
          throw new IllegalArgumentException(
            "A Lance FTS predicate cannot appear as an operand of OR. " +
              "FTS predicates must be used as a top-level conjunction (AND) predicate " +
              "or as the sole filter; OR semantics cannot be pushed to the FTS index.")
        }

      case and: And =>
        validateNoOrWithFts(and.left)
        validateNoOrWithFts(and.right)

      case other =>
        other.children.foreach(validateNoOrWithFts)
    }
  }

  private def buildFullTextQuery(afe: ApplyFunctionExpression): FullTextQuery = {
    val fnName = afe.function.name()
    val args = afe.children
    if (fnName == LanceMatchFunction.NAME) {
      buildMatchQuery(args)
    } else if (fnName == LancePhraseFunction.NAME) {
      buildPhraseQuery(args)
    } else if (fnName == LanceMultiMatchFunction.NAME) {
      buildMultiMatchQuery(args)
    } else {
      throw new IllegalArgumentException(s"Unsupported Lance FTS function: $fnName")
    }
  }

  private def buildMatchQuery(args: Seq[Expression]): FullTextQuery = {
    val column = extractColumn(args(0), "lance_match")
    val queryText = extractQueryText(args(1), "lance_match")
    if (args.size == 2) {
      FullTextQuery.`match`(queryText, column)
    } else {
      buildMatchQueryWithOptions(queryText, column, args(2))
    }
  }

  private def buildMatchQueryWithOptions(
      queryText: String,
      column: String,
      optsExpr: Expression): FullTextQuery = {
    val optsStr = optsExpr match {
      case Literal(v, _) if v != null => v.toString
      case _ =>
        throw new IllegalArgumentException(
          "lance_match: the options argument must be a string literal " +
            "(e.g. 'fuzziness=1,operator=AND')")
    }

    val optionPairs = parseKeyValueOptions(optsStr, "lance_match", knownMatchOptionKeys)

    var boost: Float = 1.0f
    var fuzziness: Option[Int] = None
    var maxExpansions: Int = 50
    var operator: Operator = Operator.OR
    var prefixLength: Int = 0

    for ((key, value) <- optionPairs) {
      key match {
        case "fuzziness" =>
          fuzziness = Some(parseNonNegativeInt(value, "fuzziness"))
        case "operator" =>
          operator = parseOperator(value, "lance_match")
        case "boost" =>
          try {
            boost = value.toFloat
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(
                s"lance_match: boost must be a number, got '$value'")
          }
        case "prefix_length" =>
          prefixLength = parseNonNegativeInt(value, "prefix_length")
        case "max_expansions" =>
          val parsed =
            try { value.toInt }
            catch {
              case _: NumberFormatException =>
                throw new IllegalArgumentException(
                  s"lance_match: max_expansions must be an integer >= 1, got '$value'")
            }
          if (parsed < 1) {
            throw new IllegalArgumentException(
              s"lance_match: max_expansions must be >= 1, got $parsed")
          }
          maxExpansions = parsed
      }
    }

    val javaFuzziness: Optional[Integer] = fuzziness match {
      case Some(fuzz) => Optional.of(fuzz.asInstanceOf[Integer])
      case None => Optional.empty()
    }

    FullTextQuery.`match`(
      queryText,
      column,
      boost,
      javaFuzziness,
      maxExpansions,
      operator,
      prefixLength)
  }

  private def parseNonNegativeInt(value: String, key: String): Int = {
    try {
      val parsed = value.toInt
      if (parsed < 0) {
        throw new IllegalArgumentException(
          s"lance_match: $key must be a non-negative integer, got $parsed")
      }
      parsed
    } catch {
      case e: IllegalArgumentException => throw e
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"lance_match: $key must be an integer, got '$value'")
    }
  }

  private def buildPhraseQuery(args: Seq[Expression]): FullTextQuery = {
    val column = extractColumn(args(0), "lance_match_phrase")
    val queryText = extractQueryText(args(1), "lance_match_phrase")
    val slop = if (args.size == 3) {
      args(2) match {
        case Literal(v: Int, IntegerType) =>
          if (v < 0) {
            throw new IllegalArgumentException(
              s"lance_match_phrase: slop must be >= 0, got $v")
          }
          v
        case _ =>
          throw new IllegalArgumentException(
            "lance_match_phrase: the slop argument must be a non-negative integer literal")
      }
    } else {
      0
    }
    FullTextQuery.phrase(queryText, column, slop)
  }

  private val knownMultiMatchOptionKeys: Set[String] = Set("operator")

  private def buildMultiMatchQuery(args: Seq[Expression]): FullTextQuery = {
    val queryText = extractQueryText(args(0), "lance_multi_match")

    // Detect optional options string at position 1: a string literal whose content contains '='.
    // Column references resolve as AttributeReference (not Literal), so a Literal containing '='
    // is unambiguously an options string in practice.
    val (operator, colStartIdx) = args(1) match {
      case Literal(v, _) if v != null && isMultiMatchOptions(v.toString) =>
        (parseMultiMatchOptions(v.toString), 2)
      case _ =>
        (Operator.OR, 1)
    }

    if (args.size - colStartIdx < 2) {
      throw new IllegalArgumentException(
        "lance_multi_match requires at least 2 column arguments. " +
          "When using an options string, supply at least: " +
          "lance_multi_match(query, 'operator=AND', col1, col2)")
    }

    val columns = new ArrayList[String]()
    for (i <- colStartIdx until args.size) {
      columns.add(extractColumn(args(i), "lance_multi_match"))
    }
    FullTextQuery.multiMatch(queryText, columns, null, operator)
  }

  // Any arg2 string literal containing '=' is treated as an options string — not a column name.
  // This means unknown keys reach parseMultiMatchOptions which raises IllegalArgumentException early,
  // rather than being silently swallowed as a column name with no diagnostic.
  private def isMultiMatchOptions(s: String): Boolean = s.indexOf('=') >= 0

  private def parseMultiMatchOptions(optsStr: String): Operator = {
    val optionPairs = parseKeyValueOptions(optsStr, "lance_multi_match", knownMultiMatchOptionKeys)
    var operator: Operator = Operator.OR
    for ((key, value) <- optionPairs) {
      key match {
        case "operator" =>
          operator = parseOperator(value, "lance_multi_match")
      }
    }
    operator
  }

  /**
   * Parses a comma-separated 'key=value' options string into a sequence of (key, value) pairs.
   * Validates that each entry has the 'key=value' form and that the key is in the allowed set.
   */
  private def parseKeyValueOptions(
      optsStr: String,
      fnName: String,
      allowedKeys: Set[String]): Seq[(String, String)] = {
    optsStr.split(",").toSeq
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { entry =>
        val eqIdx = entry.indexOf('=')
        if (eqIdx < 0) {
          throw new IllegalArgumentException(
            s"$fnName: malformed option '$entry' — expected 'key=value'")
        }
        val key = entry.substring(0, eqIdx).trim.toLowerCase(Locale.ROOT)
        val value = entry.substring(eqIdx + 1).trim
        if (!allowedKeys.contains(key)) {
          throw new IllegalArgumentException(
            s"$fnName: unknown option key '$key'. " +
              s"Supported keys: ${allowedKeys.mkString(", ")}")
        }
        (key, value)
      }
  }

  private def parseOperator(value: String, fnName: String): Operator = {
    value.toUpperCase(Locale.ROOT) match {
      case "AND" => Operator.AND
      case "OR" => Operator.OR
      case _ =>
        throw new IllegalArgumentException(
          s"$fnName: operator must be AND or OR, got '$value'")
    }
  }

  private def extractColumn(expr: Expression, fnName: String): String = expr match {
    case Literal(v, _) if v != null => v.toString
    case attr: AttributeReference => attr.name
    case _ =>
      throw new IllegalArgumentException(
        s"$fnName: the column argument must be a column reference or string literal")
  }

  private def extractQueryText(expr: Expression, fnName: String): String = expr match {
    case Literal(v, _) if v != null => v.toString
    case _ =>
      throw new IllegalArgumentException(
        s"$fnName: the query argument must be a string literal")
  }

  /**
   * Removes `target` from a conjunction tree, returning the remaining expression or None if
   * the tree reduces to nothing (i.e. the entire condition was the FTS expression).
   */
  private def removeExpression(
      expr: Expression,
      target: ApplyFunctionExpression): Option[Expression] = {
    expr match {
      case `target` => None
      case And(left, right) =>
        (removeExpression(left, target), removeExpression(right, target)) match {
          case (None, r) => r
          case (l, None) => l
          case (Some(l), Some(r)) => Some(And(l, r))
        }
      case other => Some(other)
    }
  }
}
