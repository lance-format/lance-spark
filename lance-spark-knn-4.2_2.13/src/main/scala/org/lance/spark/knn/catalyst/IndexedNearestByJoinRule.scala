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
package org.lance.spark.knn.catalyst

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, EqualTo, Expression, GetStructField, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, VectorCosineSimilarity, VectorInnerProduct, VectorL2Distance}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, NearestByDirection, NearestByDistance, NearestBySimilarity}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, NearestByJoin, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.spark.{LanceDataset, LanceSparkReadOptions}
import org.lance.spark.knn.internal.{LanceKnnJoinStage, Metric}
import org.lance.spark.utils.{BlobUtils, VectorUtils}

/**
 * Catalyst rule that rewrites a Spark [[NearestByJoin]] (`approx = true`) over a Lance scan with
 * a recognized vector-distance ranking expression into a single [[LanceKnnJoinLogicalPlan]],
 * wrapped in a top-level [[Project]] that restores `NearestByJoin.output` exactly. The paired
 * [[LanceKnnJoinStrategy]] then lowers that node to [[LanceKnnJoinExec]], which drives the
 * no-shuffle `LanceKnnJoinStage.runPartition` per-partition probe.
 *
 * == Why this rule must be a `postHocResolutionRule`, not an optimizer rule ==
 *
 * Spark's built-in [[org.apache.spark.sql.catalyst.optimizer.RewriteNearestByJoin]] rule runs in
 * the optimizer's `FinishAnalysis` batch — the very first batch. `injectOptimizerRule` adds
 * rules to `operatorOptimizationBatch`, which runs AFTER `FinishAnalysis`. By the time an
 * injected optimizer rule fires, the `NearestByJoin` operator has already been replaced with the
 * cross-product + `MaxMinByK` rewrite, and we have nothing to pattern-match.
 *
 * `injectPostHocResolutionRule` runs after analysis but before any optimizer batch — it is the
 * only injection point that sees the unrewritten `NearestByJoin`. The same constraint applies to
 * any future engine wanting to substitute a different physical strategy for `NearestByJoin`.
 *
 * == Pattern match ==
 *
 * The rule fires on the conjunction of:
 *  - `NearestByJoin(_, right, joinType, approx = true, k, rankingExpression, direction)`
 *  - `right` resolves to a Lance DSv2 relation (immediate or under a `SubqueryAlias`)
 *  - `rankingExpression` is one of three recognized vector functions, AND its direction matches
 *    the direction declared on `NearestByJoin`:
 *
 *    | Spark expression                | direction              | metric         |
 *    |---------------------------------|------------------------|----------------|
 *    | `VectorL2Distance(L, R)`        | `NearestByDistance`    | `Metric.L2`    |
 *    | `VectorCosineSimilarity(L, R)`  | `NearestBySimilarity`  | `Metric.Cosine`|
 *    | `VectorInnerProduct(L, R)`      | `NearestBySimilarity`  | `Metric.Dot`   |
 *
 *    Any other shape is left alone — Spark's default cross-product rewrite handles it.
 *
 * The two arguments of the ranking function must each resolve to an [[Attribute]] from one
 * specific side of the join. Mixed-side compounds (e.g. `l2_distance(left.vec, left.vec)`) and
 * derived expressions (e.g. `l2_distance(left.vec, slice(right.vec, ...))`) are out of scope and
 * fall through to the cross-product rewrite.
 *
 * == Lance scan detection ==
 *
 * The right relation's V2 table must be a connector [[org.lance.spark.LanceDataset]]. The probe /
 * materialize path drives Lance's Java API directly, so the indexed-path executor is Lance-specific
 * by construction — there's no general "any vector-capable backend" extension point here. From the
 * `LanceDataset` the rule captures the FULL read context: the base `LanceSparkReadOptions`, the
 * driver-side initial storage options, and the runtime namespace impl / properties. The relation's
 * OWN options are captured too — that is where a DataFrame read carries branch / version / storage
 * credentials, since `LanceDataSource` is a `SupportsCatalogOptions` whose identifier is the URI
 * alone. [[org.lance.spark.knn.catalyst.LanceKnnJoinExec]] merges + pins that context once on the
 * driver (as `LanceScanBuilder` does) and opens on executors through `Utils.openDatasetBuilder`.
 *
 * == Prefilter pushdown ==
 *
 * If the right side is a `Filter(cond, lance)` (a `WHERE` clause on the indexed table), the
 * rule translates the predicate to a Lance SQL filter string and threads it through to the
 * probe. Lance applies the filter BEFORE the index lookup (we always pass `prefilter = true`),
 * so the top-K is computed over only the rows matching the filter — the only correct behavior
 * for `right WHERE p APPROX NEAREST K`.
 *
 * Translation is conservative: it handles binary comparisons (=, !=, <, <=, >, >=), `IN`,
 * `IS [NOT] NULL`, `AND`/`OR`/`NOT` over right-side columns (top-level or nested struct fields)
 * vs. literals. Anything else
 * (UDFs, subqueries, computed expressions) means the rule REFUSES the rewrite and returns the
 * original `NearestByJoin`, falling through to Spark's brute-force cross-product. Refusal — not
 * "push what we can, drop the rest" — because dropping a residual would silently change result
 * semantics. The job becomes slow rather than wrong.
 *
 * Filter pushdown into the V2 relation does NOT happen at this point: this rule runs as a
 * `postHocResolutionRule` (before the optimizer), so the right side is still the freshly
 * analyzed `Filter` over `DataSourceV2Relation` — the V2 `SupportsPushDownFilters` step has not
 * yet run. After we rewrite, the right side is absorbed into our plan, so V2 pushdown never
 * gets a chance to drop the filter on the floor.
 */
object IndexedNearestByJoinRule extends Rule[LogicalPlan] {

  /** Configuration key that gates the rule. Off by default to keep the rule opt-in for now. */
  val EnabledConfKey: String = "spark.lance.knn.indexedNearestByJoin.enabled"

  /**
   * IVF cluster count to visit per query. Higher = better recall, more compute. Default
   * (None) leaves Lance's index-default (typically 1).
   */
  val NprobesConfKey: String = "spark.lance.knn.nprobes"

  /**
   * IVF-PQ refine factor — Lance fetches `K * refineFactor` PQ candidates and re-ranks them
   * with exact distance using full vectors. Highest-leverage recall knob for IVF-PQ. Default
   * (None) leaves Lance's index-default (= 1, no re-rank).
   */
  val RefineFactorConfKey: String = "spark.lance.knn.refineFactor"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConfString(EnabledConfKey, "false").toBoolean) {
      return plan
    }
    val nprobes = optInt(NprobesConfKey)
    val refineFactor = optInt(RefineFactorConfKey)
    plan.transformDown {
      case j @ NearestByJoin(left, right, joinType, true, k, rankingExpr, direction)
          if preservesAnalysisGuards(left, right) =>
        rewriteIfApplicable(
          j,
          left,
          right,
          joinType,
          k,
          rankingExpr,
          direction,
          nprobes,
          refineFactor).getOrElse(j)
    }
  }

  /**
   * This rule runs as a `postHocResolutionRule`, i.e. BEFORE the optimizer's `FinishAnalysis` batch
   * (which lowers `NearestByJoin` into a Cartesian product + `MaxMinByK`) and BEFORE the checks that
   * batch relies on. Replacing a `NearestByJoin` that one of those checks would REJECT would let an
   * illegal query succeed silently. So we decline the rewrite — leaving the original `NearestByJoin`
   * in place for Spark's own path to reject with the exact error the user expects — whenever a
   * standard analysis guard would fire:
   *
   *   - `spark.sql.crossJoin.enabled = false`: a `NearestByJoin` carries no equi-condition, so its
   *     default lowering is a Cartesian product, which `CheckCartesianProducts` rejects with
   *     `CROSS_JOIN_NOT_ENABLED`. Reading `conf.crossJoinEnabled` here uses the same value (and
   *     default) that check uses.
   *   - a streaming child: `NearestByJoin` over a streaming input is unsupported
   *     (`STREAMING_NOT_SUPPORTED`); leave it for Spark's unsupported-operation check to reject.
   */
  private def preservesAnalysisGuards(left: LogicalPlan, right: LogicalPlan): Boolean =
    conf.crossJoinEnabled && !left.isStreaming && !right.isStreaming

  private def optInt(key: String): Option[Int] =
    Option(conf.getConfString(key, null)).map(_.toInt)

  /**
   * Rewrite `NearestByJoin` into a single [[LanceKnnJoinLogicalPlan]] carrying the
   * [[LanceKnnJoinStage.Conf]] the executor runs per partition — the same stage the DataFrame
   * API path drives:
   *
   * {{{
   *   Project(j.output, drop __score)
   *   +- LanceKnnJoinLogicalPlan          output = left ++ right ++ __score
   *      +- left
   * }}}
   *
   * We add a top-level `Project` because `NearestByJoin.output` is `left ++ right` (no
   * score), but the join node emits `left ++ right ++ __score`. The Project slices the trailing
   * score attribute — Catalyst's ColumnPruning won't interfere because
   * `LanceKnnJoinLogicalPlan` overrides `references = child.outputSet`.
   */
  private def rewriteIfApplicable(
      j: NearestByJoin,
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      k: Int,
      rankingExpr: Expression,
      direction: NearestByDirection,
      nprobes: Option[Int],
      refineFactor: Option[Int]): Option[LogicalPlan] = {
    for {
      (metric, leftVecAttr, rightVecCol) <- recognizeRanking(rankingExpr, direction, left, right)
      lance <- unwrapLanceScan(right)
      if !hasBlobColumn(lance.output)
    } yield {
      val leftVecIdx = left.output.indexWhere(_.exprId == leftVecAttr.exprId)
      require(leftVecIdx >= 0, s"left vector attr not found in left.output: $leftVecAttr")

      val rightFields: Seq[StructField] =
        lance.output.map(a => StructField(a.name, a.dataType, nullable = true))
      val rightProjection: Seq[String] = lance.output.map(_.name)

      val stageConf = LanceKnnJoinStage.Conf(
        readOptions = lance.readOptions,
        relationOptions = lance.relationOptions,
        initialStorageOptions = lance.initialStorageOptions,
        namespaceImpl = lance.namespaceImpl,
        namespaceProperties = lance.namespaceProperties,
        vectorColumn = rightVecCol,
        metric = metric,
        k = k,
        internalK = k, // no overfetch on the SQL path
        nprobes = nprobes,
        refineFactor = refineFactor,
        ef = None,
        prefilter = lance.prefilter,
        leftVecIdx = leftVecIdx,
        rightProjection = rightProjection,
        rightFields = rightFields,
        leftFieldCount = left.output.size,
        outerJoin = joinType == LeftOuter,
        smallerIsBetter = metric.smallerIsBetter)

      // The join node emits left ++ right ++ __score. The SQL output is j.output (= left ++ right,
      // no score). Set finalOutput = j.output :+ scoreAttr so the node's output is stable; the
      // top-level Project drops __score.
      //
      // `NearestByJoin.output` widens every left+right attribute to `nullable = true` — a contract
      // the base Spark rewrite also honors. `finalSchema` feeds the `ExpressionEncoder` in
      // `LanceKnnJoinExec.doExecute`; if we left left fields at raw `nullable = false` while the
      // logical output declares them nullable, the encoder's binary layout would drift from what
      // downstream consumers expect. Widen left here to keep the encoder consistent.
      val leftSchemaStruct = StructType(
        left.output.map(a => StructField(a.name, a.dataType, a.nullable)))
      val scoreAttr = AttributeReference("__score", FloatType, nullable = true)()
      val finalSchema = StructType(
        leftSchemaStruct.fields.map(_.copy(nullable = true)) ++
          rightFields.map(f => f.copy(nullable = true)) :+
          StructField("__score", FloatType, nullable = true))
      val finalOutput: Seq[Attribute] = j.output :+ scoreAttr

      val node = LanceKnnJoinLogicalPlan(
        child = left,
        stageConf = stageConf,
        leftSchema = leftSchemaStruct,
        finalSchema = finalSchema,
        finalOutput = finalOutput)

      // Top-level Project drops the __score attr so the plan's external output matches
      // NearestByJoin.output exactly.
      Project(j.output, node)
    }
  }

  /**
   * Lance scan context extracted from a DSv2 relation, optionally with a translated prefilter. Carries
   * the full read context so the executor can merge + pin + open exactly as the connector's scan path
   * does — see [[LanceKnnJoinStage.resolveReadContext]]. `relationOptions` is where a DataFrame read
   * carries branch / version / storage credentials (the `SupportsCatalogOptions` identifier is the URI
   * alone), so it must be captured alongside the base `readOptions`.
   */
  final private case class LanceScanInfo(
      readOptions: LanceSparkReadOptions,
      relationOptions: java.util.Map[String, String],
      initialStorageOptions: java.util.Map[String, String],
      namespaceImpl: String,
      namespaceProperties: java.util.Map[String, String],
      output: Seq[Attribute],
      prefilter: Option[String])

  private def unwrapLanceScan(plan: LogicalPlan): Option[LanceScanInfo] = plan match {
    case SubqueryAlias(_, child) => unwrapLanceScan(child)
    case v: org.apache.spark.sql.catalyst.plans.logical.View =>
      // SQL `createOrReplaceTempView` + `spark.sql(... FROM <view> ...)` wraps the underlying
      // DataSourceV2Relation in a `View`. Unwrap to find the actual relation underneath.
      unwrapLanceScan(v.children.head)
    case Filter(cond, child) =>
      // Right-side `WHERE` clause. Recurse first so we have the relation's output to validate
      // attribute references against, then translate the predicate. If translation fails we
      // bail entirely (return None, no rewrite) — pushing only PART of a `WHERE` would silently
      // change query semantics. The user's filter must be pushed in full or not at all.
      unwrapLanceScan(child).flatMap { info =>
        translateFilter(cond, AttributeSet(info.output)).map { sql =>
          val combined = info.prefilter match {
            case Some(prev) => Some(s"($prev) AND ($sql)")
            case None => Some(sql)
          }
          info.copy(prefilter = combined)
        }
      }
    case Project(projectList, child) if isPassthroughProject(projectList, child) =>
      // `SELECT * FROM lance` analyzes to `Project(<child.output>, lance)` — a pass-through
      // that preserves attrs and exprIds. Unwrap it. Non-pass-through Projects (renames, drops,
      // computed columns) would change the schema we rely on for `j.output` mapping, so we
      // refuse those by falling through to the default `_ => None` case.
      unwrapLanceScan(child)
    case rel: DataSourceV2Relation if rel.table.isInstanceOf[LanceDataset] =>
      // The probe / materialize path drives Lance's Java API directly, so this rule is
      // Lance-specific by construction — there's no plug-in point for a non-Lance backend here.
      // Capture the FULL read context from the connector's `LanceDataset` (base read options +
      // runtime namespace) PLUS the relation's own options, which is where a DataFrame read carries
      // branch / version / storage credentials (`LanceDataSource` is a `SupportsCatalogOptions`
      // whose identifier is the URI alone). `LanceKnnJoinExec.doExecute` merges + pins these exactly
      // as `LanceScanBuilder` does. Namespace / storage maps are copied into fresh serializable
      // HashMaps so they ship cleanly to executors inside the stage Conf.
      val ds = rel.table.asInstanceOf[LanceDataset]
      Some(
        LanceScanInfo(
          readOptions = ds.readOptions(),
          relationOptions = new java.util.HashMap[String, String](rel.options.asCaseSensitiveMap()),
          initialStorageOptions = Option(ds.getInitialStorageOptions())
            .map(new java.util.HashMap[String, String](_))
            .orNull,
          namespaceImpl = ds.getNamespaceImpl(),
          namespaceProperties = Option(ds.getNamespaceProperties())
            .map(new java.util.HashMap[String, String](_))
            .orNull,
          output = rel.output,
          prefilter = None))
    case _ => None
  }

  /**
   * Translate a Spark `Filter` predicate into a Lance SQL filter string. Returns `None` if any
   * sub-expression isn't supported — refusal, not partial pushdown.
   *
   * Supported shapes, where `attr` is a right-side top-level column OR a nested struct field
   * (the latter rendered as a dotted path `col.field`, arbitrarily deep). Array/map element
   * access (`col[i]`) is NOT supported and falls through to refusal:
   *   - `attr <op> literal` and `literal <op> attr` for `=`, `!=`, `<`, `<=`, `>`, `>=`
   *   - `attr IS NULL` / `attr IS NOT NULL`
   *   - `attr IN (lit, lit, ...)`  (the IN list must be all foldable literals)
   *   - `AND` / `OR` over supported children
   *   - `NOT` over supported child
   *
   * Anything else — UDFs, joins, subqueries, expressions on both sides referencing the LEFT
   * input, computed sub-expressions on the right (e.g. `year(ts) = 2025`) — returns `None`.
   * Lance's SQL dialect is DataFusion-flavored; the constructs above all parse identically
   * there, so we don't need to translate operator names beyond literal serialization.
   */
  private[catalyst] def translateFilter(
      expr: Expression,
      rightAttrs: AttributeSet): Option[String] = expr match {
    case And(l, r) =>
      for {
        a <- translateFilter(l, rightAttrs)
        b <- translateFilter(r, rightAttrs)
      } yield s"($a) AND ($b)"
    case Or(l, r) =>
      for {
        a <- translateFilter(l, rightAttrs)
        b <- translateFilter(r, rightAttrs)
      } yield s"($a) OR ($b)"
    case Not(EqualTo(l, r)) =>
      // Render `NOT (a = b)` as `(a != b)` so it's the natural Lance form.
      binaryOp(l, r, rightAttrs, "!=")
    case Not(child) =>
      translateFilter(child, rightAttrs).map(s => s"NOT ($s)")
    case IsNull(c) =>
      asRightColumn(c, rightAttrs).map(name => s"$name IS NULL")
    case IsNotNull(c) =>
      asRightColumn(c, rightAttrs).map(name => s"$name IS NOT NULL")
    case EqualTo(l, r) => binaryOp(l, r, rightAttrs, "=")
    case GreaterThan(l, r) => binaryOp(l, r, rightAttrs, ">")
    case GreaterThanOrEqual(l, r) => binaryOp(l, r, rightAttrs, ">=")
    case LessThan(l, r) => binaryOp(l, r, rightAttrs, "<")
    case LessThanOrEqual(l, r) => binaryOp(l, r, rightAttrs, "<=")
    case In(value, list) if list.nonEmpty =>
      for {
        col <- asRightColumn(value, rightAttrs)
        lits <- list.foldLeft(Option(Vector.empty[String])) { (accOpt, e) =>
          accOpt.flatMap(acc => asLiteral(e).map(acc :+ _))
        }
      } yield s"$col IN (${lits.mkString(", ")})"
    case _ => None
  }

  private def binaryOp(
      l: Expression,
      r: Expression,
      rightAttrs: AttributeSet,
      op: String): Option[String] = {
    // attr <op> literal — the natural shape
    val attrLit = for {
      col <- asRightColumn(l, rightAttrs)
      lit <- asLiteral(r)
    } yield s"$col $op $lit"
    // literal <op> attr — flip when the parser/optimizer emitted args in this order. Renders
    // as `lit op col`, which DataFusion also accepts.
    attrLit.orElse {
      for {
        col <- asRightColumn(r, rightAttrs)
        lit <- asLiteral(l)
      } yield s"$lit $op $col"
    }
  }

  private def asRightColumn(e: Expression, rightAttrs: AttributeSet): Option[String] = e match {
    case a: Attribute if rightAttrs.contains(a) => Some(quoteIdentifier(a.name))
    case g: GetStructField =>
      // Nested struct field: render as a dotted path `col.field` (recursing so `a.b.c` works).
      // Lance's filter dialect treats a dotted identifier as a nested column path — its scan
      // planner runs with enable_relations = false — so this maps 1:1. Each path segment is quoted
      // independently. The recursion also gates on the ROOT resolving to a right-side attribute, so
      // a left-side or foreign root refuses.
      val fieldName = g.name.getOrElse(g.childSchema(g.ordinal).name)
      asRightColumn(g.child, rightAttrs).map(base => s"$base.${quoteIdentifier(fieldName)}")
    case _ => None
  }

  /**
   * Render a column identifier for a Lance filter by ALWAYS back-quoting it (any embedded backtick
   * doubled). A bare identifier is not merely a readability question — it is ambiguous with SQL
   * keywords and literals: a column literally named `true`, `null`, or `select` emitted bare parses
   * as the keyword/literal, not a column reference (`true = true` is a tautology, not `col = true`),
   * silently corrupting the prefilter. A "quote only the non-word identifiers" exception cannot
   * enumerate every such reserved word, so we quote unconditionally — a delimited identifier is
   * unambiguous for every name.
   *
   * The quote character is a BACKTICK, not a double-quote. Lance's filter dialect is MySQL-flavored:
   * a double-quoted token is a STRING LITERAL, so `"category" = 'A'` parses as the constant
   * comparison `'category' = 'A'` (always false) and silently prefilters to zero rows — verified
   * against a real Lance dataset. Backticks delimit an identifier. Applied per dotted-path segment,
   * so a nested field `outer.inner` becomes `` `outer`.`inner` ``.
   */
  private def quoteIdentifier(name: String): String =
    "`" + name.replace("`", "``") + "`"

  /**
   * Render a Spark literal as a Lance SQL literal. Dispatch is by `dataType`, NOT by the boxed
   * value class — Catalyst stores e.g. `Literal(0, DateType)` with the value as a plain `Int`,
   * so a value-class match would silently let a date literal through as the integer "0", a
   * recall-corrupting mistranslation.
   *
   * Supports nulls, booleans, numeric primitives, and strings (with `'`-escaped quoting). Bails
   * on dates, timestamps, decimals, binary, arrays, structs — those have non-trivial cross-
   * dialect renderings and we'd rather refuse pushdown than risk a wrong filter.
   */
  private def asLiteral(e: Expression): Option[String] = e match {
    case Literal(null, _) => Some("NULL")
    case Literal(v, BooleanType) => Some(v.toString)
    case Literal(v, ByteType) => Some(v.toString)
    case Literal(v, ShortType) => Some(v.toString)
    case Literal(v, IntegerType) => Some(v.toString)
    case Literal(v, LongType) => Some(v.toString)
    case Literal(v, FloatType) => Some(v.toString)
    case Literal(v, DoubleType) => Some(v.toString)
    case Literal(v: UTF8String, StringType) => Some(quoteString(v.toString))
    case Literal(v: String, StringType) => Some(quoteString(v))
    case _ => None
  }

  private def quoteString(s: String): String = "'" + s.replace("'", "''") + "'"

  /**
   * True iff the Project is the canonical `SELECT *` form: same number of outputs as the child,
   * each entry a bare `AttributeReference` whose `exprId` matches the child's output in order.
   * Any aliasing, reordering, dropping, or computed column — return false and refuse to
   * unwrap, since those change the schema we'd surface as the join's right-side output.
   */
  private def isPassthroughProject(
      projectList: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression],
      child: LogicalPlan): Boolean = {
    val childOut = child.output
    if (projectList.size != childOut.size) return false
    projectList.zip(childOut).forall {
      case (a: Attribute, c) => a.exprId == c.exprId
      case _ => false
    }
  }

  /**
   * Recognize `rankingExpr` as one of the supported vector-distance functions, AND verify the
   * declared `direction` on `NearestByJoin` matches the function's natural ordering.
   *
   * Returns `(metric, leftVecAttr, rightVecColName)` on success.
   */
  private def recognizeRanking(
      rankingExpr: Expression,
      direction: NearestByDirection,
      left: LogicalPlan,
      right: LogicalPlan): Option[(Metric, Attribute, String)] = {
    val (metric, lhs, rhs) = rankingExpr match {
      case VectorL2Distance(l, r) if direction == NearestByDistance => (Metric.L2, l, r)
      case VectorCosineSimilarity(l, r) if direction == NearestBySimilarity => (Metric.Cosine, l, r)
      case VectorInnerProduct(l, r) if direction == NearestBySimilarity => (Metric.Dot, l, r)
      case _ => return None
    }
    // Each argument must be a bare attribute from one side of the join. The RIGHT (Lance) argument
    // must additionally be a real fixed-size vector column — see [[isFixedSizeVector]] — otherwise
    // there is nothing for the index to probe and we must fall through to the brute-force path.
    (asAttr(lhs), asAttr(rhs)) match {
      case (Some(la), Some(ra)) =>
        val leftAttrIds = left.outputSet
        val rightAttrIds = right.outputSet
        if (leftAttrIds.contains(la) && rightAttrIds.contains(ra) && isFixedSizeVector(ra)) {
          Some((metric, la, ra.name))
        } else if (leftAttrIds.contains(ra) && rightAttrIds.contains(la) && isFixedSizeVector(la)) {
          // Argument order swapped — still valid for symmetric metrics. All three of L2/Cosine/Dot
          // are symmetric so we don't have to retain the original orientation.
          Some((metric, ra, la.name))
        } else {
          None
        }
      case _ => None
    }
  }

  private def asAttr(e: Expression): Option[Attribute] = e match {
    case a: Attribute => Some(a)
    case _ => None
  }

  /**
   * True iff `attr` is the connector's canonical fixed-size vector shape: an `ArrayType` of float /
   * double carrying the `arrow.fixed-size-list.size` metadata the connector stamps on a Lance
   * `FixedSizeList` column. A plain variable-length `List<Float>` analyzes to the SAME Spark
   * `ArrayType(FloatType)` but WITHOUT that metadata and is NOT index-searchable — probing it would
   * fail at run time. Gating the rewrite on this marker (reusing the connector's own
   * [[org.lance.spark.utils.VectorUtils.isVectorField]] so the definition lives in exactly one
   * place) lets a non-vector right column fall through to Spark's brute-force path instead of
   * producing a broken operator. A right attribute that is not an `AttributeReference` (so carries
   * no field metadata) is conservatively treated as non-vector.
   */
  private def isFixedSizeVector(attr: Attribute): Boolean = attr match {
    case ref: AttributeReference =>
      VectorUtils.isVectorField(StructField(ref.name, ref.dataType, ref.nullable, ref.metadata))
    case _ => false
  }

  /**
   * True iff any column in the Lance relation output is a blob column — legacy v1
   * (`lance-encoding:blob` metadata on a `BinaryType`) or v2 (`ARROW:extension:name = lance.blob.v2`
   * metadata on the descriptor struct). Reuses the connector's own
   * [[org.lance.spark.utils.BlobUtils.isBlobReadColumn]] so the marker definition lives in one place;
   * both markers survive into the relation's `AttributeReference.metadata`.
   *
   * When present, the rule DECLINES the rewrite and falls through to Spark's brute-force cross-
   * product. Blob columns are late-materialized: the connector's canonical reader
   * ([[org.lance.spark.internal.LanceFragmentColumnarBatchScanner]]) threads the dataset URI, column
   * name, and row addresses into `BlobStructAccessor.setBlobReferenceContext` so the descriptor can
   * be resolved to its payload. The no-shuffle probe path fetches only `_rowid` and wraps vectors
   * without that context, so a non-null legacy blob would resolve to an empty payload. Declining is
   * the conservative, always-correct choice: Spark's own scan (with full blob context) returns the
   * true payload. A non-`AttributeReference` attribute carries no field metadata and is treated as
   * non-blob.
   */
  private def hasBlobColumn(output: Seq[Attribute]): Boolean =
    output.exists {
      case ref: AttributeReference =>
        BlobUtils.isBlobReadColumn(
          StructField(ref.name, ref.dataType, ref.nullable, ref.metadata))
      case _ => false
    }
}
