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

import org.apache.spark.sql.{RowFactory, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Add, And, Attribute, AttributeSet, EqualTo, Expression, GetStructField, GreaterThan, In, IsNotNull, IsNull, LessThanOrEqual, Literal, Not, Or, VectorCosineSimilarity, VectorInnerProduct, VectorL2Distance}
import org.apache.spark.sql.catalyst.plans.{NearestByDistance, NearestBySimilarity}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, NearestByJoin, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.lance.spark.knn.internal.Metric

import java.nio.file.Path

import scala.collection.JavaConverters._

/**
 * Unit tests for [[IndexedNearestByJoinRule]]. The rule's responsibility is purely Catalyst-side
 * pattern-matching — we don't need a Lance backend to exercise it. Each test constructs a small
 * resolved plan and runs the rule, asserting either a rewrite to
 * `Project(..., LanceKnnJoinLogicalPlan(left, ...))` or a no-op fallthrough.
 *
 * Coverage:
 *  - Happy path: VectorL2Distance + NearestByDistance over a Lance DSv2 relation rewrites.
 *  - Direction mismatch (e.g. L2 distance with NearestBySimilarity) does NOT rewrite.
 *  - EXACT (`approx = false`) does NOT rewrite — Spark's brute-force keeps owning that path.
 *  - Non-Lance right side does NOT rewrite (right relation's table is not a `LanceDataset`).
 *  - Disabled by default — fires only when the gating config is set.
 *  - Prefilter pushdown: right-side `WHERE` translates to a Lance SQL filter string, or refuses
 *    the rewrite entirely when the predicate can't be pushed in full.
 *
 * The rule's runtime behavior beyond the rewrite (probe execution against real Lance) is covered
 * by the oracle tests in lance-spark-knn_2.12 and the e2e test in this module.
 */
class IndexedNearestByJoinRuleTest {

  @TempDir var tempDir: Path = _
  private var spark: SparkSession = _

  @BeforeEach def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("indexed-nearest-by-join-rule-test")
      .master("local[2]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  }

  @AfterEach def teardown(): Unit = if (spark != null) spark.stop()

  /** L2 + NearestByDistance + Lance scan + enabled config → rewrite. */
  @Test def testL2RewritesToIndexedPlan(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val join = NearestByJoin(
      left = left,
      right = right,
      joinType = Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    val plan = expectRewritten(rewritten)
    assertEquals(Metric.L2, plan.metric)
    assertEquals(5, plan.k)
    assertEquals(rightVec.name, plan.rightVecCol)
    assertEquals(leftVec.exprId, plan.leftVecAttr.exprId)
  }

  /** Cosine similarity + NearestBySimilarity → rewrite. */
  @Test def testCosineRewrites(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "cosine")
    val join = NearestByJoin(
      left,
      right,
      Inner,
      approx = true,
      numResults = 3,
      rankingExpression = VectorCosineSimilarity(leftVec, rightVec),
      direction = NearestBySimilarity)
    val rewritten = IndexedNearestByJoinRule(join)
    assertEquals(Metric.Cosine, expectRewritten(rewritten).metric)
  }

  /** Inner product + NearestBySimilarity → rewrite as Dot. */
  @Test def testDotRewrites(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "dot")
    val join = NearestByJoin(
      left,
      right,
      Inner,
      approx = true,
      numResults = 4,
      rankingExpression = VectorInnerProduct(leftVec, rightVec),
      direction = NearestBySimilarity)
    val rewritten = IndexedNearestByJoinRule(join)
    assertEquals(Metric.Dot, expectRewritten(rewritten).metric)
  }

  /** L2 distance with NearestBySimilarity is inconsistent — rule should NOT fire. */
  @Test def testDirectionMismatchDoesNotRewrite(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val join = NearestByJoin(
      left,
      right,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestBySimilarity)
    val rewritten = IndexedNearestByJoinRule(join)
    assertSame(join, rewritten, "rule should not fire on direction/metric mismatch")
  }

  /** EXACT mode (approx = false) is owned by Spark's brute-force rewrite. */
  @Test def testExactModeDoesNotRewrite(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val join = NearestByJoin(
      left,
      right,
      Inner,
      approx = false,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    assertSame(join, rewritten, "EXACT queries must not be intercepted")
  }

  /** Disabled flag (default) → no rewrite even when otherwise applicable. */
  @Test def testDisabledByDefault(): Unit = {
    spark.conf.unset(IndexedNearestByJoinRule.EnabledConfKey)
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val join = NearestByJoin(
      left,
      right,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    assertSame(join, rewritten, "rule must be opt-in")
  }

  /** Non-Lance right side (regular DataFrame as Project, no DSv2 relation) → no rewrite. */
  @Test def testNonLanceRightDoesNotRewrite(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val left = trivialPlan("lid", "lvec")
    val right = trivialPlan("rid", "rvec")
    val leftVec = left.output.find(_.name == "lvec").get
    val rightVec = right.output.find(_.name == "rvec").get
    val join = NearestByJoin(
      left,
      right,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    assertSame(join, rewritten, "non-Lance right must fall through")
  }

  /** Right side wrapped in SubqueryAlias still rewrites — alias unwrapping happens in the rule. */
  @Test def testSubqueryAliasOnRightStillRewrites(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val aliased = SubqueryAlias("d", right)
    val join = NearestByJoin(
      left,
      aliased,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    // Rule emits `Project(j.output, LanceKnnJoinLogicalPlan(left, ...))`. Asserting on the top
    // Project wrapping the join node is enough for the "did the rule fire" check.
    assertTrue(
      rewritten.isInstanceOf[Project] &&
        rewritten.asInstanceOf[Project].child.isInstanceOf[LanceKnnJoinLogicalPlan],
      s"expected Project(..., LanceKnnJoinLogicalPlan(...)), got: " +
        s"${rewritten.getClass.getSimpleName}")
  }

  // -- prefilter pushdown -------------------------------------------------------------------

  /**
   * Right side wrapped in `Filter(simple predicate)` rewrites AND the predicate lands on the
   * indexed plan as a Lance SQL filter string. The filter must be pushed in full (not dropped)
   * for the result to be semantically equivalent to the original plan.
   */
  @Test def testFilterOverLancePushesAsPrefilter(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val category = right.output.find(_.name == "category").get
    val bucket = right.output.find(_.name == "bucket").get
    val cond = And(
      EqualTo(category, Literal(UTF8String.fromString("A"), StringType)),
      GreaterThan(bucket, Literal(5, IntegerType)))
    val filtered = Filter(cond, right)
    val join = NearestByJoin(
      left,
      filtered,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    val plan = expectRewritten(rewritten)
    assertTrue(plan.prefilter.isDefined, "prefilter should be populated")
    val sql = plan.prefilter.get
    assertTrue(sql.contains("category"), s"prefilter missing column ref: $sql")
    assertTrue(sql.contains("'A'"), s"prefilter missing string literal: $sql")
    assertTrue(sql.contains("bucket"), s"prefilter missing column ref: $sql")
    assertTrue(sql.contains("> 5"), s"prefilter missing numeric comparison: $sql")
    assertTrue(sql.contains("AND"), s"prefilter missing conjunction: $sql")
  }

  /**
   * Predicate touches a left-side attribute — translator can't safely render that as a Lance
   * SQL string (Lance only sees the right table's columns). Rule must REFUSE the rewrite, not
   * drop the predicate. We verify the original `NearestByJoin` is returned unchanged.
   */
  @Test def testPredicateReferencingLeftAttrRefusesRewrite(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val lid = left.output.find(_.name == "lid").get
    val cond = EqualTo(lid, Literal(0, IntegerType))
    val filtered = Filter(cond, right)
    val join = NearestByJoin(
      left,
      filtered,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    assertSame(
      join,
      rewritten,
      "predicate touching left side must refuse pushdown — not partial-push")
  }

  /**
   * Predicate is a computed expression (e.g. `bucket + 1 = 6`), not a bare attr-vs-literal
   * comparison. Translator returns None, rule refuses.
   */
  @Test def testComputedPredicateRefusesRewrite(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val bucket = right.output.find(_.name == "bucket").get
    val cond = EqualTo(Add(bucket, Literal(1, IntegerType)), Literal(6, IntegerType))
    val filtered = Filter(cond, right)
    val join = NearestByJoin(
      left,
      filtered,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    assertSame(join, rewritten, "computed expression must refuse pushdown")
  }

  /** Filter wrapped in SubqueryAlias still pushes — order of unwrap shouldn't matter. */
  @Test def testFilterUnderSubqueryAliasPushes(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val (left, leftVec, right, rightVec) = buildPlans(metricFunction = "l2")
    val category = right.output.find(_.name == "category").get
    val cond = EqualTo(category, Literal(UTF8String.fromString("X"), StringType))
    val plan = SubqueryAlias("d", Filter(cond, right))
    val join = NearestByJoin(
      left,
      plan,
      Inner,
      approx = true,
      numResults = 3,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val rewritten = IndexedNearestByJoinRule(join)
    val p = expectRewritten(rewritten)
    assertTrue(p.prefilter.isDefined, s"prefilter should be set; got ${p.prefilter}")
  }

  // -- predicate translator unit tests -----------------------------------------------------

  /**
   * Direct unit tests on `translateFilter` to lock in the supported shapes. Uses a synthetic
   * AttributeSet so we don't need a logical plan.
   */
  @Test def testTranslatorHandlesSupportedShapes(): Unit = {
    val rid = makeAttr("rid", IntegerType)
    val category = makeAttr("category", StringType)
    val bucket = makeAttr("bucket", IntegerType)
    val meta =
      makeAttr("meta", new StructType().add("category", StringType).add("bucket", IntegerType))
    val attrs = AttributeSet(Seq(rid, category, bucket, meta))

    val cases: Seq[(Expression, String)] = Seq(
      EqualTo(category, lit("A")) -> "category = 'A'",
      Not(EqualTo(category, lit("A"))) -> "category != 'A'",
      GreaterThan(bucket, lit(5)) -> "bucket > 5",
      LessThanOrEqual(bucket, lit(5)) -> "bucket <= 5",
      IsNull(category) -> "category IS NULL",
      IsNotNull(category) -> "category IS NOT NULL",
      In(bucket, Seq(lit(1), lit(2), lit(3))) -> "bucket IN (1, 2, 3)",
      And(EqualTo(category, lit("A")), GreaterThan(bucket, lit(5))) ->
        "(category = 'A') AND (bucket > 5)",
      Or(EqualTo(category, lit("A")), EqualTo(category, lit("B"))) ->
        "(category = 'A') OR (category = 'B')",
      // String-literal escape — single quotes inside the value get doubled.
      EqualTo(category, lit("O'Brien")) -> "category = 'O''Brien'",
      // literal-on-left flip
      EqualTo(lit(5), bucket) -> "5 = bucket",
      // nested struct field access -> dotted path (distinct from the top-level `category` attr)
      EqualTo(GetStructField(meta, 0, Some("category")), lit("A")) -> "meta.category = 'A'")
    cases.foreach { case (expr, expected) =>
      val got = IndexedNearestByJoinRule.translateFilter(expr, attrs)
      assertEquals(Some(expected), got, s"translation mismatch for: $expr")
    }
  }

  /** Translator must return None for unsupported expressions so the rule refuses pushdown. */
  @Test def testTranslatorRefusesUnsupportedShapes(): Unit = {
    val rid = makeAttr("rid", IntegerType)
    val ts = makeAttr("ts", DateType) // date literals not in our supported set
    val foreignMeta = makeAttr("fmeta", new StructType().add("category", StringType))
    val attrs = AttributeSet(Seq(rid, ts))

    val rejected: Seq[Expression] = Seq(
      // Two attributes — no literal — translator can't render `attr op attr` safely (Lance can,
      // but we don't promise it; refuse to keep the rule conservative).
      EqualTo(rid, makeAttr("rid2", IntegerType)),
      // Foreign attribute (not in `attrs`) — translator must reject.
      EqualTo(makeAttr("foreign", IntegerType), lit(1)),
      // Empty IN list.
      In(rid, Seq.empty),
      // Date literal — out of supported types.
      EqualTo(ts, Literal(0, DateType)),
      // Nested struct field over a FOREIGN root attr (not in `attrs`) — the recursion must gate
      // on the root and refuse. (Array/map element access like `col[i]` refuses the same way,
      // via the translator's catch-all.)
      EqualTo(GetStructField(foreignMeta, 0, Some("category")), lit("A")))
    rejected.foreach { e =>
      assertEquals(
        None,
        IndexedNearestByJoinRule.translateFilter(e, attrs),
        s"expected refusal for: $e")
    }
  }

  /**
   * Identifiers that aren't plain `[A-Za-z_][A-Za-z0-9_]*` — spaces, punctuation — must be
   * double-quoted (embedded quotes doubled) so the Lance filter string is well-formed rather than
   * malformed. Plain identifiers stay bare (covered by the shapes test above). Applies per
   * dotted-path segment for nested struct fields.
   */
  @Test def testTranslatorQuotesUnsafeIdentifiers(): Unit = {
    val spaced = makeAttr("weird col", StringType)
    val quoteName = makeAttr("has\"quote", IntegerType)
    val outer = makeAttr("outer", new StructType().add("inner field", StringType))
    val attrs = AttributeSet(Seq(spaced, quoteName, outer))

    val cases: Seq[(Expression, String)] = Seq(
      EqualTo(spaced, lit("A")) -> "\"weird col\" = 'A'",
      GreaterThan(quoteName, lit(5)) -> "\"has\"\"quote\" > 5",
      // nested field with a space -> only the unsafe segment gets quoted
      EqualTo(GetStructField(outer, 0, Some("inner field")), lit("A")) ->
        "outer.\"inner field\" = 'A'")
    cases.foreach { case (expr, expected) =>
      assertEquals(
        Some(expected),
        IndexedNearestByJoinRule.translateFilter(expr, attrs),
        s"identifier quoting mismatch for: $expr")
    }
  }

  /**
   * A DataFrame read carries branch / version / storage credentials in the DSv2 RELATION options,
   * not the base read options (`LanceDataSource` is a `SupportsCatalogOptions` whose identifier is
   * the URI alone). The rule must capture `rel.options` into the stage `Conf` so the executor can
   * merge + pin them; capturing only the base read options would silently read `main` HEAD without
   * credentials — the exact bug this regression guards against.
   */
  @Test def testCapturesBranchAndStorageOptionsFromRelation(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
    val left = trivialPlan("lid", "lvec")
    val schema = new StructType(Array(
      StructField("rid", IntegerType, nullable = false),
      StructField("rvec", ArrayType(FloatType, containsNull = false), nullable = false)))
    val uri = tempDir.resolve("branch_lance").toString
    val table = new FakeLanceTable(schema, uri)
    val opts = new java.util.HashMap[String, String]()
    opts.put("path", uri)
    opts.put("branch", "frozen")
    opts.put("storage.account_key", "secret")
    val cims = new org.apache.spark.sql.util.CaseInsensitiveStringMap(opts)
    val right = DataSourceV2Relation.create(table, None, None, cims)
    val leftVec = left.output.find(_.name == "lvec").get
    val rightVec = right.output.find(_.name == "rvec").get
    val join = NearestByJoin(
      left,
      right,
      Inner,
      approx = true,
      numResults = 5,
      rankingExpression = VectorL2Distance(leftVec, rightVec),
      direction = NearestByDistance)
    val conf = IndexedNearestByJoinRule(join) match {
      case Project(_, node: LanceKnnJoinLogicalPlan) => node.stageConf
      case other => fail(s"expected rewrite, got: $other"); ???
    }
    assertEquals(uri, conf.readOptions.getDatasetUri, "base read options should carry the URI")
    assertEquals(
      "frozen",
      conf.relationOptions.get("branch"),
      "relation branch must be captured for merge + pin")
    assertEquals(
      "secret",
      conf.relationOptions.get("storage.account_key"),
      "relation storage credential must be captured")
  }

  // -- helpers ------------------------------------------------------------------------------

  /**
   * Construct a left-side regular plan and a right-side Lance DSv2 scan (a `FakeLanceTable`, which
   * IS a `LanceDataset`). Avoids the need for a real Lance reader.
   */
  private def buildPlans(metricFunction: String)
      : (LogicalPlan, Attribute, LogicalPlan, Attribute) = {
    val left = trivialPlan("lid", "lvec")
    val rightLance = lanceLikeDsv2Relation()
    val leftVec = left.output.find(_.name == "lvec").get
    val rightVec = rightLance.output.find(_.name == "rvec").get
    (left, leftVec, rightLance, rightVec)
  }

  private def trivialPlan(idCol: String, vecCol: String): LogicalPlan = {
    val schema = new StructType(Array(
      StructField(idCol, IntegerType, nullable = false),
      StructField(vecCol, ArrayType(FloatType, containsNull = false), nullable = false)))
    val rows = (0 until 4).map(i => RowFactory.create(Integer.valueOf(i), Array.fill(8)(0.0f)))
    spark.createDataFrame(rows.asJava, schema).queryExecution.analyzed
  }

  /**
   * Build a `DataSourceV2Relation` backed by a `FakeLanceTable` (a real connector `LanceDataset`
   * subclass) so the rule's `instanceof LanceDataset` check accepts it. We don't run any I/O — the
   * `LanceDataset` constructor only stores its options + schema. Includes a `category` (string) and
   * `bucket` (int) column so prefilter-pushdown tests can build realistic filter predicates without
   * needing to extend the schema separately.
   */
  private def lanceLikeDsv2Relation(): LogicalPlan = {
    val schema = new StructType(Array(
      StructField("rid", IntegerType, nullable = false),
      StructField("category", StringType, nullable = true),
      StructField("bucket", IntegerType, nullable = true),
      StructField("rvec", ArrayType(FloatType, containsNull = false), nullable = false)))
    val uri = tempDir.resolve("fake_lance").toString
    val table = new FakeLanceTable(schema, uri)
    val opts = new java.util.HashMap[String, String]()
    opts.put("path", uri)
    val cims = new org.apache.spark.sql.util.CaseInsensitiveStringMap(opts)
    DataSourceV2Relation.create(table, None, None, cims)
  }

  /**
   * Extract an assertion-friendly summary of the rule's rewrite output. The rule produces
   * `Project(j.output, LanceKnnJoinLogicalPlan(left, stageConf, ...))`; this helper pulls out the
   * fields the test cases want to check straight off `stageConf`.
   */
  private case class RewriteSummary(
      metric: Metric,
      k: Int,
      rightVecCol: String,
      leftVecAttr: Attribute,
      prefilter: Option[String])

  private def expectRewritten(plan: LogicalPlan): RewriteSummary = plan match {
    case Project(_, node: LanceKnnJoinLogicalPlan) =>
      val conf = node.stageConf
      RewriteSummary(
        metric = conf.metric,
        k = conf.k,
        rightVecCol = conf.vectorColumn,
        leftVecAttr = node.child.output(conf.leftVecIdx),
        prefilter = conf.prefilter)
    case other =>
      fail(s"expected Project(LanceKnnJoinLogicalPlan(...)), got: $other"); ???
  }

  private def makeAttr(name: String, dt: DataType): Attribute =
    org.apache.spark.sql.catalyst.expressions.AttributeReference(name, dt, nullable = true)()

  private def lit(v: Int): Literal = Literal(v, IntegerType)
  private def lit(s: String): Literal = Literal(UTF8String.fromString(s), StringType)
}

/**
 * Stub table that IS a connector `LanceDataset` (the rule requires `instanceof LanceDataset`). The
 * `LanceDataset` constructor does no I/O — it only stores its read options + schema — so building
 * one over a fake URI is safe and keeps these tests backend-free. `readOptions()` returns options
 * carrying the fake URI; `getInitialStorageOptions()`/namespace getters return the empty / null
 * values the constructor stores, which is exactly the read context the rule captures. Lives in the
 * test source tree.
 */
class FakeLanceTable(_schema: StructType, uri: String)
  extends org.lance.spark.LanceDataset(
    org.lance.spark.LanceSparkReadOptions.from(uri),
    _schema,
    java.util.Collections.emptyMap[String, String](),
    null,
    null,
    false,
    null)
