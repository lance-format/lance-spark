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

import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.lance.spark.knn.internal.LanceVectorIndexBuilder
import org.lance.spark.knn.testutil.ClusteredEmbeddings

import java.nio.file.Path
import java.util.Random

import scala.collection.JavaConverters._

/**
 * SQL end-to-end tests for the Catalyst integration. Every test drives the full path:
 *
 *   ANTLR parser  ─▶  Analyzer  ─▶  IndexedNearestByJoinRule (our postHoc)  ─▶
 *      Optimizer  ─▶  LanceKnnJoinStrategy  ─▶  LanceKnnJoinExec  ─▶
 *      Lance native per-row probe + late materialize  ─▶  Rows
 *
 * Requires Spark 4.2 (the release where `NearestByJoin` exists, added by SPARK-56395) AND the
 * `lance-spark-4.2_2.13` connector built against the same Spark version.
 *
 * Two groups of tests, sharing the same SparkSession + Lance scaffolding:
 *
 *   1. EXACT path (no vector index) — Lance does an exact per-fragment scan, so results must
 *      equal the brute-force oracle exactly. Covers oracle equivalence, right-side `WHERE`
 *      prefilter pushdown, and the rule's opt-in gating (disabled → falls through to Spark's
 *      `RewriteNearestByJoin`, still correct).
 *   2. APPROXIMATE path (IVF-PQ index) — Lance returns approximate top-K, so recall is < 1.0.
 *      Covers that the indexed path engages, recall stays in a sane range at default settings,
 *      and `refineFactor > 1` improves (or matches) recall.
 *
 * The rule's plan-side pattern-matching (metric/direction, alias/filter unwrapping, prefilter
 * translation) is unit-tested separately in `IndexedNearestByJoinRuleTest`.
 */
class IndexedNearestByJoinSqlTest {

  @TempDir var tempDir: Path = _
  private var spark: SparkSession = _
  // Monotonic suffix so each SQL invocation gets fresh temp-view names (a single test builds
  // more than one left/right pair).
  private var viewSeq: Int = 0

  // Exact-path scale — kept tiny for speed; no index, so size doesn't affect correctness.
  private val ExactDim = 16
  private val ExactRight = 64
  private val ExactLeft = 8
  // Approximate-path scale — IVF-PQ needs more rows to be meaningful (1024 rows / 4 partitions
  // ≈ 256 per cluster). Still small enough to run in a few seconds.
  private val Dim = 32
  private val NumRight = 1024
  private val NumLeft = 32
  private val K = 10
  private val Seed = 0xCAFEL

  @BeforeEach def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("indexed-nearest-by-join-sql")
      .master("local[2]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "org.lance.spark.knn.extensions.LanceKnnSparkSessionExtensions")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    // Enabled by default; the "rule disabled" test flips it off explicitly.
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "true")
  }

  @AfterEach def teardown(): Unit = if (spark != null) spark.stop()

  // -- exact path (no index): oracle equivalence, WHERE pushdown, rule-off fallthrough -------

  /**
   * Full SQL path with the rule enabled. The physical plan must contain the `LanceKnnJoin` exec
   * AND the result must match the brute-force oracle on every left row (exact, no index).
   */
  @Test def testSqlApproxNearestRoutesThroughIndexedPathAndMatchesOracle(): Unit = {
    val leftVecs = generateUniform(ExactLeft, ExactDim, Seed)
    val (leftDf, leftIds, _) = buildLeftDf(leftVecs, ExactDim)
    val rightVecs = generateUniform(ExactRight, ExactDim, Seed + 1)
    val (rightUri, rightIds, _) = writeRightDf(rightVecs, ExactDim, idBase = 1000)

    val k = 5
    val (q, d) = registerViews(leftDf, rightUri)
    val df = spark.sql(
      s"""SELECT q.lid, d.rid
         |FROM $q q INNER JOIN $d d
         |APPROX NEAREST $k BY DISTANCE vector_l2_distance(q.lvec, d.rvec)""".stripMargin)

    // Plan-shape: confirm the rule fired (logical node present, AQE-independent) AND the strategy
    // lowered it to the `LanceKnnJoin` physical exec.
    val joinLogicals = df.queryExecution.optimizedPlan.collect {
      case p: LanceKnnJoinLogicalPlan => p
    }
    assertTrue(
      joinLogicals.nonEmpty,
      s"expected LanceKnnJoinLogicalPlan in optimized plan; got:\n${df.queryExecution.optimizedPlan}")
    val tree = df.queryExecution.executedPlan.treeString
    assertTrue(tree.contains("LanceKnnJoin"), s"expected LanceKnnJoin exec in tree:\n$tree")

    // Correctness: oracle equivalence.
    val rows = df.collect()
    assertEquals(ExactLeft * k, rows.length, "expected k results per left row")
    val byLid = rows.groupBy(_.getAs[Int]("lid"))
    leftIds.zip(leftVecs).foreach { case (lid, lvec) =>
      val oracle = oracleTopKIds(lvec, rightVecs.indices, rightIds, rightVecs, k)
      val actual = byLid(lid).map(_.getAs[Int]("rid")).toSet
      assertEquals(oracle, actual, s"top-K mismatch for lid=$lid (rule on, brute-force oracle)")
    }
  }

  /**
   * Right-side `WHERE` clause must round-trip through the prefilter pushdown — Lance computes
   * top-K only over rows matching the filter, so the result must equal the brute-force oracle
   * computed AFTER applying the same filter. If the rule pushed the filter wrong (or dropped
   * it), this test would diverge from the oracle. Two right-side rows share each `category`, so
   * `WHERE category = 'A'` shrinks the candidate pool meaningfully without zeroing it out.
   */
  @Test def testSqlWherePushdownMatchesFilteredOracle(): Unit = {
    val leftVecs = generateUniform(ExactLeft, ExactDim, Seed + 200)
    val (leftDf, leftIds, _) = buildLeftDf(leftVecs, ExactDim)
    val (rightVecs, rightIds, rightCategories, rightUri) =
      writeRightWithCategories(ExactRight, ExactDim, Seed + 201)

    val k = 4
    val targetCat = "A"
    val (q, d) = registerViews(leftDf, rightUri)
    val df = spark.sql(
      s"""SELECT q.lid, d.rid
         |FROM $q q INNER JOIN (SELECT * FROM $d WHERE category = '$targetCat') d
         |APPROX NEAREST $k BY DISTANCE vector_l2_distance(q.lvec, d.rvec)""".stripMargin)

    val joinLogicals = df.queryExecution.optimizedPlan.collect {
      case p: LanceKnnJoinLogicalPlan => p
    }
    assertTrue(
      joinLogicals.nonEmpty,
      s"expected LanceKnnJoinLogicalPlan; optimized plan was:\n${df.queryExecution.optimizedPlan}")
    val prefilter = joinLogicals.head.stageConf.prefilter
    assertTrue(
      prefilter.exists(_.contains(s"'$targetCat'")),
      s"expected prefilter to carry category='$targetCat'; got: $prefilter")

    // Oracle: brute-force top-K computed AFTER applying the same filter on the right side.
    val filteredIdxs = rightCategories.indices.filter(rightCategories(_) == targetCat)
    val rows = df.collect()
    val byLid = rows.groupBy(_.getAs[Int]("lid"))
    leftIds.zip(leftVecs).foreach { case (lid, lvec) =>
      val oracle = oracleTopKIds(lvec, filteredIdxs, rightIds, rightVecs, k)
      assertTrue(
        oracle.nonEmpty,
        s"oracle is empty for lid=$lid — test setup didn't produce filterable rows")
      val actual = byLid(lid).map(_.getAs[Int]("rid")).toSet
      assertEquals(
        oracle,
        actual,
        s"top-K mismatch under WHERE pushdown for lid=$lid (filtered brute-force oracle)")
    }
  }

  /**
   * With the gating config disabled, the SAME SQL falls through to Spark's
   * `RewriteNearestByJoin` (cross-product + `MaxMinByK`). The plan contains NO
   * `LanceKnnJoinLogicalPlan` and (importantly) results still match the oracle — proving the
   * rule's opt-in behavior: turning it off doesn't break correctness.
   */
  @Test def testSqlFallsThroughToBruteForceWhenRuleDisabled(): Unit = {
    spark.conf.set(IndexedNearestByJoinRule.EnabledConfKey, "false")

    val leftVecs = generateUniform(ExactLeft, ExactDim, Seed + 100)
    val (leftDf, leftIds, _) = buildLeftDf(leftVecs, ExactDim)
    val rightVecs = generateUniform(ExactRight, ExactDim, Seed + 101)
    val (rightUri, rightIds, _) = writeRightDf(rightVecs, ExactDim, idBase = 1000)

    val k = 4
    val (q, d) = registerViews(leftDf, rightUri)
    val df = spark.sql(
      s"""SELECT q.lid, d.rid
         |FROM $q q INNER JOIN $d d
         |APPROX NEAREST $k BY DISTANCE vector_l2_distance(q.lvec, d.rvec)""".stripMargin)

    val joinLogicals = df.queryExecution.optimizedPlan.collect {
      case p: LanceKnnJoinLogicalPlan => p
    }
    assertTrue(
      joinLogicals.isEmpty,
      s"rule disabled — expected NO LanceKnnJoinLogicalPlan; got:\n${df.queryExecution.optimizedPlan}")

    val rows = df.collect()
    assertEquals(ExactLeft * k, rows.length)
    val byLid = rows.groupBy(_.getAs[Int]("lid"))
    leftIds.zip(leftVecs).foreach { case (lid, lvec) =>
      val oracle = oracleTopKIds(lvec, rightVecs.indices, rightIds, rightVecs, k)
      val actual = byLid(lid).map(_.getAs[Int]("rid")).toSet
      assertEquals(oracle, actual, s"top-K mismatch for lid=$lid (rule off, brute-force fallback)")
    }
  }

  /**
   * Read-context end-to-end: a `version` supplied through the Lance read option
   * (`spark.read.format("lance").option("version", "1")`) must be captured from the DataSourceV2
   * relation options, merged, and pinned on the driver — so every probe scans that snapshot, not
   * HEAD. `LanceDataSource` is a `SupportsCatalogOptions` whose identifier is the URI alone, so this
   * option lives ONLY on the relation options; a rule that captured just the base read options would
   * silently read HEAD.
   *
   * We write v1, then append v2 rows that are EXACT duplicates of each left query vector (distance
   * 0 — they would be the #1 nearest neighbor if visible), then assert:
   *   - pinned to v1: no appended row (`rid >= DupIdBase`) may appear, and every left row still gets
   *     `k` v1 hits;
   *   - latest (no version option): each left row's own duplicate (`rid == DupIdBase + lid`) DOES
   *     appear — proving the pin is what excluded them, so the pinned assertion is discriminating,
   *     not vacuously satisfied.
   */
  @Test def testSqlVersionReadOptionPinsSnapshot(): Unit = {
    val DupIdBase = 5000
    val leftVecs = generateUniform(ExactLeft, ExactDim, Seed + 300)
    val (leftDf, leftIds, _) = buildLeftDf(leftVecs, ExactDim)

    // v1: baseline right rows (ids 1000..1063, all < DupIdBase).
    val v1Vecs = generateUniform(ExactRight, ExactDim, Seed + 301)
    val (rightUri, _, _) = writeRightDf(v1Vecs, ExactDim, idBase = 1000)

    // v2: append one exact duplicate of each left query vector (id DupIdBase + lid).
    val dupRows = leftIds.zip(leftVecs).map { case (lid, v) =>
      RowFactory.create(Integer.valueOf(DupIdBase + lid), v)
    }
    spark
      .createDataFrame(dupRows.toSeq.asJava, rightSchema(ExactDim))
      .write
      .format("lance")
      .mode("append")
      .save(rightUri)

    val k = 5

    // Pinned to version 1: the appended duplicates must be invisible.
    val pinnedRows = runKnnSqlVersioned(leftDf, rightUri, k, version = Some(1L))
    assertEquals(
      ExactLeft * k,
      pinnedRows.length,
      "expected k v1 hits per left row when pinned to v1")
    val pinnedByLid = pinnedRows.groupBy(_.getAs[Int]("lid"))
    leftIds.foreach { lid =>
      val rids = pinnedByLid(lid).map(_.getAs[Int]("rid"))
      assertTrue(
        rids.forall(_ < DupIdBase),
        s"pinned-to-v1 result for lid=$lid leaked an appended (v2) row: ${rids.mkString(",")}")
    }

    // Latest (no version option): each left row's own duplicate is a nearest hit (distance 0).
    val latestRows = runKnnSqlVersioned(leftDf, rightUri, k, version = None)
    val latestByLid = latestRows.groupBy(_.getAs[Int]("lid"))
    leftIds.foreach { lid =>
      val rids = latestByLid(lid).map(_.getAs[Int]("rid")).toSet
      assertTrue(
        rids.contains(DupIdBase + lid),
        s"latest result for lid=$lid should contain its duplicate ${DupIdBase + lid}; got $rids")
    }
  }

  // -- approximate path (IVF-PQ): recall floors, refineFactor -------------------------------

  /**
   * Build IVF-PQ, run the SQL `APPROX NEAREST`, measure recall@10 against the brute-force oracle.
   * With 1024 rows × 4 IVF partitions, each partition holds ~256 rows; a default-`nprobes` query
   * hits ~1 partition, so recall should be substantially > 0 but below 1.0. A threshold this
   * loose (0.3) would only fail on a real bug (index path not engaging), not IVF's inherent
   * approximation.
   */
  @Test def testIvfPqRecallReasonableAtDefaults(): Unit = {
    val leftVecs = generateUniform(NumLeft, Dim, Seed)
    val (leftDf, leftIds, _) = buildLeftDf(leftVecs, Dim)
    val rightVecs = generateUniform(NumRight, Dim, Seed + 1)
    val (rightUri, rightIds, _) = writeRightDf(rightVecs, Dim, idBase = 100000)
    LanceVectorIndexBuilder.buildIvfPq(
      datasetUri = rightUri,
      vectorColumn = "rvec",
      numPartitions = 4,
      numSubVectors = 8,
      numBits = 8)
    assertEquals(
      1,
      LanceVectorIndexBuilder.listIndexCount(rightUri),
      "expected exactly one index after build")

    val rows = runKnnSql(leftDf, rightUri, K, refineFactor = None)
    val recall = computeRecallAtK(rows, leftIds, leftVecs, rightIds, rightVecs, K)
    println(s"  IVF-PQ recall@$K (no refine, default nprobes): $recall")
    assertTrue(recall > 0.3, s"recall@$K=$recall too low; index path probably not engaging")
  }

  /**
   * Production-realistic distribution: clustered Gaussian mixture, unit-sphere-normalized — the
   * geometry of typical sentence-transformer / image-feature embeddings (uniform-random vectors
   * are IVF's WORST case; k-means has no cluster structure to latch onto). Asserts clustered
   * recall@K >= 0.5 at default IVF-PQ settings; prints both uniform and clustered so a reviewer
   * can see the realistic case helps.
   *
   * We don't `assert(clustered >= uniform)`: Lance's k-means init is non-deterministic across JVM
   * sessions, so on a tiny 1024-row dataset run-to-run noise routinely exceeds the structural
   * advantage. A reliable comparison would need many seeds or much larger N — we chose to print
   * both and assert only the realistic-floor invariant.
   */
  @Test def testClusteredEmbeddingsRecallSurvives(): Unit = {
    val (uniformDf, uniformIds, uniformVecs) =
      buildLeftDf(generateUniform(NumLeft, Dim, Seed), Dim)
    val (uniformUri, uniformRightIds, uniformRightVecs) =
      writeRightDf(generateUniform(NumRight, Dim, Seed + 1), Dim, idBase = 100000)
    LanceVectorIndexBuilder.buildIvfPq(uniformUri, "rvec", numPartitions = 4, numSubVectors = 8)

    val (clusteredDf, clusteredIds, clusteredVecs) = buildLeftDf(
      ClusteredEmbeddings.generate(NumLeft, Dim, numClusters = 4, seed = Seed + 2),
      Dim)
    val (clusteredUri, clusteredRightIds, clusteredRightVecs) = writeRightDf(
      ClusteredEmbeddings.generate(NumRight, Dim, numClusters = 16, seed = Seed + 3),
      Dim,
      idBase = 100000)
    LanceVectorIndexBuilder.buildIvfPq(clusteredUri, "rvec", numPartitions = 4, numSubVectors = 8)

    val uniformRecall = recallAgainst(
      uniformDf,
      uniformUri,
      uniformIds,
      uniformVecs,
      uniformRightIds,
      uniformRightVecs)
    val clusteredRecall = recallAgainst(
      clusteredDf,
      clusteredUri,
      clusteredIds,
      clusteredVecs,
      clusteredRightIds,
      clusteredRightVecs)
    println(
      s"  IVF-PQ recall@$K: uniform=$uniformRecall, clustered=$clusteredRecall " +
        "(uniform = IVF worst case; clustered = production-shaped)")

    assertTrue(
      clusteredRecall >= 0.5,
      s"clustered-data recall@$K=$clusteredRecall is unexpectedly low; " +
        "defaults should comfortably exceed 0.5 on production-shaped embeddings — " +
        "if this fails, suspect a regression in Lance's index path or in our probe wiring")
  }

  /**
   * `refineFactor > 1` engages Lance's exact-distance re-rank: fetch `K * refineFactor`
   * approximate candidates, re-rank, trim back to K. Strictly improves (or matches) recall vs. no
   * refine. We assert `>=` rather than a strict `>` so the test isn't flaky on tiny datasets where
   * both paths find the same K rows. The knob is set through `spark.lance.knn.refineFactor`.
   */
  @Test def testRefineFactorImprovesRecall(): Unit = {
    val leftVecs = generateUniform(NumLeft, Dim, Seed)
    val (leftDf, leftIds, _) = buildLeftDf(leftVecs, Dim)
    val rightVecs = generateUniform(NumRight, Dim, Seed + 1)
    val (rightUri, rightIds, _) = writeRightDf(rightVecs, Dim, idBase = 100000)
    LanceVectorIndexBuilder.buildIvfPq(rightUri, "rvec", numPartitions = 4)

    val baselineRows = runKnnSql(leftDf, rightUri, K, refineFactor = None)
    val refinedRows = runKnnSql(leftDf, rightUri, K, refineFactor = Some(8))

    val recallBaseline = computeRecallAtK(baselineRows, leftIds, leftVecs, rightIds, rightVecs, K)
    val recallRefined = computeRecallAtK(refinedRows, leftIds, leftVecs, rightIds, rightVecs, K)
    println(s"  IVF-PQ recall@$K: no refine = $recallBaseline, refineFactor=8 = $recallRefined")
    assertTrue(
      recallRefined >= recallBaseline,
      s"refineFactor should not hurt recall: baseline=$recallBaseline, refined=$recallRefined")
  }

  // -- helpers ------------------------------------------------------------------------------

  /** Register the left DataFrame and the right Lance dataset as fresh temp views. */
  private def registerViews(leftDf: DataFrame, rightUri: String): (String, String) = {
    viewSeq += 1
    val q = s"queries_$viewSeq"
    val d = s"docs_$viewSeq"
    leftDf.createOrReplaceTempView(q)
    spark.read.format("lance").load(rightUri).createOrReplaceTempView(d)
    (q, d)
  }

  /**
   * Run a simple `INNER JOIN ... APPROX NEAREST k` (no WHERE) through the indexed rule and collect.
   * `refineFactor` is threaded via the `spark.lance.knn.refineFactor` config the rule reads
   * (unset => Lance default = no re-rank).
   */
  private def runKnnSql(
      leftDf: DataFrame,
      rightUri: String,
      k: Int,
      refineFactor: Option[Int]): Array[Row] = {
    val (q, d) = registerViews(leftDf, rightUri)
    refineFactor match {
      case Some(rf) => spark.conf.set(IndexedNearestByJoinRule.RefineFactorConfKey, rf.toString)
      case None => spark.conf.unset(IndexedNearestByJoinRule.RefineFactorConfKey)
    }
    spark.sql(
      s"""SELECT q.lid, d.rid
         |FROM $q q INNER JOIN $d d
         |APPROX NEAREST $k BY DISTANCE vector_l2_distance(q.lvec, d.rvec)""".stripMargin).collect()
  }

  /**
   * Like [[runKnnSql]] but registers the right Lance view through an optional `version` READ OPTION,
   * exercising the DataSourceV2-relation-options path that carries branch / version / storage
   * credentials. Also asserts the indexed rule still fires when a version option is present.
   */
  private def runKnnSqlVersioned(
      leftDf: DataFrame,
      rightUri: String,
      k: Int,
      version: Option[Long]): Array[Row] = {
    viewSeq += 1
    val q = s"queries_$viewSeq"
    val d = s"docs_$viewSeq"
    leftDf.createOrReplaceTempView(q)
    val reader = spark.read.format("lance")
    version.foreach(v => reader.option("version", v.toString))
    reader.load(rightUri).createOrReplaceTempView(d)
    val df = spark.sql(
      s"""SELECT q.lid, d.rid
         |FROM $q q INNER JOIN $d d
         |APPROX NEAREST $k BY DISTANCE vector_l2_distance(q.lvec, d.rvec)""".stripMargin)
    assertTrue(
      df.queryExecution.optimizedPlan.collect { case p: LanceKnnJoinLogicalPlan => p }.nonEmpty,
      s"expected indexed rewrite even with a version option; plan:\n${df.queryExecution.optimizedPlan}")
    df.collect()
  }

  private def leftSchema(dim: Int): StructType = new StructType(Array(
    StructField("lid", IntegerType, nullable = false),
    fixedSizeVec("lvec", dim)))

  private def rightSchema(dim: Int): StructType = new StructType(Array(
    StructField("rid", IntegerType, nullable = false),
    fixedSizeVec("rvec", dim)))

  private def rightSchemaWithCategories(dim: Int): StructType = new StructType(Array(
    StructField("rid", IntegerType, nullable = false),
    StructField("category", StringType, nullable = false),
    fixedSizeVec("rvec", dim)))

  private def fixedSizeVec(name: String, dim: Int): StructField =
    StructField(
      name,
      ArrayType(FloatType, containsNull = false),
      nullable = false,
      new MetadataBuilder().putLong("arrow.fixed-size-list.size", dim.toLong).build())

  /** Build a left query DataFrame (lid, lvec) from pre-generated vectors. */
  private def buildLeftDf(
      vectors: Array[Array[Float]],
      dim: Int): (DataFrame, Array[Int], Array[Array[Float]]) = {
    val ids = vectors.indices.toArray
    val rows = ids.zip(vectors).map { case (id, v) => RowFactory.create(Integer.valueOf(id), v) }
    (spark.createDataFrame(rows.toSeq.asJava, leftSchema(dim)), ids, vectors)
  }

  /** Write a right Lance dataset (rid, rvec) from pre-generated vectors. */
  private def writeRightDf(
      vectors: Array[Array[Float]],
      dim: Int,
      idBase: Int): (String, Array[Int], Array[Array[Float]]) = {
    val ids = vectors.indices.map(_ + idBase).toArray
    val rows = ids.zip(vectors).map { case (id, v) => RowFactory.create(Integer.valueOf(id), v) }
    val df = spark.createDataFrame(rows.toSeq.asJava, rightSchema(dim))
    val out = tempDir.resolve(s"right_${System.nanoTime()}").toString
    df.write.format("lance").save(out)
    (out, ids, vectors)
  }

  /**
   * Write a right Lance dataset whose rows also carry a `category` from a small alphabet, so the
   * WHERE-pushdown test has a non-trivial filter to apply. Returns the vectors, ids, per-row
   * categories, and the dataset URI.
   */
  private def writeRightWithCategories(
      n: Int,
      dim: Int,
      seed: Long): (Array[Array[Float]], Array[Int], Array[String], String) = {
    val vectors = generateUniform(n, dim, seed)
    val ids = vectors.indices.map(_ + 2000).toArray
    val alphabet = Array("A", "B", "C", "D")
    val categories = vectors.indices.map(i => alphabet(i % alphabet.length)).toArray
    val rows = ids.zip(vectors).zip(categories).map { case ((id, v), cat) =>
      RowFactory.create(Integer.valueOf(id), cat, v)
    }
    val df = spark.createDataFrame(rows.toSeq.asJava, rightSchemaWithCategories(dim))
    val out = tempDir.resolve(s"right_cat_${System.nanoTime()}").toString
    df.write.format("lance").save(out)
    (vectors, ids, categories, out)
  }

  /** Run an indexed nearest join (SQL path) against the given right dataset and compute recall@K. */
  private def recallAgainst(
      leftDf: DataFrame,
      rightUri: String,
      leftIds: Array[Int],
      leftVecs: Array[Array[Float]],
      rightIds: Array[Int],
      rightVecs: Array[Array[Float]]): Double = {
    val rows = runKnnSql(leftDf, rightUri, K, refineFactor = None)
    computeRecallAtK(rows, leftIds, leftVecs, rightIds, rightVecs, K)
  }

  /** Uniform-random vectors over the unit hypercube — the IVF-worst-case data distribution. */
  private def generateUniform(n: Int, dim: Int, seed: Long): Array[Array[Float]] = {
    val rng = new Random(seed)
    Array.fill(n)(randomVector(rng, dim))
  }

  /**
   * Brute-force top-K right ids for a query vector, over the given candidate right indices (pass
   * `rightVecs.indices` for the whole dataset, or a filtered subset for WHERE-pushdown oracles).
   */
  private def oracleTopKIds(
      lvec: Array[Float],
      candidateIdxs: Seq[Int],
      rightIds: Array[Int],
      rightVecs: Array[Array[Float]],
      k: Int): Set[Int] =
    candidateIdxs
      .map(i => (rightIds(i), l2(lvec, rightVecs(i))))
      .sortBy(_._2)
      .take(k)
      .map(_._1)
      .toSet

  /**
   * Mean recall@K across all left rows: |indexed top-K ∩ brute-force top-K| / K. 1.0 means the
   * indexed path returned the same K rows as brute force; lower means the IVF cluster cut excluded
   * some true neighbors.
   */
  private def computeRecallAtK(
      joinedRows: Array[Row],
      leftIds: Array[Int],
      leftVecs: Array[Array[Float]],
      rightIds: Array[Int],
      rightVecs: Array[Array[Float]],
      k: Int): Double = {
    val byLid = joinedRows.groupBy(_.getAs[Int]("lid"))
    val perLidRecall = leftIds.zip(leftVecs).map { case (lid, lvec) =>
      val oracle = oracleTopKIds(lvec, rightVecs.indices, rightIds, rightVecs, k)
      val actual = byLid.getOrElse(lid, Array.empty).map(_.getAs[Int]("rid")).toSet
      (oracle intersect actual).size.toDouble / k
    }
    perLidRecall.sum / perLidRecall.length
  }

  private def randomVector(rng: Random, dim: Int): Array[Float] = {
    val v = new Array[Float](dim)
    var i = 0
    while (i < dim) { v(i) = rng.nextFloat(); i += 1 }
    v
  }

  private def l2(a: Array[Float], b: Array[Float]): Float = {
    var s = 0.0f
    var i = 0
    while (i < a.length) { val d = a(i) - b(i); s += d * d; i += 1 }
    s
  }
}
