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
package org.lance.spark.knn.internal

import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.lance.spark.LanceSparkReadOptions

import java.nio.file.Path
import java.util.{Collections, Random}

import scala.collection.JavaConverters._

/**
 * End-to-end validation of [[LanceProbe]] against a real Lance dataset written by Spark. These are
 * the day-1 validation tasks the implementation plan calls out:
 *
 *  - Per-probe call should succeed and return Lance's nearest neighbors.
 *  - Repeated probes against the same `LanceProbe` instance should reuse the open dataset
 *    handle; the second call should not re-pay the dataset open cost.
 *  - `fragmentIds` restriction should narrow the search to specified fragments only.
 *  - Without an explicit vector index the probe falls back to a brute-force per-fragment scan,
 *    which gives recall = 1.0 — making the no-index path the natural correctness oracle.
 *
 * These tests do NOT require an actual vector index; that is exercised in the indexed test
 * suites which build IVF-PQ via Lance's index DDL. Validating the brute-force path first lets us
 * isolate any LanceProbe bugs from index-quality issues.
 */
class LanceProbeValidationTest {

  @TempDir var tempDir: Path = _
  private var spark: SparkSession = _

  // Small synthetic dataset: 64 vectors, dim 8. Enough to exercise the probe loop without making
  // the test slow.
  private val NumRows = 64
  private val VectorDim = 8
  private val Seed = 42L

  @BeforeEach def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("lance-probe-validation")
      .master("local[2]")
      // Pin the driver to loopback so test JVMs in restricted networks (CI sandboxes, dev
      // containers) can bind without scanning the host's interfaces.
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  }

  @AfterEach def teardown(): Unit = {
    if (spark != null) spark.stop()
  }

  /**
   * Smoke test: write a dataset, probe it, get K rows back. No correctness assertion beyond
   * "result has the right shape" — the brute-force-equivalence test below covers semantics.
   */
  @Test def testProbeReturnsKResults(): Unit = {
    val datasetUri = writeSyntheticDataset()
    val query = randomVector(new Random(7L), VectorDim)

    val probe = new LanceProbe(datasetUri, fragmentIds = None)
    try {
      val results = probe.probe(vectorColumn = "vec", query, k = 5, metric = Metric.L2)
      assertEquals(5, results.size, "probe should return exactly k results")
      // Distances must be monotonically non-decreasing for L2 (best-first).
      val scores = results.map(_.score)
      assertEquals(scores, scores.sorted, "L2 results should be sorted ascending by distance")
      // Row addresses are stable u64s; we just sanity-check they aren't all zero.
      assertTrue(results.exists(_.rowAddr != 0L), "row addresses should be populated")
    } finally probe.close()
  }

  /**
   * Without a vector index, Lance does an exact per-fragment scan. That makes it a recall = 1.0
   * oracle: the probe result should equal the ground-truth top-K computed in plain Scala.
   */
  @Test def testProbeMatchesBruteForceOracle(): Unit = {
    val rng = new Random(Seed)
    val (rows, vectors) = generateRows(rng, NumRows, VectorDim)
    val datasetUri = writeRows(rows)

    val query = randomVector(new Random(123L), VectorDim)
    val k = 10

    val oracle: Seq[(Int, Float)] = vectors.zipWithIndex
      .map { case (v, idx) => (idx, l2Distance(query, v)) }
      .sortBy(_._2)
      .take(k)

    val probe = new LanceProbe(datasetUri, fragmentIds = None)
    val actual =
      try probe.probe("vec", query, k, Metric.L2)
      finally probe.close()

    assertEquals(k, actual.size)
    // Compare scores within float tolerance.
    val expectedScores = oracle.map(_._2)
    val actualScores = actual.map(_.score)
    expectedScores.zip(actualScores).foreach { case (expected, actualScore) =>
      assertEquals(
        expected,
        actualScore,
        1e-4f,
        s"top-K distance mismatch: oracle=$expectedScores actual=$actualScores")
    }
  }

  /**
   * Cosine and Dot must rank best-first the same direction L2 does. Lance returns a DISTANCE for
   * every metric (`1 - cosine_similarity`, `1 - dot_product` for the similarity-flavored ones), so
   * smaller is better for all three — [[Metric.smallerIsBetter]] must be `true` for each. This
   * reproduces the gatekeeper's failure directly: run a real Lance query, then merge the results
   * through the size-1 [[TopKHeap]] the join stage uses, keyed by the metric's own direction flag.
   * The nearest ref (Lance returns best-first, so `refs.head`) must survive; a wrong flag (treating a
   * Lance distance as larger-is-better) would retain the FARTHEST ref instead.
   */
  @Test def testMetricFlagsKeepNearestThroughHeap(): Unit = {
    val datasetUri = writeSyntheticDataset()
    val query = randomVector(new Random(555L), VectorDim)

    val probe = new LanceProbe(datasetUri, fragmentIds = None)
    try {
      Seq[Metric](Metric.L2, Metric.Cosine, Metric.Dot).foreach { metric =>
        val refs = probe.probe("vec", query, k = 5, metric)
        assertEquals(5, refs.size, s"$metric probe should return k results")
        // Lance returns best-first, so refs.head is the true nearest for this metric.
        val nearest = refs.head
        val heap = new TopKHeap(k = 1, metric.smallerIsBetter)
        heap.offerAll(refs)
        val survivor = heap.drain()
        assertEquals(1, survivor.length, s"$metric: size-1 heap should retain one ref")
        assertEquals(
          nearest.rowAddr,
          survivor.head.rowAddr,
          s"$metric: size-1 heap must keep the nearest ref (rowAddr=${nearest.rowAddr}), " +
            s"got ${survivor.head.rowAddr} — wrong smallerIsBetter direction?")
        assertEquals(nearest.score, survivor.head.score, 1e-6f, s"$metric: kept score mismatch")
      }
    } finally probe.close()
  }

  /**
   * A projected payload column WITHOUT a supplied Spark type must be preserved through the generic
   * Arrow conversion — the same fallback [[LanceProbe.materialize]] / `readRows` apply — not silently
   * dropped. Regression: `projection = Seq("id")` with empty `projectionFields` must return payload
   * keys `Set("id")` (the injected `_rowid` / score columns stay out of the payload).
   */
  @Test def testProbeRowsPreservesUnmappedProjectedFields(): Unit = {
    val datasetUri = writeSyntheticDataset()
    val query = randomVector(new Random(321L), VectorDim)

    val probe = new LanceProbe(datasetUri, fragmentIds = None)
    try {
      val hits = probe.probeRows(
        "vec",
        query,
        k = 5,
        Metric.L2,
        projection = Seq("id"),
        projectionFields = Seq.empty)
      assertEquals(5, hits.size, "probeRows should return k hits")
      hits.foreach { h =>
        assertEquals(
          Set("id"),
          h.row.keySet,
          s"unmapped projected field must be preserved (id only, no _rowid/_distance); " +
            s"got ${h.row.keySet}")
        assertNotNull(h.row("id"), "unmapped id payload must be populated")
      }
    } finally probe.close()
  }

  /**
   * The fused [[LanceProbe.probeRows]] scan injects `_rowid` and the score column itself, so a
   * payload projection naming one of those reserved columns would collide inside the single scan.
   * `probeRows` must reject such a projection (so the join stage can route it through the split
   * probe + materialize path) rather than surfacing an opaque Arrow "merge incompatible fields"
   * error, and [[LanceProbe.fusesCleanly]] must report it as not fusible.
   */
  @Test def testProbeRowsRejectsReservedProjection(): Unit = {
    val datasetUri = writeSyntheticDataset()
    val query = randomVector(new Random(1L), VectorDim)

    val probe = new LanceProbe(datasetUri, fragmentIds = None)
    try {
      LanceProbe.ReservedProjectionColumns.foreach { reserved =>
        assertFalse(
          LanceProbe.fusesCleanly(Seq("id", reserved)),
          s"projection naming reserved column '$reserved' must not be fusible")
        val ex = assertThrows(
          classOf[IllegalArgumentException],
          () =>
            probe.probeRows(
              "vec",
              query,
              k = 5,
              Metric.L2,
              projection = Seq("id", reserved),
              projectionFields = Seq.empty))
        assertTrue(
          String.valueOf(ex.getMessage).contains(reserved),
          s"guard message should name reserved column '$reserved'; got: ${ex.getMessage}")
      }
    } finally probe.close()
  }

  /**
   * The folded fast path ([[LanceProbe.probeRows]]) must be observationally identical to the split
   * probe + materialize path: the SAME (rowAddr, score) hits in the SAME order, and the SAME
   * materialized payload per row. This is the invariant the SQL join relies on when it single-scans
   * (internalK == k) instead of probing then late-materializing. Runs on the brute-force path so the
   * search itself is deterministic.
   */
  @Test def testProbeRowsMatchesProbeThenMaterialize(): Unit = {
    val datasetUri = writeSyntheticDataset()
    val query = randomVector(new Random(321L), VectorDim)
    val k = 8
    val projection = Seq("id", "vec")
    val projectionFields = Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("vec", ArrayType(FloatType, containsNull = false), nullable = false))

    val probe = new LanceProbe(datasetUri, fragmentIds = None)
    try {
      // Split path: search for refs, then late-materialize the payload by _rowid.
      val refs = probe.probe("vec", query, k, Metric.L2)
      val expectedPayload: Map[Long, Map[String, Any]] = probe
        .materialize(refs.map(_.rowAddr), projection, projectionFields)
        .map(m => rowAddrOf(m) -> m)
        .toMap

      // Folded path: search AND project the payload in one scan.
      val hits = probe.probeRows("vec", query, k, Metric.L2, projection, projectionFields)

      assertEquals(k, hits.size, "probeRows should return exactly k hits")
      // Same search: identical (rowAddr, score) sequence, in order.
      assertEquals(
        refs.map(r => (r.rowAddr, r.score)),
        hits.map(h => (h.rowAddr, h.score)),
        "probeRows hits must match probe refs (rowAddr + score), in order")
      // Same payload: each folded hit equals the split materialize's row for that rowAddr, on the
      // projected columns.
      hits.foreach { h =>
        val expected = expectedPayload(h.rowAddr)
        assertEquals(expected("id"), h.row("id"), s"id mismatch for rowAddr=${h.rowAddr}")
        assertEquals(
          expected("vec").asInstanceOf[Seq[_]].toList,
          h.row("vec").asInstanceOf[Seq[_]].toList,
          s"vec mismatch for rowAddr=${h.rowAddr}")
      }
    } finally probe.close()
  }

  /**
   * Validate the dataset handle is reused across calls. The exact perf invariant ("second call
   * faster than first by some factor") is too brittle for CI, so we only assert that repeated
   * probes succeed and don't OOM — i.e., no JNI handle / Arrow buffer leak per call.
   */
  @Test def testRepeatedProbesShareDatasetHandle(): Unit = {
    val datasetUri = writeSyntheticDataset()
    val probe = new LanceProbe(datasetUri, None)
    try {
      val rng = new Random(99L)
      val k = 4
      var i = 0
      while (i < 50) {
        val results = probe.probe("vec", randomVector(rng, VectorDim), k, Metric.L2)
        assertEquals(k, results.size, s"iteration $i returned wrong size")
        i += 1
      }
    } finally probe.close()
  }

  /** Empty fragment-id list ⇒ no rows match. Confirms the pushdown actually narrows search. */
  @Test def testEmptyFragmentRestrictionReturnsNothing(): Unit = {
    val datasetUri = writeSyntheticDataset()
    val probe = new LanceProbe(datasetUri, Some(Seq.empty))
    try {
      val results = probe.probe("vec", randomVector(new Random(1L), VectorDim), 5, Metric.L2)
      assertTrue(results.isEmpty, s"empty fragmentIds should yield no results, got ${results.size}")
    } finally probe.close()
  }

  /**
   * When `executor_credential_refresh = false`, the probe must NOT rebuild (or even load) the
   * runtime namespace on the worker — exactly the policy `LanceFragmentScanner.create` applies.
   * Regression against the earlier `openDataset` that called `builder.runtimeNamespace(impl, ...)`
   * unconditionally, which forced the namespace impl class to load regardless of the refresh flag.
   *
   * We pass a namespace impl that does not exist on the classpath and point the probe at a
   * non-existent dataset URI. The open must fail because the DATASET is missing — not because it
   * tried to load the bogus namespace class. Asserting the failure message does not mention the
   * namespace class proves the namespace path was skipped.
   */
  @Test def testExecutorCredentialRefreshFalseSkipsNamespaceRebuild(): Unit = {
    val missingUri = tempDir.resolve("does_not_exist").toString
    val readOptions = LanceSparkReadOptions
      .builder()
      .datasetUri(missingUri)
      .executorCredentialRefresh(false)
      .build()
    val ex = assertThrows(
      classOf[RuntimeException],
      () =>
        new LanceProbe(
          readOptions,
          null,
          "example.namespace.MustNotBeLoaded",
          Collections.emptyMap[String, String](),
          None))
    assertFalse(
      String.valueOf(ex.getMessage).contains("MustNotBeLoaded"),
      s"namespace impl must not be loaded when executor credential refresh is off; got: " +
        ex.getMessage)
  }

  // -- helpers ------------------------------------------------------------------------------

  /** Write a fresh dataset and return its file:// URI. */
  private def writeSyntheticDataset(): String = {
    val rng = new Random(Seed)
    val (rows, _) = generateRows(rng, NumRows, VectorDim)
    writeRows(rows)
  }

  private def writeRows(rows: Seq[Row]): String = {
    val schema = new StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField(
        "vec",
        ArrayType(FloatType, containsNull = false),
        nullable = false,
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", VectorDim.toLong).build())))
    val df = spark.createDataFrame(rows.asJava, schema)

    val outDir = tempDir.resolve(s"probe_test_${System.nanoTime()}").toString
    df.write.format("lance").save(outDir)
    outDir
  }

  private def generateRows(rng: Random, n: Int, dim: Int): (Seq[Row], Seq[Array[Float]]) = {
    val vectors = (0 until n).map(_ => randomVector(rng, dim))
    val rows = vectors.zipWithIndex.map { case (v, idx) =>
      RowFactory.create(Integer.valueOf(idx), v)
    }
    (rows, vectors)
  }

  /** Read the `_rowid` key out of a materialized row map (a boxed / stringy long). */
  private def rowAddrOf(m: Map[String, Any]): Long = m(LanceProbe.RowIdColumn) match {
    case l: java.lang.Long => l.longValue()
    case l: Long => l
    case other => other.toString.toLong
  }

  private def randomVector(rng: Random, dim: Int): Array[Float] = {
    val v = new Array[Float](dim)
    var i = 0
    while (i < dim) { v(i) = rng.nextFloat(); i += 1 }
    v
  }

  private def l2Distance(a: Array[Float], b: Array[Float]): Float = {
    var s = 0.0f
    var i = 0
    while (i < a.length) {
      val d = a(i) - b(i)
      s += d * d
      i += 1
    }
    s
  }
}
