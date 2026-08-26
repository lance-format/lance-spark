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
