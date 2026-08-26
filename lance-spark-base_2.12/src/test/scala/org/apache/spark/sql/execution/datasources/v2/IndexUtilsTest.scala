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

import org.apache.spark.sql.catalyst.plans.logical.LanceNamedArgument
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.index.IndexType

/**
 * Unit tests for [[IndexUtils]] helper methods.
 *
 * These tests are pure (no SparkSession, no Lance native library) and execute on the JVM only,
 * so they can run in any CI environment without native dependencies.
 */
class IndexUtilsTest {

  private def fragmentWorkloads(rows: Long*): List[FragmentWorkload] =
    rows.zipWithIndex.map { case (rowCount, fragmentId) =>
      FragmentWorkload(java.lang.Integer.valueOf(fragmentId), rowCount)
    }.toList

  /**
   * Asserts the batches partition the fragments into contiguous runs of the id order.
   *
   * Concatenating the batches in order and getting an ascending id sequence back is exactly that
   * property: any partition whose concatenation is sorted consists of consecutive slices.
   */
  private def assertContiguousBatches(batches: Seq[List[Integer]]): Unit = {
    val flattened = batches.flatten.map(_.intValue)
    assertEquals(
      flattened.sorted,
      flattened,
      s"batches must partition the fragments in id order, got $batches")
    batches.foreach(batch => assertFalse(batch.isEmpty, s"no batch may be empty, got $batches"))
  }

  // ── extractTrain ──────────────────────────────────────────────────────────

  @Test
  def extractTrain_defaultsToTrueWhenArgAbsent(): Unit = {
    assertTrue(IndexUtils.extractTrain(Seq.empty))
  }

  @Test
  def extractTrain_returnsTrueForExplicitTrue(): Unit = {
    val args = Seq(LanceNamedArgument("train", java.lang.Boolean.TRUE))
    assertTrue(IndexUtils.extractTrain(args))
  }

  @Test
  def extractTrain_returnsFalseForExplicitFalse(): Unit = {
    val args = Seq(LanceNamedArgument("train", java.lang.Boolean.FALSE))
    assertFalse(IndexUtils.extractTrain(args))
  }

  @Test
  def extractTrain_ignoresUnrelatedArgs(): Unit = {
    val args = Seq(
      LanceNamedArgument("base_tokenizer", "simple"),
      LanceNamedArgument("language", "English"))
    assertTrue(IndexUtils.extractTrain(args))
  }

  @Test
  def extractTrain_trainFalseAlongsideOtherArgs(): Unit = {
    val args = Seq(
      LanceNamedArgument("base_tokenizer", "simple"),
      LanceNamedArgument("train", java.lang.Boolean.FALSE))
    assertFalse(IndexUtils.extractTrain(args))
  }

  @Test
  def extractTrain_throwsOnNonBooleanValue(): Unit = {
    val args = Seq(LanceNamedArgument("train", "yes"))
    assertThrows(
      classOf[IllegalArgumentException],
      () => IndexUtils.extractTrain(args))
  }

  @Test
  def extractTrain_throwsOnIntegerValue(): Unit = {
    val args = Seq(LanceNamedArgument("train", java.lang.Integer.valueOf(1)))
    assertThrows(
      classOf[IllegalArgumentException],
      () => IndexUtils.extractTrain(args))
  }

  // ── toJson — SparkOnlyOptions filtering ───────────────────────────────────

  @Test
  def toJson_emptyArgsReturnsEmptyObject(): Unit = {
    assertEquals("{}", IndexUtils.toJson(Seq.empty))
  }

  @Test
  def toJson_filtersTrainFromOutput(): Unit = {
    val args = Seq(LanceNamedArgument("train", java.lang.Boolean.FALSE))
    assertEquals("{}", IndexUtils.toJson(args))
  }

  @Test
  def toJson_filtersBuildModeFromOutput(): Unit = {
    val args = Seq(LanceNamedArgument("build_mode", "range"))
    assertEquals("{}", IndexUtils.toJson(args))
  }

  @Test
  def toJson_filtersRowsPerRangeFromOutput(): Unit = {
    val args = Seq(LanceNamedArgument("rows_per_range", java.lang.Long.valueOf(500000L)))
    assertEquals("{}", IndexUtils.toJson(args))
  }

  @Test
  def toJson_filtersNumSegmentsFromOutput(): Unit = {
    val args = Seq(LanceNamedArgument("num_segments", java.lang.Integer.valueOf(4)))
    assertEquals("{}", IndexUtils.toJson(args))
  }

  @Test
  def toJson_filtersAllSparkOnlyOptionsLeavingIndexParams(): Unit = {
    val args = Seq(
      LanceNamedArgument("train", java.lang.Boolean.FALSE),
      LanceNamedArgument("build_mode", "range"),
      LanceNamedArgument("rows_per_range", java.lang.Long.valueOf(1000000L)),
      LanceNamedArgument("num_segments", java.lang.Integer.valueOf(8)),
      LanceNamedArgument("base_tokenizer", "simple"),
      LanceNamedArgument("language", "English"))
    val json = IndexUtils.toJson(args)
    assertFalse(json.contains("train"), "train must be stripped from JSON params")
    assertFalse(json.contains("build_mode"), "build_mode must be stripped from JSON params")
    assertFalse(json.contains("rows_per_range"), "rows_per_range must be stripped from JSON params")
    assertFalse(json.contains("num_segments"), "num_segments must be stripped from JSON params")
    assertTrue(json.contains("base_tokenizer"), "index param base_tokenizer must be present")
    assertTrue(json.contains("language"), "index param language must be present")
  }

  @Test
  def toJson_preservesStringParams(): Unit = {
    val args = Seq(LanceNamedArgument("base_tokenizer", "simple"))
    val json = IndexUtils.toJson(args)
    assertTrue(json.contains("\"base_tokenizer\""))
    assertTrue(json.contains("\"simple\""))
  }

  @Test
  def toJson_preservesBooleanParams(): Unit = {
    val args = Seq(LanceNamedArgument("with_position", java.lang.Boolean.TRUE))
    val json = IndexUtils.toJson(args)
    assertTrue(json.contains("\"with_position\""))
    assertTrue(json.contains("true"))
  }

  @Test
  def toJson_preservesLongParams(): Unit = {
    val args = Seq(LanceNamedArgument("zone_size", java.lang.Long.valueOf(64L)))
    val json = IndexUtils.toJson(args)
    assertTrue(json.contains("\"zone_size\""))
    assertTrue(json.contains("64"))
  }

  // ── buildIndexType ─────────────────────────────────────────────────────────

  @Test
  def buildIndexType_btreeCaseInsensitive(): Unit = {
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("btree"))
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("BTREE"))
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("BTree"))
  }

  @Test
  def buildIndexType_ftsAndInvertedReturnInverted(): Unit = {
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("fts"))
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("FTS"))
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("inverted"))
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("INVERTED"))
  }

  @Test
  def buildScalarIndexParamType_ftsAndInvertedReturnInverted(): Unit = {
    assertEquals("inverted", IndexUtils.buildScalarIndexParamType("fts"))
    assertEquals("inverted", IndexUtils.buildScalarIndexParamType("inverted"))
  }

  @Test
  def scalarSegmentIndexType_mapsAllSupportedMethods(): Unit = {
    val expected = Seq(
      ("zonemap", IndexType.ZONEMAP, "zonemap"),
      ("bitmap", IndexType.BITMAP, "bitmap"),
      ("label_list", IndexType.LABEL_LIST, "labellist"),
      ("ngram", IndexType.NGRAM, "ngram"),
      ("bloomfilter", IndexType.BLOOM_FILTER, "bloomfilter"),
      ("rtree", IndexType.RTREE, "rtree"),
      ("fts", IndexType.INVERTED, "inverted"),
      ("inverted", IndexType.INVERTED, "inverted"))

    expected.foreach { case (method, indexType, coreParamType) =>
      Seq(method, method.toUpperCase).foreach { spelling =>
        assertEquals(indexType, IndexUtils.scalarSegmentIndexType(spelling).get)
        assertEquals(indexType, IndexUtils.buildIndexType(spelling))
        assertEquals(coreParamType, IndexUtils.buildScalarIndexParamType(spelling))
      }
    }
  }

  @Test
  def scalarSegmentIndexType_rejectsAliases(): Unit = {
    Seq("labellist", "bloom_filter", "r_tree").foreach { alias =>
      assertTrue(IndexUtils.scalarSegmentIndexType(alias).isEmpty)
      assertThrows(
        classOf[UnsupportedOperationException],
        () => IndexUtils.buildIndexType(alias))
    }
  }

  @Test
  def buildIndexType_throwsOnUnknown(): Unit = {
    assertThrows(
      classOf[UnsupportedOperationException],
      () => IndexUtils.buildIndexType("ivf_pq"))
  }

  @Test
  def batchFragments_respectsNumSegmentsAndDefaults(): Unit = {
    val fragments = fragmentWorkloads(1, 1, 1, 1)

    assertEquals(Seq(2, 2), IndexUtils.batchFragments(fragments, Some(2), 4).map(_.size))
    assertEquals(4, IndexUtils.batchFragments(fragments, Some(4), 4).size)
    assertEquals(4, IndexUtils.batchFragments(fragments, Some(10), 4).size)
    assertEquals(4, IndexUtils.batchFragments(fragments, None, 8).size)
    assertEquals(2, IndexUtils.batchFragments(fragments, None, 2).size)
    assertEquals(Seq.empty, IndexUtils.batchFragments(Nil, None, 4))
  }

  /**
   * Interleaved coverage makes Lance's compaction planner treat every adjacent fragment pair as
   * ungroupable, so OPTIMIZE stops coalescing the table entirely. Batches must be contiguous runs.
   */
  @Test
  def batchFragments_producesContiguousRuns(): Unit = {
    Seq(
      fragmentWorkloads(1, 1, 1, 1, 1, 1),
      fragmentWorkloads(100, 1, 1, 1),
      fragmentWorkloads(1, 1, 1, 100),
      fragmentWorkloads(5, 9, 2, 7, 3, 8, 1, 6),
      fragmentWorkloads(0, 0, 0, 0, 0)).foreach { fragments =>
      (1 to fragments.size).foreach { segments =>
        val batches = IndexUtils.batchFragments(fragments, Some(segments), 4)
        assertEquals(
          segments,
          batches.size,
          s"expected $segments batches for ${fragments.size} fragments, got $batches")
        assertContiguousBatches(batches)
        assertEquals(
          fragments.map(_.fragmentId),
          batches.flatten,
          "every fragment must be assigned exactly once")
      }
    }
  }

  @Test
  def batchFragments_balancesRowsDeterministically(): Unit = {
    val fragments = fragmentWorkloads(80, 50, 30, 20)
    // Splitting after fragment 0 gives 80 / 100; the contiguous alternative gives 130 / 50.
    val expected = Seq(
      List(java.lang.Integer.valueOf(0)),
      List(
        java.lang.Integer.valueOf(1),
        java.lang.Integer.valueOf(2),
        java.lang.Integer.valueOf(3)))

    assertEquals(expected, IndexUtils.batchFragments(fragments, Some(2), 4))
    assertEquals(expected, IndexUtils.batchFragments(fragments.reverse, Some(2), 4))
  }

  /** A single dominant fragment must not collapse the requested parallelism. */
  @Test
  def batchFragments_keepsRequestedParallelismUnderSkew(): Unit = {
    val batches = IndexUtils.batchFragments(fragmentWorkloads(100, 1, 1, 1), Some(4), 4)

    assertEquals(
      Seq(
        List(java.lang.Integer.valueOf(0)),
        List(java.lang.Integer.valueOf(1)),
        List(java.lang.Integer.valueOf(2)),
        List(java.lang.Integer.valueOf(3))),
      batches)
  }

  @Test
  def batchFragments_distributesZeroRowFragmentsAcrossSegments(): Unit = {
    val fragments = fragmentWorkloads(0, 0, 0, 0).reverse

    assertEquals(
      Seq(
        List(java.lang.Integer.valueOf(0)),
        List(java.lang.Integer.valueOf(1)),
        List(java.lang.Integer.valueOf(2), java.lang.Integer.valueOf(3))),
      IndexUtils.batchFragments(fragments, Some(3), 4))
  }

  /** Fragment ids are not necessarily dense or zero-based once a table has been compacted. */
  @Test
  def batchFragments_keepsSparseFragmentIdsContiguousByPosition(): Unit = {
    val fragments = List(
      FragmentWorkload(java.lang.Integer.valueOf(17), 10L),
      FragmentWorkload(java.lang.Integer.valueOf(4), 10L),
      FragmentWorkload(java.lang.Integer.valueOf(9), 10L),
      FragmentWorkload(java.lang.Integer.valueOf(31), 10L))

    assertEquals(
      Seq(
        List(java.lang.Integer.valueOf(4), java.lang.Integer.valueOf(9)),
        List(java.lang.Integer.valueOf(17), java.lang.Integer.valueOf(31))),
      IndexUtils.batchFragments(fragments, Some(2), 4))
  }

  private def workloads(batches: Seq[List[Integer]], rows: Seq[Long]): Seq[Long] =
    batches.map(_.map(id => rows(id.intValue)).sum)

  /** Smallest achievable heaviest batch over all contiguous partitions into `segmentCount` runs. */
  private def optimalHeaviestBatch(rows: Seq[Long], segmentCount: Int): Long = {
    def best(from: Int, runs: Int): Long =
      if (runs == 1) {
        rows.drop(from).sum
      } else {
        (from until rows.size - runs + 1).map { cut =>
          math.max(rows.slice(from, cut + 1).sum, best(cut + 1, runs - 1))
        }.min
      }
    best(0, segmentCount)
  }

  /**
   * The heaviest batch has to be as light as a contiguous partition allows. This workload is the
   * counter-example that sank an earlier prefix-crossing heuristic: it cut after the three
   * indivisible leading fragments had already overshot their even shares, leaving one batch of 162
   * where 95 is forced by fragment 0 alone.
   */
  @Test
  def batchFragments_minimisesTheHeaviestBatch(): Unit = {
    val rows = Seq(95L, 93L, 89L, 8L, 1L, 4L, 74L, 88L, 38L)
    val fragments = rows.zipWithIndex.map { case (count, fragmentId) =>
      FragmentWorkload(java.lang.Integer.valueOf(fragmentId), count)
    }.toList

    val batches = IndexUtils.batchFragments(fragments, Some(6), 1)

    assertEquals(6, batches.size)
    assertContiguousBatches(batches)
    assertEquals(
      95L,
      workloads(batches, rows).max,
      s"expected the optimal heaviest batch, got ${workloads(batches, rows)}")
  }

  /**
   * Optimality is checked against every contiguous partition rather than against a fixed expected
   * split, so the property is pinned instead of one of its consequences.
   */
  @Test
  def batchFragments_matchesTheOptimalContiguousPartition(): Unit = {
    val workloadShapes = Seq(
      Seq(95L, 93L, 89L, 8L, 1L, 4L, 74L, 88L, 38L),
      Seq(100L, 1L, 1L, 1L, 1L, 1L),
      Seq(1L, 1L, 1L, 1L, 1L, 100L),
      Seq(5L, 9L, 2L, 7L, 3L, 8L, 1L, 6L),
      Seq(7L, 7L, 7L, 7L, 7L, 7L, 7L),
      Seq(50L, 1L, 50L, 1L, 50L, 1L, 50L),
      Seq(0L, 0L, 5L, 0L, 0L))

    workloadShapes.foreach { rows =>
      val fragments = rows.zipWithIndex.map { case (count, fragmentId) =>
        FragmentWorkload(java.lang.Integer.valueOf(fragmentId), count)
      }.toList
      (1 to rows.size).foreach { segmentCount =>
        val batches = IndexUtils.batchFragments(fragments, Some(segmentCount), 1)
        assertEquals(segmentCount, batches.size, s"$rows into $segmentCount")
        assertContiguousBatches(batches)
        assertEquals(
          optimalHeaviestBatch(rows, segmentCount),
          workloads(batches, rows).max,
          s"$rows into $segmentCount batches: got ${workloads(batches, rows)}")
      }
    }
  }

  @Test
  def batchFragments_rejectsWorkloadOverflow(): Unit = {
    assertThrows(
      classOf[ArithmeticException],
      () => IndexUtils.batchFragments(fragmentWorkloads(Long.MaxValue, 1), Some(1), 1))
  }
}
