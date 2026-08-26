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
import org.lance.index.{Index, IndexType}

import scala.collection.JavaConverters._

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

  /** An index segment carrying only the metadata these helpers read. */
  private def segment(
      fragmentIds: Option[Seq[Int]],
      indexDetails: Option[Array[Byte]] = None): Index = {
    val builder = Index
      .builder()
      .uuid(java.util.UUID.randomUUID())
      .name("idx_id")
      .indexType(IndexType.INVERTED)
    fragmentIds.foreach(ids =>
      builder.fragments(ids.map(java.lang.Integer.valueOf).asJava))
    indexDetails.foreach(builder.indexDetails)
    builder.build()
  }

  private def coveringSegment(fragmentIds: Int*): Index = segment(Some(fragmentIds))

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

  @Test
  def batchFragments_rejectsWorkloadOverflow(): Unit = {
    assertThrows(
      classOf[ArithmeticException],
      () => IndexUtils.batchFragments(fragmentWorkloads(Long.MaxValue, 1), Some(1), 1))
  }

  // ── methodForIndexType ────────────────────────────────────────────────────

  @Test
  def methodForIndexType_roundTripsEverySegmentBuildableType(): Unit = {
    val types = Seq(
      IndexType.BTREE,
      IndexType.ZONEMAP,
      IndexType.BITMAP,
      IndexType.LABEL_LIST,
      IndexType.NGRAM,
      IndexType.BLOOM_FILTER,
      IndexType.RTREE,
      IndexType.INVERTED)

    types.foreach { indexType =>
      val method = IndexUtils.methodForIndexType(indexType)
      assertTrue(method.isDefined, s"expected a method name for $indexType")
      assertEquals(
        Some(indexType),
        IndexUtils.scalarSegmentIndexType(method.get),
        s"method '${method.get}' should resolve back to $indexType")
    }
  }

  @Test
  def methodForIndexType_mapsInvertedToCanonicalFtsSpelling(): Unit = {
    assertEquals(Some("fts"), IndexUtils.methodForIndexType(IndexType.INVERTED))
  }

  @Test
  def methodForIndexType_returnsNoneForVectorType(): Unit = {
    assertEquals(None, IndexUtils.methodForIndexType(IndexType.VECTOR))
  }

  @Test
  def methodForIndexType_returnsNoneForNull(): Unit = {
    assertEquals(None, IndexUtils.methodForIndexType(null))
  }

  // ── isSystemIndex ─────────────────────────────────────────────────────────

  @Test
  def isSystemIndex_matchesLanceMaintainedIndexesCaseInsensitively(): Unit = {
    assertTrue(IndexUtils.isSystemIndex("__lance_frag_reuse"))
    assertTrue(IndexUtils.isSystemIndex("__LANCE_MEM_WAL"))
  }

  @Test
  def isSystemIndex_rejectsUserIndexesAndNull(): Unit = {
    assertFalse(IndexUtils.isSystemIndex("idx_id"))
    assertFalse(IndexUtils.isSystemIndex(null))
  }

  // ── declaredCoverage / committedCoverage ──────────────────────────────────

  @Test
  def declaredCoverage_unionsSegmentBitmaps(): Unit = {
    assertEquals(
      Set(0, 1, 4),
      IndexUtils.declaredCoverage(Seq(coveringSegment(0, 1), coveringSegment(4))))
  }

  @Test
  def declaredCoverage_treatsAbsentBitmapAsNoCoverage(): Unit = {
    assertEquals(Set(2), IndexUtils.declaredCoverage(Seq(segment(None), coveringSegment(2))))
    assertEquals(Set.empty[Int], IndexUtils.declaredCoverage(Seq.empty))
  }

  @Test
  def committedCoverage_returnsDeclaredCoverageWhenEverythingIsLive(): Unit = {
    assertEquals(
      Set(0, 2),
      IndexUtils.committedCoverage(
        Set(0, 1, 2),
        Seq(coveringSegment(0), coveringSegment(2)),
        "idx_id"))
  }

  /**
   * A fragment can leave the manifest because its rows moved or because they were all deleted. Only
   * the first leaves data unindexed, and either way the segments covering what remains are correct,
   * so the command reports the coverage it achieved rather than discarding the whole build.
   */
  @Test
  def committedCoverage_dropsRetiredFragmentsAndKeepsTheRest(): Unit = {
    assertEquals(
      Set(1),
      IndexUtils.committedCoverage(Set(1, 5), Seq(coveringSegment(0, 1)), "idx_id"))
  }

  @Test
  def committedCoverage_failsWhenNothingWouldBeCovered(): Unit = {
    val error = assertThrows(
      classOf[IllegalStateException],
      () => IndexUtils.committedCoverage(Set(5), Seq(coveringSegment(0, 7)), "idx_id"))

    assertTrue(error.getMessage.contains("idx_id"), error.getMessage)
    assertTrue(error.getMessage.contains("0, 7"), error.getMessage)
    assertTrue(error.getMessage.contains("re-run"), error.getMessage)
  }

  @Test
  def committedCoverage_summarizesLargeRetiredSets(): Unit = {
    val error = assertThrows(
      classOf[IllegalStateException],
      () =>
        IndexUtils.committedCoverage(Set.empty[Int], Seq(coveringSegment(0 to 20: _*)), "idx_id"))

    assertTrue(error.getMessage.contains("21 total"), error.getMessage)
  }

  // ── retainedSegments ──────────────────────────────────────────────────────

  private def namedSegment(name: String, fragmentIds: Int*): Index =
    Index
      .builder()
      .uuid(java.util.UUID.randomUUID())
      .name(name)
      .indexType(IndexType.ZONEMAP)
      .fragments(fragmentIds.map(java.lang.Integer.valueOf).asJava)
      .build()

  @Test
  def retainedSegments_keepsOnlySegmentsOfTheNamedIndexThatStillCoverLiveFragments(): Unit = {
    val kept = namedSegment("idx_id", 0, 1)
    val allRetired = namedSegment("idx_id", 7)
    val otherIndex = namedSegment("idx_other", 0)

    assertEquals(
      Seq(kept),
      IndexUtils.retainedSegments(Seq(kept, allRetired, otherIndex), "idx_id", Set(0, 1)))
  }

  /**
   * A commit against a name Lance no longer knows creates that index instead of extending it, so a
   * DROP INDEX during the build would otherwise be undone with only the planned coverage.
   */
  @Test
  def retainedSegments_failsWhenTheIndexIsGone(): Unit = {
    val error = assertThrows(
      classOf[IllegalStateException],
      () => IndexUtils.retainedSegments(Seq(namedSegment("idx_other", 0)), "idx_id", Set(0)))

    assertTrue(error.getMessage.contains("idx_id"), error.getMessage)
    assertTrue(error.getMessage.contains("no longer exists"), error.getMessage)
    assertTrue(error.getMessage.contains("CREATE INDEX"), error.getMessage)
  }

  @Test
  def retainedSegments_matchesIndexNameExactly(): Unit = {
    assertThrows(
      classOf[IllegalStateException],
      () => IndexUtils.retainedSegments(Seq(namedSegment("IDX_ID", 0)), "idx_id", Set(0)))
  }

  // ── requireUniformSegmentDetails ──────────────────────────────────────────

  @Test
  def requiresUniformSegmentDetails_onlyForInverted(): Unit = {
    assertTrue(IndexUtils.requiresUniformSegmentDetails(IndexType.INVERTED))
    Seq(
      IndexType.BTREE,
      IndexType.ZONEMAP,
      IndexType.BITMAP,
      IndexType.LABEL_LIST,
      IndexType.NGRAM,
      IndexType.BLOOM_FILTER,
      IndexType.RTREE,
      IndexType.VECTOR).foreach { indexType =>
      assertFalse(IndexUtils.requiresUniformSegmentDetails(indexType), indexType.name())
    }
    assertFalse(IndexUtils.requiresUniformSegmentDetails(null))
  }

  @Test
  def requireUniformSegmentDetails_acceptsMatchingDetails(): Unit = {
    val details = Array[Byte](1, 2, 3)
    IndexUtils.requireUniformSegmentDetails(
      IndexType.INVERTED,
      "idx_id",
      "fts",
      Seq(segment(Some(Seq(0)), Some(details.clone()))),
      Seq(segment(Some(Seq(1)), Some(details.clone()))))
  }

  /**
   * An inverted index loads one set of details for the whole logical index, so segments built with
   * different options fail every full-text query. Rejecting before the commit leaves it untouched.
   */
  @Test
  def requireUniformSegmentDetails_rejectsDifferingDetails(): Unit = {
    val error = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        IndexUtils.requireUniformSegmentDetails(
          IndexType.INVERTED,
          "idx_id",
          "fts",
          Seq(segment(Some(Seq(0)), Some(Array[Byte](1, 2, 3)))),
          Seq(segment(Some(Seq(1)), Some(Array[Byte](1, 2, 4))))))

    assertTrue(error.getMessage.contains("idx_id"), error.getMessage)
    assertTrue(error.getMessage.contains("fts"), error.getMessage)
    assertTrue(error.getMessage.contains("CREATE INDEX"), error.getMessage)
  }

  @Test
  def requireUniformSegmentDetails_rejectsDisagreementWithinEitherSide(): Unit = {
    val a = Array[Byte](1)
    val b = Array[Byte](2)
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        IndexUtils.requireUniformSegmentDetails(
          IndexType.INVERTED,
          "idx_id",
          "fts",
          Seq(segment(Some(Seq(0)), Some(a)), segment(Some(Seq(1)), Some(b))),
          Seq(segment(Some(Seq(2)), Some(a)))))
  }

  @Test
  def requireUniformSegmentDetails_ignoresIndexTypesWithoutTheRequirement(): Unit = {
    IndexUtils.requireUniformSegmentDetails(
      IndexType.ZONEMAP,
      "idx_id",
      "zonemap",
      Seq(segment(Some(Seq(0)), Some(Array[Byte](1)))),
      Seq(segment(Some(Seq(1)), Some(Array[Byte](2)))))
  }

  /** Nothing to compare against: a segment predating index details, or a wholesale replacement. */
  @Test
  def requireUniformSegmentDetails_defersWhenEitherSideHasNoDetails(): Unit = {
    IndexUtils.requireUniformSegmentDetails(
      IndexType.INVERTED,
      "idx_id",
      "fts",
      Seq(segment(Some(Seq(0)))),
      Seq(segment(Some(Seq(1)), Some(Array[Byte](1)))))
    IndexUtils.requireUniformSegmentDetails(
      IndexType.INVERTED,
      "idx_id",
      "fts",
      Seq.empty,
      Seq(segment(Some(Seq(1)), Some(Array[Byte](1)))))
  }

  // ── parseNumSegments ──────────────────────────────────────────────────────

  @Test
  def parseNumSegments_acceptsPositiveIntegers(): Unit = {
    assertEquals(
      8,
      IndexUtils.parseNumSegments(LanceNamedArgument("num_segments", java.lang.Long.valueOf(8))))
  }

  @Test
  def parseNumSegments_rejectsNonPositiveValues(): Unit = {
    Seq(0L, -1L).foreach { value =>
      assertThrows(
        classOf[IllegalArgumentException],
        () =>
          IndexUtils.parseNumSegments(
            LanceNamedArgument("num_segments", java.lang.Long.valueOf(value))))
    }
  }

  @Test
  def parseNumSegments_rejectsValuesWiderThanInt(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        IndexUtils.parseNumSegments(
          LanceNamedArgument("num_segments", java.lang.Long.valueOf(Int.MaxValue.toLong + 1))))
  }

  @Test
  def parseNumSegments_rejectsNonNumericAndNullValues(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => IndexUtils.parseNumSegments(LanceNamedArgument("num_segments", "eight")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => IndexUtils.parseNumSegments(LanceNamedArgument("num_segments", null)))
  }
}
