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

  private def fragmentWorkloads(rows: Long*): List[FragmentWorkload] =
    rows.zipWithIndex.map { case (rowCount, fragmentId) =>
      FragmentWorkload(java.lang.Integer.valueOf(fragmentId), rowCount)
    }.toList

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

  @Test
  def batchFragments_balancesRowsDeterministically(): Unit = {
    val fragments = fragmentWorkloads(80, 50, 30, 20)
    val expected = Seq(
      List(java.lang.Integer.valueOf(0), java.lang.Integer.valueOf(3)),
      List(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2)))

    assertEquals(expected, IndexUtils.batchFragments(fragments, Some(2), 4))
    assertEquals(expected, IndexUtils.batchFragments(fragments.reverse, Some(2), 4))
  }

  @Test
  def batchFragments_distributesZeroRowFragmentsAcrossSegments(): Unit = {
    val fragments = fragmentWorkloads(0, 0, 0, 0).reverse

    assertEquals(
      Seq(
        List(java.lang.Integer.valueOf(0), java.lang.Integer.valueOf(3)),
        List(java.lang.Integer.valueOf(1)),
        List(java.lang.Integer.valueOf(2))),
      IndexUtils.batchFragments(fragments, Some(3), 4))
  }

  @Test
  def batchFragments_rejectsWorkloadOverflow(): Unit = {
    assertThrows(
      classOf[ArithmeticException],
      () => IndexUtils.batchFragments(fragmentWorkloads(Long.MaxValue, 1), Some(1), 1))
  }

  // ── declaredCoverage / committedCoverage ───────────────────────

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

  /** A segment keeping its identity but reporting narrower coverage, as a pruned commit returns. */
  private def prunedTo(built: Index, fragmentIds: Seq[Int]): Index =
    Index
      .builder()
      .uuid(built.uuid)
      .name(built.name)
      .indexType(built.indexType)
      .fragments(fragmentIds.map(java.lang.Integer.valueOf).asJava)
      .build()

  @Test
  def requireCommittableCoverage_acceptsCoverageThatSurvives(): Unit = {
    IndexUtils.requireCommittableCoverage(Set(1, 5), Seq(coveringSegment(0, 1)), "idx_id")
  }

  @Test
  def requireCommittableCoverage_acceptsSegmentsThatDeclareNothing(): Unit = {
    IndexUtils.requireCommittableCoverage(Set(1), Seq(segment(None)), "idx_id")
    IndexUtils.requireCommittableCoverage(Set.empty[Int], Seq.empty, "idx_id")
  }

  @Test
  def requireCommittableCoverage_refusesASetThatWouldCoverNothing(): Unit = {
    val error = assertThrows(
      classOf[IllegalStateException],
      () => IndexUtils.requireCommittableCoverage(Set(5), Seq(coveringSegment(0, 7)), "idx_id"))

    assertTrue(error.getMessage.contains("idx_id"), error.getMessage)
    assertTrue(error.getMessage.contains("0, 7"), error.getMessage)
    assertTrue(error.getMessage.contains("re-run"), error.getMessage)
  }

  @Test
  def requireCommittableCoverage_summarizesLargeRetiredSets(): Unit = {
    val error = assertThrows(
      classOf[IllegalStateException],
      () =>
        IndexUtils.requireCommittableCoverage(
          Set.empty[Int],
          Seq(coveringSegment(0 to 20: _*)),
          "idx_id"))

    assertTrue(error.getMessage.contains("21 total"), error.getMessage)
  }

  // ── establishedCoverage ───────────────────────────────────────────────────

  @Test
  def establishedCoverage_reportsWhatTheCommitReturned(): Unit = {
    val built = Seq(coveringSegment(0, 1), coveringSegment(2))

    assertEquals(
      Set(0, 1, 2),
      IndexUtils.establishedCoverage(built, built, Set(0, 1, 2), "idx_id"))
  }

  /**
   * Lance prunes a fragment whose indexed field was rewritten under the same id, so the committed
   * bitmap can be narrower than the one handed in while every fragment is still live. No comparison
   * of fragment ids can see that, which is why the report has to come from what the commit returned.
   */
  @Test
  def establishedCoverage_followsAPrunedCommitEvenWhileEveryFragmentIsLive(): Unit = {
    val built = coveringSegment(0, 1)

    assertEquals(
      Set(0),
      IndexUtils.establishedCoverage(
        Seq(built),
        Seq(prunedTo(built, Seq(0))),
        Set(0, 1),
        "idx_id"))
  }

  @Test
  def establishedCoverage_excludesFragmentsRetiredByTheCommit(): Unit = {
    val built = coveringSegment(0, 1)

    assertEquals(
      Set(1),
      IndexUtils.establishedCoverage(Seq(built), Seq(built), Set(1, 5), "idx_id"))
  }

  /** Existing segments survive a commit they are disjoint from; their coverage is not ours. */
  @Test
  def establishedCoverage_countsOnlyTheSegmentsThisBuildProduced(): Unit = {
    val built = coveringSegment(2)
    val survivor = coveringSegment(0, 1)

    assertEquals(
      Set(2),
      IndexUtils.establishedCoverage(
        Seq(built),
        Seq(survivor, built),
        Set(0, 1, 2),
        "idx_id"))
  }

  @Test
  def establishedCoverage_isEmptyWhenNothingOfThisBuildSurvived(): Unit = {
    val built = coveringSegment(0)

    assertEquals(
      Set.empty[Int],
      IndexUtils.establishedCoverage(
        Seq(built),
        Seq(prunedTo(built, Seq.empty)),
        Set(0),
        "idx_id"))
  }

}
