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
 * Pure unit tests for [[IndexUtils]] helper methods.
 *
 * Written with JUnit 5 (not ScalaTest) so surefire actually executes them: this
 * project's surefire is configured for the JUnit Platform Provider only; classes
 * extending AnyFunSuite silently run zero tests. See VectorIndexParamsResolverTest
 * and LargeBinaryWriterTest for the same pattern.
 */
class IndexUtilsTest {

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

  @Test
  def buildIndexTypeMapsScalarMethodsCaseInsensitive(): Unit = {
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("btree"))
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("BTREE"))
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("BTree"))
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("fts"))
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("FTS"))
    assertEquals(IndexType.ZONEMAP, IndexUtils.buildIndexType("zonemap"))
    assertEquals(IndexType.ZONEMAP, IndexUtils.buildIndexType("ZONEMAP"))
  }

  @Test
  def buildIndexTypeMapsFiveIvfMethodsCaseInsensitive(): Unit = {
    val pairs = Seq(
      "ivf_flat" -> IndexType.IVF_FLAT,
      "IVF_FLAT" -> IndexType.IVF_FLAT,
      "ivf_pq" -> IndexType.IVF_PQ,
      "ivf_sq" -> IndexType.IVF_SQ,
      "ivf_hnsw_pq" -> IndexType.IVF_HNSW_PQ,
      "ivf_hnsw_sq" -> IndexType.IVF_HNSW_SQ)
    pairs.foreach { case (method, expected) =>
      assertEquals(expected, IndexUtils.buildIndexType(method), s"method=$method")
    }
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

  // Exercises every scalar-segment method's mapping to IndexType
  // AND the core scalar-plugin param-type name (which uses "labellist" / "bloomfilter"
  // spellings that don't match either the SQL method names or the enum names).
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
      () => IndexUtils.buildIndexType("unknown_index"))
  }

  @Test
  def buildIndexType_throwsClearErrorForIvfHnswFlat(): Unit = {
    val ex = assertThrows(
      classOf[UnsupportedOperationException],
      () => IndexUtils.buildIndexType("ivf_hnsw_flat"))
    assertTrue(ex.getMessage.contains("IVF_HNSW_FLAT"), ex.getMessage)
    assertTrue(ex.getMessage.contains("ivf_hnsw_pq"), ex.getMessage)
    assertTrue(ex.getMessage.contains("ivf_hnsw_sq"), ex.getMessage)
  }

  @Test
  def useLogicalSegmentCommitTrueForScalarSegmentAndSupportedIvf(): Unit = {
    Seq(
      IndexType.BTREE,
      IndexType.ZONEMAP,
      IndexType.BITMAP,
      IndexType.LABEL_LIST,
      IndexType.NGRAM,
      IndexType.BLOOM_FILTER,
      IndexType.RTREE,
      IndexType.INVERTED,
      IndexType.IVF_FLAT,
      IndexType.IVF_PQ,
      IndexType.IVF_SQ,
      IndexType.IVF_HNSW_PQ,
      IndexType.IVF_HNSW_SQ).foreach { indexType =>
      assertTrue(
        IndexUtils.useLogicalSegmentCommit(indexType),
        s"$indexType should use logical-segment commit")
    }
  }

  @Test
  def useLogicalSegmentCommitFalseForUnsupported(): Unit = {
    // BTREE now goes through the shared scalar-segment path — it is no longer
    // an exception to logical-segment commit. Only IVF_HNSW_FLAT stays unsupported in the
    // first pass: lance-core Java VectorIndexParams.Builder.build() rejects HNSW without a
    // PQ/SQ quantizer.
    assertFalse(IndexUtils.useLogicalSegmentCommit(IndexType.IVF_HNSW_FLAT))
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
}
