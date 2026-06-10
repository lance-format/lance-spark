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
 * Pure unit tests for [[VectorIndexParamsResolver.parseAndValidate]] — no Spark, no Dataset.
 *
 * Written with JUnit 5 (not ScalaTest) so surefire actually executes them: this project's
 * surefire configuration uses only the JUnit Platform Provider with no scalatest-junit5
 * bridge, so any class extending AnyFunSuite silently runs zero tests.
 */
class VectorIndexParamsResolverTest {

  // ---------- IVF_FLAT defaults ----------
  @Test
  def ivfFlatUsesDefaults(): Unit = {
    val plan = VectorIndexParamsResolver.parseAndValidate(
      IndexType.IVF_FLAT,
      Seq.empty,
      dim = 32,
      numRows = 400L)
    assertEquals(IndexType.IVF_FLAT, plan.indexType)
    assertEquals("L2", plan.distanceTypeName)
    assertEquals(20, plan.ivf.numPartitions) // sqrt(400) = 20
    assertEquals(256, plan.ivf.sampleRate)
    assertEquals(50, plan.ivf.maxIters)
    assertTrue(plan.pq.isEmpty)
    assertTrue(plan.sq.isEmpty)
    assertTrue(plan.hnsw.isEmpty)
  }

  // ---------- IVF_PQ default num_sub_vectors ----------
  @Test
  def ivfPqInfersNumSubVectorsFromDimDiv16(): Unit = {
    val plan = VectorIndexParamsResolver.parseAndValidate(
      IndexType.IVF_PQ,
      Seq.empty,
      dim = 32,
      numRows = 400L)
    assertEquals(2, plan.pq.get.numSubVectors) // 32 / 16
    assertEquals(8, plan.pq.get.numBits) // default
  }

  @Test
  def ivfPqInfersNumSubVectorsFromDimDiv8WhenNotDiv16(): Unit = {
    val plan = VectorIndexParamsResolver.parseAndValidate(
      IndexType.IVF_PQ,
      Seq.empty,
      dim = 24,
      numRows = 400L)
    assertEquals(3, plan.pq.get.numSubVectors) // 24 / 8
  }

  @Test
  def ivfPqRejectsWhenDimNotDivisibleBy16Or8(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_PQ,
          Seq.empty,
          dim = 23,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("not divisible by 16 or 8"))
  }

  @Test
  def ivfPqAcceptsExplicitNumSubVectorsThatDividesDim(): Unit = {
    val plan = VectorIndexParamsResolver.parseAndValidate(
      IndexType.IVF_PQ,
      Seq(LanceNamedArgument("num_sub_vectors", java.lang.Long.valueOf(8L))),
      dim = 32,
      numRows = 400L)
    assertEquals(8, plan.pq.get.numSubVectors)
  }

  @Test
  def ivfPqRejectsNumSubVectorsThatDoesNotDivideDim(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_PQ,
          Seq(LanceNamedArgument("num_sub_vectors", java.lang.Long.valueOf(7L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("does not"))
  }

  // ---------- distance_type ----------
  @Test
  def distanceTypeParsingIsCaseInsensitiveAndMapsEuclideanToL2(): Unit = {
    Seq(
      "l2" -> "L2",
      "L2" -> "L2",
      "Cosine" -> "Cosine",
      "COSINE" -> "Cosine",
      "dot" -> "Dot",
      "hamming" -> "Hamming",
      "euclidean" -> "L2",
      "EUCLIDEAN" -> "L2").foreach { case (input, expected) =>
      val plan = VectorIndexParamsResolver.parseAndValidate(
        IndexType.IVF_FLAT,
        Seq(LanceNamedArgument("distance_type", input)),
        dim = 32,
        numRows = 400L)
      assertEquals(expected, plan.distanceTypeName, s"input=$input")
    }
  }

  @Test
  def distanceTypeUnknownIsRejectedWithValidListInError(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_FLAT,
          Seq(LanceNamedArgument("distance_type", "manhattan")),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("manhattan"))
    assertTrue(ex.getMessage.contains("l2"))
    assertTrue(ex.getMessage.contains("cosine"))
  }

  // ---------- key whitelist ----------
  @Test
  def unknownWithKeyIsRejected(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_FLAT,
          Seq(LanceNamedArgument("zone_size", java.lang.Long.valueOf(2048L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("zone_size"))
  }

  @Test
  def numSubVectorsIsRejectedForIvfFlat(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_FLAT,
          Seq(LanceNamedArgument("num_sub_vectors", java.lang.Long.valueOf(4L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("only supported for IVF_PQ"))
  }

  @Test
  def hnswParamsAreRejectedForNonHnswTypes(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_PQ,
          Seq(LanceNamedArgument("m", java.lang.Long.valueOf(16L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("IVF_HNSW"))
  }

  @Test
  def duplicateKeysAreRejected(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_FLAT,
          Seq(
            LanceNamedArgument("num_partitions", java.lang.Long.valueOf(8L)),
            LanceNamedArgument("num_partitions", java.lang.Long.valueOf(16L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("Duplicate"))
    assertTrue(ex.getMessage.contains("num_partitions"))
  }

  // ---------- numeric validation ----------
  @Test
  def numPartitionsExceedingNumRowsIsRejected(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_FLAT,
          Seq(LanceNamedArgument("num_partitions", java.lang.Long.valueOf(10000L))),
          dim = 32,
          numRows = 200L))
    assertTrue(ex.getMessage.contains("cannot exceed total rows"))
  }

  @Test
  def numPartitionsDefaultsToAtLeastOneEvenWhenNumRowsIsZero(): Unit = {
    val plan = VectorIndexParamsResolver.parseAndValidate(
      IndexType.IVF_FLAT,
      Seq.empty,
      dim = 32,
      numRows = 0L)
    assertEquals(1, plan.ivf.numPartitions)
  }

  @Test
  def sampleRateBelowTwoIsRejected(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_FLAT,
          Seq(LanceNamedArgument("sample_rate", java.lang.Long.valueOf(1L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("sample_rate"))
  }

  // ---------- HNSW ----------
  @Test
  def ivfHnswPqDefaultValues(): Unit = {
    val plan = VectorIndexParamsResolver.parseAndValidate(
      IndexType.IVF_HNSW_PQ,
      Seq.empty,
      dim = 32,
      numRows = 400L)
    val hnsw = plan.hnsw.get
    assertEquals(20, hnsw.m)
    assertEquals(150, hnsw.efConstruction)
    assertEquals(7, hnsw.maxLevel)
    assertEquals(Some(2), hnsw.prefetchDistance)
    assertTrue(plan.pq.isDefined)
  }

  @Test
  def ivfHnswSqExplicitOverridesLandOnHnswPlan(): Unit = {
    val plan = VectorIndexParamsResolver.parseAndValidate(
      IndexType.IVF_HNSW_SQ,
      Seq(
        LanceNamedArgument("m", java.lang.Long.valueOf(16L)),
        LanceNamedArgument("ef_construction", java.lang.Long.valueOf(200L)),
        LanceNamedArgument("max_level", java.lang.Long.valueOf(5L))),
      dim = 32,
      numRows = 400L)
    val hnsw = plan.hnsw.get
    assertEquals(16, hnsw.m)
    assertEquals(200, hnsw.efConstruction)
    assertEquals(5, hnsw.maxLevel)
    assertTrue(plan.sq.isDefined)
  }

  @Test
  def numBitsRejectedForIvfFlatPointsToSqAndPq(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_FLAT,
          Seq(LanceNamedArgument("num_bits", java.lang.Long.valueOf(8L))),
          dim = 32,
          numRows = 400L))
    // Error must mention all four supported types so users targeting SQ aren't misdirected.
    assertTrue(ex.getMessage.contains("IVF_PQ"), ex.getMessage)
    assertTrue(ex.getMessage.contains("IVF_SQ"), ex.getMessage)
    assertTrue(ex.getMessage.contains("IVF_HNSW_PQ"), ex.getMessage)
    assertTrue(ex.getMessage.contains("IVF_HNSW_SQ"), ex.getMessage)
  }

  @Test
  def hnswMRejectedWhenZero(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_HNSW_SQ,
          Seq(LanceNamedArgument("m", java.lang.Long.valueOf(0L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("m must be positive"), ex.getMessage)
  }

  @Test
  def hnswEfConstructionRejectedWhenNegative(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_HNSW_SQ,
          Seq(LanceNamedArgument("ef_construction", java.lang.Long.valueOf(-1L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("ef_construction"), ex.getMessage)
  }

  @Test
  def hnswPrefetchDistanceRejectedWhenNegative(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        VectorIndexParamsResolver.parseAndValidate(
          IndexType.IVF_HNSW_SQ,
          Seq(LanceNamedArgument("prefetch_distance", java.lang.Long.valueOf(-1L))),
          dim = 32,
          numRows = 400L))
    assertTrue(ex.getMessage.contains("prefetch_distance"), ex.getMessage)
  }
}
