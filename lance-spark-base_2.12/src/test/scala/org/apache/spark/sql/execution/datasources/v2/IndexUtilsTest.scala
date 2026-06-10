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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.index.IndexType

/**
 * Pure unit tests for [[IndexUtils]] dispatcher and fragment-batching helpers.
 *
 * Written with JUnit 5 (not ScalaTest) so surefire actually executes them: this
 * project's surefire is configured for the JUnit Platform Provider only; classes
 * extending AnyFunSuite silently run zero tests. See VectorIndexParamsResolverTest
 * and LargeBinaryWriterTest for the same pattern.
 */
class IndexUtilsTest {

  @Test
  def buildIndexTypeMapsFiveIvfMethodsCaseInsensitive(): Unit = {
    val pairs = Seq(
      "ivf_flat" -> IndexType.IVF_FLAT,
      "IVF_FLAT" -> IndexType.IVF_FLAT,
      "ivf_pq" -> IndexType.IVF_PQ,
      "ivf_sq" -> IndexType.IVF_SQ,
      "ivf_hnsw_pq" -> IndexType.IVF_HNSW_PQ,
      "ivf_hnsw_sq" -> IndexType.IVF_HNSW_SQ)
    pairs.foreach { case (m, expected) =>
      assertEquals(expected, IndexUtils.buildIndexType(m), s"method=$m")
    }
  }

  @Test
  def useLogicalSegmentCommitTrueForZonemapAndSupportedIvf(): Unit = {
    Seq(
      IndexType.ZONEMAP,
      IndexType.IVF_FLAT,
      IndexType.IVF_PQ,
      IndexType.IVF_SQ,
      IndexType.IVF_HNSW_PQ,
      IndexType.IVF_HNSW_SQ).foreach { t =>
      assertTrue(
        IndexUtils.useLogicalSegmentCommit(t),
        s"$t should use logical-segment commit")
    }
  }

  @Test
  def useLogicalSegmentCommitFalseForUnsupported(): Unit = {
    assertFalse(IndexUtils.useLogicalSegmentCommit(IndexType.BTREE))
    assertFalse(IndexUtils.useLogicalSegmentCommit(IndexType.INVERTED))
    // IVF_HNSW_FLAT is unsupported in the first pass: lance-core 7.0.0 Java
    // VectorIndexParams.Builder.build() rejects HNSW without a PQ/SQ quantizer.
    assertFalse(IndexUtils.useLogicalSegmentCommit(IndexType.IVF_HNSW_FLAT))
  }

  @Test
  def batchFragmentsRespectsNumSegmentsAndClamps(): Unit = {
    val ids = (0 until 4).map(java.lang.Integer.valueOf).toList

    // explicit num_segments=2: split 4 ids into [2, 2]
    assertEquals(Seq(2, 2), IndexUtils.batchFragments(ids, Some(2), 4).map(_.size))

    // num_segments larger than fragment count: clamp to n=4
    assertEquals(4, IndexUtils.batchFragments(ids, Some(10), 4).size)

    // None + parallelism > n: clamp to n
    assertEquals(4, IndexUtils.batchFragments(ids, None, 8).size)

    // None + parallelism < n: parallelism wins
    assertEquals(2, IndexUtils.batchFragments(ids, None, 2).size)

    // empty input: empty result
    assertEquals(Seq.empty, IndexUtils.batchFragments(Nil, None, 4))
  }

  @Test
  def batchFragmentsCoversAllFragmentsExactlyOnce(): Unit = {
    val ids = (0 until 7).map(java.lang.Integer.valueOf).toList
    val batches = IndexUtils.batchFragments(ids, Some(3), 4)
    val flattened = batches.flatten
    assertEquals(7, flattened.size, "all 7 ids must appear")
    assertEquals(7, flattened.distinct.size, "no duplicates")
    assertTrue(batches.forall(_.nonEmpty), "no empty batch")
  }
}
