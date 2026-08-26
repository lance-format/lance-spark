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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Unit tests for [[TopKHeap]]. The heap's correctness is the foundation of the merge stage —
 * any off-by-one or wrong-direction ordering would silently corrupt top-K results. We test both
 * metric directions explicitly.
 */
class TopKHeapTest {

  private def ref(addr: Long, score: Float): ScoredRowRef = ScoredRowRef(addr, score)

  /** Distance metric: smaller score is better. Top-K must hold the K smallest. */
  @Test def testDistanceKeepsKSmallest(): Unit = {
    val heap = new TopKHeap(k = 3, smallerIsBetter = true)
    Seq(5.0f, 1.0f, 4.0f, 2.0f, 8.0f, 0.5f).zipWithIndex.foreach { case (s, i) =>
      heap.offer(ref(i.toLong, s))
    }
    val out = heap.drain()
    val scores = out.map(_.score).toSeq
    assertEquals(Seq(0.5f, 1.0f, 2.0f), scores, "distance heap should retain three smallest")
  }

  /** Similarity metric: larger score is better. Top-K must hold the K largest. */
  @Test def testSimilarityKeepsKLargest(): Unit = {
    val heap = new TopKHeap(k = 3, smallerIsBetter = false)
    Seq(5.0f, 1.0f, 4.0f, 2.0f, 8.0f, 0.5f).zipWithIndex.foreach { case (s, i) =>
      heap.offer(ref(i.toLong, s))
    }
    val out = heap.drain()
    val scores = out.map(_.score).toSeq
    assertEquals(Seq(8.0f, 5.0f, 4.0f), scores, "similarity heap should retain three largest")
  }

  /** Drain order is best-first regardless of insertion order. */
  @Test def testDrainOrderIsBestFirst(): Unit = {
    val heap = new TopKHeap(k = 4, smallerIsBetter = true)
    heap.offerAll(Seq(ref(1, 9f), ref(2, 1f), ref(3, 5f), ref(4, 3f), ref(5, 2f)))
    val drained = heap.drain()
    val scores = drained.map(_.score).toSeq
    assertEquals(Seq(1f, 2f, 3f, 5f), scores)
    assertTrue(heap.isEmpty, "drain should leave the heap empty")
  }

  /** Heap with fewer than K elements drains them all in best-first order. */
  @Test def testFewerThanKReturnsAll(): Unit = {
    val heap = new TopKHeap(k = 10, smallerIsBetter = true)
    heap.offerAll(Seq(ref(1, 3f), ref(2, 1f), ref(3, 2f)))
    assertEquals(Seq(1f, 2f, 3f), heap.drain().map(_.score).toSeq)
  }

  /** A worse-than-current-worst candidate is rejected. */
  @Test def testRejectsWorseCandidate(): Unit = {
    val heap = new TopKHeap(k = 2, smallerIsBetter = true)
    heap.offer(ref(1, 1f))
    heap.offer(ref(2, 2f))
    heap.offer(ref(3, 5f)) // worse than existing 2 → rejected
    val drained = heap.drain()
    assertEquals(Seq(1f, 2f), drained.map(_.score).toSeq)
    assertEquals(Seq(1L, 2L), drained.map(_.rowAddr).toSeq)
  }

  /** `merge` combines two pre-sorted arrays preserving top-K. */
  @Test def testMergeCombinesTwoArrays(): Unit = {
    val a = Array(ref(1, 1f), ref(2, 3f), ref(3, 5f))
    val b = Array(ref(4, 2f), ref(5, 4f), ref(6, 6f))
    val merged = TopKHeap.merge(a, b, k = 4, smallerIsBetter = true)
    assertEquals(Seq(1f, 2f, 3f, 4f), merged.map(_.score).toSeq)
  }

  /** Merging with one empty input is a noop modulo trim to K. */
  @Test def testMergeWithEmpty(): Unit = {
    val a = Array(ref(1, 1f), ref(2, 2f), ref(3, 3f))
    val merged = TopKHeap.merge(a, Array.empty[ScoredRowRef], k = 2, smallerIsBetter = true)
    assertEquals(Seq(1f, 2f), merged.map(_.score).toSeq)
  }
}
