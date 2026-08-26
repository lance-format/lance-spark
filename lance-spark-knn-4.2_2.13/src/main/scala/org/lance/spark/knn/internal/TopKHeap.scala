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

import scala.collection.mutable

/**
 * Bounded top-K heap with metric-aware ordering. Used by the probe stage for map-side combine
 * across fragments owned by a single task — keeps the per-left-row state at exactly K entries no
 * matter how many fragments contribute, and again on the reduce side to merge contributions from
 * different tasks for the same `leftId`.
 *
 * Semantics:
 *  - `smallerIsBetter = true`  (distance, e.g. L2): retain the K smallest-score entries.
 *  - `smallerIsBetter = false` (similarity, e.g. cosine): retain the K largest-score entries.
 *
 * Internally, the heap's *head* holds the worst surviving element so eviction is O(log K). Scala's
 * `mutable.PriorityQueue` is a max-heap by the supplied `Ordering`, so the ordering is chosen to
 * place "worst surviving" at the top:
 *  - distance     → max-heap on `score`            (largest score is worst)
 *  - similarity   → max-heap on `-score`           (smallest score is worst)
 *
 * Not thread-safe. Each left row in a probe stage gets its own heap.
 */
final class TopKHeap(k: Int, smallerIsBetter: Boolean) {
  require(k > 0, "k must be positive")

  private val ord: Ordering[ScoredRowRef] =
    if (smallerIsBetter) Ordering.by[ScoredRowRef, Float](_.score)
    else Ordering.by[ScoredRowRef, Float](-_.score)

  private val heap = new mutable.PriorityQueue[ScoredRowRef]()(ord)

  /**
   * Insert `ref` if it would survive the top-K cut. Either grows the heap up to K or evicts the
   * current worst-surviving element if `ref` is strictly better than it.
   */
  def offer(ref: ScoredRowRef): Unit = {
    if (heap.size < k) {
      heap.enqueue(ref)
    } else {
      val worst = heap.head
      val isBetter =
        if (smallerIsBetter) ref.score < worst.score
        else ref.score > worst.score
      if (isBetter) {
        heap.dequeue()
        heap.enqueue(ref)
      }
    }
  }

  def offerAll(refs: TraversableOnce[ScoredRowRef]): Unit = refs.foreach(offer)

  /**
   * Drain the heap into a best-first sorted Array. After this call the heap is empty. Best-first
   * means index 0 is the top-ranked entry (smallest score for distance, largest for similarity).
   */
  def drain(): Array[ScoredRowRef] = {
    val out = new Array[ScoredRowRef](heap.size)
    var i = heap.size - 1
    // PriorityQueue.dequeue returns the worst surviving element first; walking the array in
    // reverse places best at index 0.
    while (i >= 0) {
      out(i) = heap.dequeue()
      i -= 1
    }
    out
  }

  def size: Int = heap.size
  def isEmpty: Boolean = heap.isEmpty
}

object TopKHeap {

  /**
   * Convenience: merge several already-sorted (best-first) ref arrays into one top-K array. Used
   * by the merge stage as the `reduceByKey` combine function.
   */
  def merge(
      a: Array[ScoredRowRef],
      b: Array[ScoredRowRef],
      k: Int,
      smallerIsBetter: Boolean): Array[ScoredRowRef] = {
    if (a.isEmpty) return takeBest(b, k, smallerIsBetter)
    if (b.isEmpty) return takeBest(a, k, smallerIsBetter)
    val heap = new TopKHeap(k, smallerIsBetter)
    heap.offerAll(a)
    heap.offerAll(b)
    heap.drain()
  }

  private def takeBest(
      arr: Array[ScoredRowRef],
      k: Int,
      smallerIsBetter: Boolean): Array[ScoredRowRef] = {
    if (arr.length <= k) return arr
    val heap = new TopKHeap(k, smallerIsBetter)
    heap.offerAll(arr)
    heap.drain()
  }
}
