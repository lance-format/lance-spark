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

/**
 * A reference to a single right-side row produced by a vector index probe, along with the ranking
 * score. Carries no payload — payloads are fetched in the materialize stage by row address. Pairing
 * a tiny ref with a score is the unit of work passing through the shuffle and is what keeps the
 * shuffle volume to `O(|L| × tasks × K × ~24B)` instead of `O(|L| × tasks × K × payload_bytes)`.
 *
 * @param rowAddr Lance row address (`_rowaddr`): packed `(frag_id << 32) | row_in_frag`. Stable
 *               within a Lance dataset version.
 * @param score   Distance or similarity returned by Lance's vector search. Smaller-is-better for
 *               distance metrics (L2), larger-is-better for similarity metrics (cosine/dot).
 *               Direction is carried out-of-band in the operator config; this struct stays metric-
 *               agnostic.
 */
final case class ScoredRowRef(rowAddr: Long, score: Float)

object ScoredRowRef {

  /** Order best-first for distance metrics (smallest score wins). */
  val distanceOrdering: Ordering[ScoredRowRef] =
    Ordering.by[ScoredRowRef, Float](_.score)

  /** Order best-first for similarity metrics (largest score wins). */
  val similarityOrdering: Ordering[ScoredRowRef] =
    Ordering.by[ScoredRowRef, Float](-_.score)
}

/**
 * A single probe hit WITH its materialized payload — the unit produced by the no-overfetch fast path
 * ([[LanceProbe.probeRows]]), which folds the nearest search and the payload projection into one
 * scan. Unlike [[ScoredRowRef]] (a payload-free ref bound for a later materialize point-fetch), this
 * already carries the right row.
 *
 * @param rowAddr Lance row id of the hit (kept for de-duplication / re-keying by the join stage).
 * @param score   Distance or similarity from Lance's vector search; direction is carried out-of-band
 *                in the operator config, so this stays metric-agnostic (see [[ScoredRowRef.score]]).
 * @param row     Materialized right-side payload keyed by column name, each value already the Spark
 *                EXTERNAL representation its target type expects (the join's `ExpressionEncoder`
 *                accepts it directly).
 */
final case class MaterializedHit(rowAddr: Long, score: Float, row: Map[String, Any])
