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

import org.lance.index.IndexType

/**
 * Fully resolved, validated vector index parameters.
 *
 * Plain Scala case classes — Serializable by virtue of `Product with Serializable`.
 * Holds only primitive types, Option, and the IndexType enum (Java enums are
 * Serializable). Does NOT hold lance-core BuildParams classes, because those are
 * not guaranteed Serializable and we want clean task closures.
 *
 * `distanceTypeName` is the canonical Lance distance type name ("L2", "Cosine",
 * "Dot", "Hamming"); we ship the string and rebuild [[org.lance.index.DistanceType]]
 * on the executor with `DistanceType.valueOf(name)`.
 */
case class VectorIndexPlan(
    indexType: IndexType,
    distanceTypeName: String,
    ivf: IvfPlan,
    pq: Option[PqPlan],
    sq: Option[SqPlan],
    hnsw: Option[HnswPlan]) {

  /** True when this plan needs PQ codebook training. */
  def requiresPq: Boolean = pq.isDefined

  /** True when this plan involves HNSW graph build. */
  def requiresHnsw: Boolean = hnsw.isDefined
}

case class IvfPlan(numPartitions: Int, sampleRate: Int, maxIters: Int)

case class PqPlan(numSubVectors: Int, numBits: Int, sampleRate: Int, maxIters: Int)

case class SqPlan(numBits: Int, sampleRate: Int)

case class HnswPlan(m: Int, efConstruction: Int, maxLevel: Int, prefetchDistance: Option[Int])
