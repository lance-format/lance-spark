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

import org.lance.index.DistanceType

/**
 * Vector distance / similarity metric. Mirrors `org.lance.index.DistanceType` but exposed as a
 * Scala enumeration so callers don't have to import Lance internals. Each metric fixes the
 * "best-first" direction used during merge.
 *
 * Lance's vector search returns a DISTANCE for every metric — including the similarity-flavored
 * ones, which it reports as `1 - cosine_similarity` and `1 - dot_product`. So smaller is better for
 * ALL three metrics; there is no larger-is-better case:
 *
 *  - L2:     smaller score is better (squared L2 distance)
 *  - Cosine: smaller score is better (`1 - cosine_similarity`)
 *  - Dot:    smaller score is better (`1 - dot_product`)
 */
sealed trait Metric {

  /** The Lance distance type used when configuring a `Query`. */
  def lanceType: DistanceType

  /** True if smaller scores rank better (distance), false if larger (similarity). */
  def smallerIsBetter: Boolean
}

object Metric {

  case object L2 extends Metric {
    val lanceType: DistanceType = DistanceType.L2
    val smallerIsBetter: Boolean = true
  }

  case object Cosine extends Metric {
    val lanceType: DistanceType = DistanceType.Cosine
    // Lance returns `1 - cosine_similarity`, a distance: smaller is better.
    val smallerIsBetter: Boolean = true
  }

  case object Dot extends Metric {
    val lanceType: DistanceType = DistanceType.Dot
    // Lance returns `1 - dot_product`, a distance: smaller is better.
    val smallerIsBetter: Boolean = true
  }

  /**
   * Parse a metric name. Accepts the same set of names Lance accepts plus a few synonyms commonly
   * used in Spark vector functions:
   *
   *  - "l2" | "euclidean"        → L2
   *  - "cosine"                  → Cosine
   *  - "dot" | "inner" | "ip"    → Dot
   */
  def fromName(name: String): Metric = name.trim.toLowerCase match {
    case "l2" | "euclidean" => L2
    case "cosine" => Cosine
    case "dot" | "inner" | "ip" => Dot
    case other =>
      throw new IllegalArgumentException(
        s"Unknown metric '$other'. Expected one of: l2, cosine, dot.")
  }
}
