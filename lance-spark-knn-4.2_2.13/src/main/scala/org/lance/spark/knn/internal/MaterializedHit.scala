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
 * A single probe hit WITH its materialized payload — the unit produced by [[LanceProbe.probeRows]],
 * which folds the nearest search and the payload projection into one scan. Each hit already carries
 * its right-side row, so the join stage assembles output rows without a second point-fetch.
 *
 * @param rowAddr Lance row id of the hit (kept for de-duplication / re-keying by the join stage).
 * @param score   Distance or similarity from Lance's vector search. Smaller-is-better for distance
 *                metrics (L2), larger-is-better for similarity metrics (cosine/dot); the direction is
 *                carried out-of-band in the operator config, so this stays metric-agnostic.
 * @param row     Materialized right-side payload keyed by column name, each value already the Spark
 *                EXTERNAL representation its target type expects (the join's `ExpressionEncoder`
 *                accepts it directly).
 */
final case class MaterializedHit(rowAddr: Long, score: Float, row: Map[String, Any])
