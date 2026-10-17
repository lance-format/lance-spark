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
package org.lance.spark.knn.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.lance.spark.knn.catalyst.{IndexedNearestByJoinRule, LanceKnnJoinStrategy}

/**
 * Registers the Catalyst integration for the indexed nearest-by join (the SQL
 * `APPROX NEAREST K BY DISTANCE ...` syntax added in Spark 4.2 by SPARK-56395).
 *
 * Wire this into a SparkSession with:
 *
 * {{{
 *   SparkSession.builder()
 *     .config("spark.sql.extensions",
 *             "org.lance.spark.knn.extensions.LanceKnnSparkSessionExtensions")
 *     .config("spark.lance.knn.indexedNearestByJoin.enabled", "true")
 *     ...
 * }}}
 *
 * The `enabled` flag gates the rule itself — see [[IndexedNearestByJoinRule.EnabledConfKey]]. Off
 * by default to keep the integration opt-in.
 *
 * == Injection point: postHocResolutionRule, NOT optimizerRule ==
 *
 * Spark's `RewriteNearestByJoin` runs in `FinishAnalysis`, which precedes the
 * `operatorOptimizationBatch` that `injectOptimizerRule` adds rules to. By the time an injected
 * optimizer rule fires, the `NearestByJoin` operator has already been replaced with the
 * cross-product + `MaxMinByK` rewrite. `injectPostHocResolutionRule` runs after analysis but
 * before any optimizer batch — this is the only injection point that sees the unrewritten
 * `NearestByJoin`. See [[IndexedNearestByJoinRule]]'s class doc for the full rationale.
 *
 * Coexistence: this extension does not replace `LanceSparkSessionExtensions` from the connector
 * modules; both can be wired together in a comma-separated `spark.sql.extensions` value.
 */
class LanceKnnSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPostHocResolutionRule(_ => IndexedNearestByJoinRule)
    // Lowers the single `LanceKnnJoinLogicalPlan` the rule emits to `LanceKnnJoinExec` — the
    // no-shuffle `LanceKnnJoinStage.runPartition` per-partition probe.
    extensions.injectPlannerStrategy(_ => LanceKnnJoinStrategy)
  }
}
