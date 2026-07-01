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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}

/**
 * Reflective bridge to Spark's internal `CommandUtils.computeColumnStats`, which computes
 * min / max / nullCount / (HLL-approximate) distinctCount / avgLen / maxLen for the given
 * attributes in a single aggregation job — the exact computation Spark's own
 * `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` runs. Reusing it (instead of a hand-rolled
 * `df.agg(...)`) guarantees the produced `ColumnStat` values are in the internal catalyst
 * representation that `CatalogColumnStat.toMap` expects and that Spark's CBO consumes.
 *
 * <p>Histogram computation is force-disabled for the duration of the call (see below): Lance
 * ANALYZE never persists histograms, so spending a percentile-sketch pass to build one we would
 * immediately discard is pure waste.
 *
 * <p>Reflection is required: `computeColumnStats`'s session parameter is
 * `org.apache.spark.sql.SparkSession` in Spark 3.x but `org.apache.spark.sql.classic.SparkSession`
 * in Spark 4.x (the Connect/Classic split). The shared `lance-spark-base_2.12` source compiles
 * standalone and against every supported Spark version, so it cannot name the 4.x `classic` type
 * to call the method statically. The lookup is by name + arity and fails loudly if Spark's
 * internal signature ever changes, so a Spark upgrade that breaks it surfaces immediately rather
 * than silently misbehaving.
 */
object LanceNativeColumnStats {

  /**
   * @return (rowCount, perColumnStats) — distinctCount is HLL-approximate, matching Spark's
   *         native ANALYZE; avgLen/maxLen are always populated. No histogram is ever produced:
   *         `spark.sql.statistics.histogram.enabled` is forced off for the duration of the call.
   */
  def computeColumnStats(
      spark: SparkSession,
      relation: LogicalPlan,
      attributes: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    // CommandUtils.computeColumnStats reads sparkSession.sessionState.conf.histogramEnabled to
    // decide whether to add the percentile-sketch aggregation that builds a histogram. Lance
    // ANALYZE never persists histograms (the read path discards them — the V2 ColumnStatistics
    // histogram type differs from catalyst's), so force the flag off here rather than computing a
    // histogram and stripping it from the serialized output afterward. Save/restore so we don't
    // disturb the user's session setting for unrelated queries.
    val histogramKey = "spark.sql.statistics.histogram.enabled"
    val runtimeConf = spark.conf
    val previousHistogram: Option[String] =
      if (runtimeConf.contains(histogramKey)) Some(runtimeConf.get(histogramKey)) else None
    runtimeConf.set(histogramKey, "false")
    try {
      val module =
        Class
          .forName("org.apache.spark.sql.execution.command.CommandUtils$")
          .getField("MODULE$")
          .get(null)
      val method = module.getClass.getMethods
        .find(m => m.getName == "computeColumnStats" && m.getParameterCount == 3)
        .getOrElse(
          throw new IllegalStateException(
            "Spark CommandUtils.computeColumnStats(session, relation, Seq[Attribute]) was not "
              + "found via reflection. A Spark upgrade likely changed this internal API; update "
              + "LanceNativeColumnStats to match."))
      val raw =
        try {
          method.invoke(module, spark, relation, attributes)
        } catch {
          case e: java.lang.reflect.InvocationTargetException if e.getCause != null =>
            throw e.getCause
        }
      val tuple = raw.asInstanceOf[scala.Tuple2[Any, Map[Attribute, ColumnStat]]]
      val rowCount = tuple._1 match {
        case n: java.lang.Number => n.longValue()
        case b: scala.math.BigInt => b.toLong
        case other => other.toString.toLong
      }
      (rowCount, tuple._2)
    } finally {
      previousHistogram match {
        case Some(v) => runtimeConf.set(histogramKey, v)
        case None => runtimeConf.unset(histogramKey)
      }
    }
  }
}
