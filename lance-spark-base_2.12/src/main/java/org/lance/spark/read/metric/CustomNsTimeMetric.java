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
package org.lance.spark.read.metric;

import org.apache.spark.sql.connector.metric.CustomSumMetric;

/**
 * Base class for custom metrics whose task values are nanoseconds. Aggregation behaves like {@link
 * CustomSumMetric} (sum of task values), but {@link #aggregateTaskMetrics(long[])} returns a
 * human-readable duration string ("1.2 s", "350 ms", "47 us") instead of a raw long, so the Spark
 * UI displays a usable value. Spark's {@code SQLMetrics.stringValue} formatter is {@code
 * private[sql]}, so we reimplement it here.
 */
public abstract class CustomNsTimeMetric extends CustomSumMetric {
  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    long total = 0;
    for (long v : taskMetrics) {
      total += v;
    }
    return formatDurationNs(total);
  }

  /** Visible for testing. Formats nanoseconds as "X ns" / "X us" / "X ms" / "X.Y s". */
  static String formatDurationNs(long ns) {
    if (ns < 0) {
      ns = 0;
    }
    if (ns < 1_000L) {
      return ns + " ns";
    }
    if (ns < 1_000_000L) {
      return (ns / 1_000L) + " us";
    }
    if (ns < 1_000_000_000L) {
      return (ns / 1_000_000L) + " ms";
    }
    long secWhole = ns / 1_000_000_000L;
    long secTenths = (ns % 1_000_000_000L) / 100_000_000L;
    return secWhole + "." + secTenths + " s";
  }
}
