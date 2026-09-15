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
package org.lance.spark.write.metric;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;

/**
 * Custom metrics for the Lance write path, displayed on the Spark UI write node.
 *
 * <p>{@code bytesWritten} and {@code recordsWritten} are reserved names: {@code
 * execution.metric.CustomMetrics#updateMetrics} recognizes exactly these two and forwards them to
 * the task's output metrics, which is what populates stage-level {@code outputBytes}/{@code
 * outputRecords}. That happens for any reported task metric with a reserved name, advertised here
 * or not.
 *
 * <p>Only {@code recordsWritten} is advertised as a SQL metric. SQL metrics are set from {@code
 * currentMetricsValues()}, which Spark stops polling before {@code commit()}, and the last
 * fragment's byte size is not known until then, so an advertised {@code bytesWritten} would read 0
 * on a single-fragment write. {@link LanceWriteMetricsTracker#publishOutputMetrics()} reports it
 * after commit instead.
 */
public final class LanceWriteMetrics {
  /** Reserved Spark metric name, routed to {@code OutputMetrics.setBytesWritten}. */
  public static final String BYTES_WRITTEN = "bytesWritten";

  /** Reserved Spark metric name, routed to {@code OutputMetrics.setRecordsWritten}. */
  public static final String RECORDS_WRITTEN = "recordsWritten";

  private LanceWriteMetrics() {}

  // Each inner class MUST have a public no-arg constructor (Spark instantiates via reflection).

  public static class RecordsWrittenMetric extends CustomSumMetric {
    @Override
    public String name() {
      return RECORDS_WRITTEN;
    }

    @Override
    public String description() {
      return "number of rows written";
    }
  }

  private static final CustomMetric[] ALL_METRICS = {
    new RecordsWrittenMetric(),
  };

  /** Returns all supported custom metrics, used by SparkWrite.supportedCustomMetrics(). */
  public static CustomMetric[] allMetrics() {
    return ALL_METRICS.clone();
  }
}
