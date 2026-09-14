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
 * <p>Both names here are reserved by Spark: {@code
 * org.apache.spark.sql.execution.metric.CustomMetrics#updateMetrics} recognizes exactly {@code
 * bytesWritten} and {@code recordsWritten} and forwards them to {@code
 * TaskContext.get().taskMetrics().outputMetrics()}, which is what populates the stage-level {@code
 * outputBytes}/{@code outputRecords} reported by the Spark history server REST API and the Stages
 * UI. Any other metric name would only reach the SQL tab. Renaming these therefore silently breaks
 * external tooling — see {@link LanceWriteMetricsTracker}. That routing is independent of {@link
 * #allMetrics()}: it happens for any reported task metric with a reserved name, advertised or not.
 *
 * <p><b>Only {@code recordsWritten} is advertised as a SQL custom metric.</b> A SQL metric can only
 * ever be set from {@code DataWriter.currentMetricsValues()}, which Spark 3.4-4.1 stops calling
 * before {@code DataWriter.commit()}, and there is no driver-side path back: after {@code
 * BatchWrite.commit(WriterCommitMessage[])} the write exec only logs and records streaming commit
 * progress, never touching the SQL metrics. Rows are counted in {@code write()} so {@code
 * recordsWritten} is already final by the last poll, but a fragment's byte size is only known once
 * its creation task resolves — for the last fragment, inside {@code commit()} — so an advertised
 * {@code bytesWritten} would render as 0 on every single-fragment write and short by the final
 * fragment on sharded ones. It is reported as a task metric (for the output-metrics routing above)
 * but deliberately kept off the SQL tab rather than shown wrong.
 */
public final class LanceWriteMetrics {
  /**
   * Reserved Spark metric name, routed to {@code OutputMetrics.setBytesWritten}. Reported as a task
   * metric only; not advertised as a SQL custom metric (see the class javadoc).
   */
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
