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
 * <p>Both names are reserved by Spark: {@code
 * org.apache.spark.sql.execution.metric.CustomMetrics#updateMetrics} recognizes exactly {@code
 * bytesWritten} and {@code recordsWritten} and forwards them to {@code
 * TaskContext.get().taskMetrics().outputMetrics()}, which is what populates the stage-level {@code
 * outputBytes}/{@code outputRecords} shown in the Stages UI and the history server REST API. Any
 * other name would reach only the SQL tab. That routing happens for any reported task metric with a
 * reserved name, whether or not it is advertised by {@link #allMetrics()}.
 *
 * <p>Only {@code recordsWritten} is advertised as a SQL custom metric. A SQL metric can only be set
 * from {@code DataWriter.currentMetricsValues()}, which Spark stops polling before {@code
 * DataWriter.commit()}, and no driver-side path updates it afterwards. Rows are counted in {@code
 * write()} so {@code recordsWritten} is final by the last poll; a fragment's byte size is not known
 * until its creation task resolves, which for the last fragment happens inside {@code commit()}, so
 * an advertised {@code bytesWritten} would render as 0 on a single-fragment write. It is reported
 * as a task metric only.
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
