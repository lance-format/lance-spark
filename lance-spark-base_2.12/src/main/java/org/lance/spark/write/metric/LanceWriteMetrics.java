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

/** Custom metrics advertised on the Spark SQL write node. */
public final class LanceWriteMetrics {
  public static final String BYTES_WRITTEN = "bytesWritten";

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
