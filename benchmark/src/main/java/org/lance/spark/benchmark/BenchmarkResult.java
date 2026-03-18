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
package org.lance.spark.benchmark;

public class BenchmarkResult {

  private final String queryName;
  private final String format;
  private final int iteration;
  private final long elapsedMs;
  private final long rowCount;
  private final boolean success;
  private final String errorMessage;
  private final QueryMetrics metrics;

  private BenchmarkResult(
      String queryName,
      String format,
      int iteration,
      long elapsedMs,
      long rowCount,
      boolean success,
      String errorMessage,
      QueryMetrics metrics) {
    this.queryName = queryName;
    this.format = format;
    this.iteration = iteration;
    this.elapsedMs = elapsedMs;
    this.rowCount = rowCount;
    this.success = success;
    this.errorMessage = errorMessage;
    this.metrics = metrics;
  }

  public static BenchmarkResult success(
      String queryName, String format, int iteration, long elapsedMs, long rowCount) {
    return new BenchmarkResult(queryName, format, iteration, elapsedMs, rowCount, true, null, null);
  }

  public static BenchmarkResult success(
      String queryName,
      String format,
      int iteration,
      long elapsedMs,
      long rowCount,
      QueryMetrics metrics) {
    return new BenchmarkResult(
        queryName, format, iteration, elapsedMs, rowCount, true, null, metrics);
  }

  public static BenchmarkResult failure(
      String queryName, String format, int iteration, long elapsedMs, String errorMessage) {
    return new BenchmarkResult(
        queryName, format, iteration, elapsedMs, -1, false, errorMessage, null);
  }

  public String getQueryName() {
    return queryName;
  }

  public String getFormat() {
    return format;
  }

  public int getIteration() {
    return iteration;
  }

  public long getElapsedMs() {
    return elapsedMs;
  }

  public long getRowCount() {
    return rowCount;
  }

  public boolean isSuccess() {
    return success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public QueryMetrics getMetrics() {
    return metrics;
  }

  public String toCsvLine() {
    String base =
        String.join(
            ",",
            queryName,
            format,
            String.valueOf(iteration),
            String.valueOf(elapsedMs),
            String.valueOf(rowCount),
            String.valueOf(success),
            errorMessage == null ? "" : "\"" + errorMessage.replace("\"", "\"\"") + "\"");
    if (metrics != null) {
      base += "," + metrics.toCsvFragment();
    }
    return base;
  }

  public static String csvHeader() {
    return "query,format,iteration,elapsed_ms,row_count,success,error";
  }

  public static String csvHeaderWithMetrics() {
    return csvHeader() + "," + QueryMetrics.csvHeaderFragment();
  }
}
