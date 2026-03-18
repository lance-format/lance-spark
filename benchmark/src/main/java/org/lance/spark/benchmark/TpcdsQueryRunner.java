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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TpcdsQueryRunner {

  private final SparkSession spark;
  private final int iterations;

  public TpcdsQueryRunner(SparkSession spark, int iterations) {
    this.spark = spark;
    this.iterations = iterations;
  }

  public List<BenchmarkResult> runAllQueries(String format) {
    List<BenchmarkResult> results = new ArrayList<>();
    List<String> queryNames = getAvailableQueries();

    System.out.println(
        "Running " + queryNames.size() + " queries x " + iterations + " iterations for " + format);
    System.out.flush();

    for (String queryName : queryNames) {
      String sql = loadQuery(queryName);
      if (sql == null) {
        continue;
      }

      for (int i = 1; i <= iterations; i++) {
        BenchmarkResult result = runQuery(queryName, format, sql, i);
        results.add(result);

        String status = result.isSuccess() ? "OK" : "FAIL";
        System.out.printf(
            "  [%s] %s iter=%d time=%dms rows=%d%n",
            status, queryName, i, result.getElapsedMs(), result.getRowCount());
        if (!result.isSuccess()) {
          System.out.println("        Error: " + result.getErrorMessage());
        }
        System.out.flush();
      }
    }

    return results;
  }

  private BenchmarkResult runQuery(String queryName, String format, String sql, int iteration) {
    long start = System.currentTimeMillis();
    try {
      // Split on semicolons to handle multi-statement queries; execute each and
      // keep the last result for row count.
      String[] statements = sql.split(";");
      long rowCount = 0;
      for (String stmt : statements) {
        String trimmed = stmt.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        Dataset<Row> result = spark.sql(trimmed);
        rowCount = result.count();
      }
      long elapsed = System.currentTimeMillis() - start;
      return BenchmarkResult.success(queryName, format, iteration, elapsed, rowCount);
    } catch (Exception e) {
      long elapsed = System.currentTimeMillis() - start;
      String msg = e.getMessage();
      if (msg != null && msg.length() > 200) {
        msg = msg.substring(0, 200) + "...";
      }
      return BenchmarkResult.failure(queryName, format, iteration, elapsed, msg);
    }
  }

  List<String> getAvailableQueries() {
    List<String> queries = new ArrayList<>();
    for (int i = 1; i <= 99; i++) {
      String name = "q" + i;
      String resourcePath = "/tpcds-queries/" + name + ".sql";
      if (getClass().getResourceAsStream(resourcePath) != null) {
        queries.add(name);
      }
      // Check for a/b variants (e.g., q14a, q14b)
      for (String suffix : new String[] {"a", "b"}) {
        String variantName = "q" + i + suffix;
        String variantPath = "/tpcds-queries/" + variantName + ".sql";
        if (getClass().getResourceAsStream(variantPath) != null) {
          queries.add(variantName);
        }
      }
    }
    return queries;
  }

  private String loadQuery(String queryName) {
    String resourcePath = "/tpcds-queries/" + queryName + ".sql";
    try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
      if (is == null) {
        return null;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (Exception e) {
      System.err.println("Failed to load query " + queryName + ": " + e.getMessage());
      return null;
    }
  }
}
