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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;

/**
 * Runs TPC-DS queries against pre-generated tables and reports results.
 *
 * <p>Tables must already exist under {@code <data-dir>/<format>/} — use
 * {@link TpcdsDataGenerator} to create them first.
 */
public class TpcdsBenchmarkRunner {

  public static void main(String[] args) throws Exception {
    String dataDir = null;
    String resultsDir = null;
    String formatsStr = "lance,parquet";
    int iterations = 3;
    boolean explain = false;
    boolean metrics = false;
    String queries = null;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--data-dir":
          dataDir = args[++i];
          break;
        case "--results-dir":
          resultsDir = args[++i];
          break;
        case "--formats":
          formatsStr = args[++i];
          break;
        case "--iterations":
          iterations = Integer.parseInt(args[++i]);
          break;
        case "--explain":
          explain = true;
          break;
        case "--metrics":
          metrics = true;
          break;
        case "--queries":
          queries = args[++i];
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (dataDir == null || resultsDir == null) {
      System.err.println("Missing required arguments.");
      printUsage();
      System.exit(1);
    }

    String[] formats = formatsStr.split(",");

    SparkSession spark =
        SparkSession.builder().appName("TPC-DS Benchmark").getOrCreate();

    try {
      // Register metrics listener if requested
      QueryMetricsListener metricsListener = null;
      if (metrics) {
        metricsListener = new QueryMetricsListener();
        spark.sparkContext().addSparkListener(metricsListener);
      }

      TpcdsDataLoader loader = new TpcdsDataLoader(spark, dataDir);
      TpcdsQueryRunner runner =
          new TpcdsQueryRunner(spark, iterations, explain, metricsListener, queries);
      List<BenchmarkResult> allResults = new ArrayList<>();

      for (String format : formats) {
        format = format.trim();
        System.out.println();
        System.out.println("=== Format: " + format + " ===");
        System.out.flush();

        // Register pre-generated tables as temp views
        loader.registerTables(format);

        // Run queries
        List<BenchmarkResult> formatResults = runner.runAllQueries(format);
        allResults.addAll(formatResults);

        // Unregister tables for next format
        loader.unregisterTables();
      }

      // Report
      BenchmarkReporter reporter = new BenchmarkReporter(allResults);

      String timestamp =
          LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
      String csvPath = resultsDir + "/tpcds_" + timestamp + ".csv";
      reporter.writeCsv(csvPath);
      reporter.printSummary();

    } finally {
      spark.stop();
    }
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpcdsBenchmarkRunner"
            + " --data-dir <path>"
            + " --results-dir <path>"
            + " [--formats lance,parquet]"
            + " [--iterations 3]"
            + " [--explain]"
            + " [--metrics]"
            + " [--queries q1,q3,q14a]");
  }
}
