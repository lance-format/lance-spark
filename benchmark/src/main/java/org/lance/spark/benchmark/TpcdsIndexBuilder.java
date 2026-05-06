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

import org.apache.spark.sql.SparkSession;

/**
 * Spark job that creates btree indexes on TPC-DS dimension tables to enable
 * zonemap-based fragment pruning during joins.
 *
 * <p>The indexes cover columns that appear in filter predicates across TPC-DS
 * queries. Each btree index carries zonemap metadata that the Lance scan
 * builder uses for fragment pruning and storage-partitioned joins (SPJ).
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.TpcdsIndexBuilder \
 *     benchmark.jar \
 *     --data-dir /path/to/tpcds/data
 * </pre>
 */
public class TpcdsIndexBuilder {

  /** Table names in priority order (highest-impact tables first). */
  private static final String[] TABLE_NAMES = {
      "date_dim", "item", "customer_demographics", "store", "promotion",
      "customer_address", "household_demographics"
  };

  public static void main(String[] args) throws Exception {
    String dataDir = null;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--data-dir":
          dataDir = args[++i];
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (dataDir == null) {
      System.err.println("Missing required argument: --data-dir");
      printUsage();
      System.exit(1);
    }

    String lanceRoot = dataDir + "/lance";

    SparkSession spark =
        SparkSession.builder().appName("TPC-DS Index").getOrCreate();

    // Ensure lance_default catalog is registered for path-based table loading
    if (!spark.conf().contains("spark.sql.catalog.lance_default")) {
      spark.conf().set("spark.sql.catalog.lance_default",
          "org.lance.spark.LanceNamespaceSparkCatalog");
    }

    try {
      List<String> statements = loadIndexStatements(lanceRoot);

      System.out.println("=== TPC-DS Index Builder ===");
      System.out.println("Data dir:    " + dataDir);
      System.out.println("Lance root:  " + lanceRoot);
      System.out.println("Indexes:     " + statements.size());
      System.out.println();
      System.out.flush();

      int succeeded = 0;
      int failed = 0;

      for (int i = 0; i < statements.size(); i++) {
        String sql = statements.get(i);
        System.out.print("[" + (i + 1) + "/" + statements.size() + "] "
            + sql + " ...");
        System.out.flush();

        long start = System.currentTimeMillis();
        try {
          spark.sql(sql);
          long elapsed = System.currentTimeMillis() - start;
          System.out.println(" OK (" + elapsed + "ms)");
          succeeded++;
        } catch (Exception e) {
          long elapsed = System.currentTimeMillis() - start;
          System.out.println(" FAILED (" + elapsed + "ms)");
          System.out.println("  Error: " + e.getMessage());
          failed++;
        }
        System.out.flush();
      }

      System.out.println();
      System.out.println("=== Index creation complete ===");
      System.out.println("Succeeded: " + succeeded + ", Failed: " + failed);
      System.out.flush();
    } finally {
      spark.stop();
    }
  }

  private static List<String> loadIndexStatements(String lanceRoot) {
    List<String> statements = new ArrayList<>();
    for (String table : TABLE_NAMES) {
      String tablePath =
          TpcdsDataGenerator.toLancePath(lanceRoot + "/" + table) + ".lance";
      String resourcePath = "/index/" + table + ".sql";
      try (InputStream is =
          TpcdsIndexBuilder.class.getResourceAsStream(resourcePath)) {
        if (is == null) {
          throw new IllegalStateException(
              "Index resource not found: " + resourcePath);
        }
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(is, StandardCharsets.UTF_8))) {
          for (String statement : reader.lines().collect(Collectors.joining("\n")).split(";")) {
            String trimmed = statement.trim();
            if (!trimmed.isEmpty()) {
              trimmed = trimmed.replace(
                  "ALTER TABLE " + table,
                  "ALTER TABLE lance_default.`" + tablePath + "`");
              statements.add(trimmed);
            }
          }
        }
      } catch (IllegalStateException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to load index resource: " + resourcePath, e);
      }
    }
    return statements;
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpcdsIndexBuilder --data-dir <path>");
  }
}
