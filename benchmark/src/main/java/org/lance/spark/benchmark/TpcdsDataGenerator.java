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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Spark job that generates TPC-DS data using the Kyuubi TPC-DS connector
 * and writes each table directly into the target format(s) (Lance, Parquet, etc.).
 *
 * <p>The Kyuubi connector generates data in parallel across Spark executors —
 * no external {@code dsdgen} binary or intermediate CSV/dat files are needed.
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.TpcdsDataGenerator \
 *     benchmark.jar \
 *     --data-dir s3a://bucket/tpcds/sf10 \
 *     --scale-factor 10 \
 *     --formats parquet,lance
 * </pre>
 */
public class TpcdsDataGenerator {

  /** The 24 TPC-DS tables as named in the Kyuubi catalog. */
  static final List<String> TPCDS_TABLES =
      Arrays.asList(
          "call_center",
          "catalog_page",
          "catalog_returns",
          "catalog_sales",
          "customer",
          "customer_address",
          "customer_demographics",
          "date_dim",
          "household_demographics",
          "income_band",
          "inventory",
          "item",
          "promotion",
          "reason",
          "ship_mode",
          "store",
          "store_returns",
          "store_sales",
          "time_dim",
          "warehouse",
          "web_page",
          "web_returns",
          "web_sales",
          "web_site");

  public static void main(String[] args) throws Exception {
    String dataDir = null;
    int scaleFactor = 1;
    String formatsStr = "parquet,lance";
    boolean useDoubleForDecimal = false;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--data-dir":
          dataDir = args[++i];
          break;
        case "--scale-factor":
          scaleFactor = Integer.parseInt(args[++i]);
          break;
        case "--formats":
          formatsStr = args[++i];
          break;
        case "--use-double-for-decimal":
          useDoubleForDecimal = true;
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

    String[] formats = formatsStr.split(",");

    // Configure Kyuubi TPC-DS catalog
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("TPC-DS Data Generator (SF=" + scaleFactor + ")")
            .config(
                "spark.sql.catalog.tpcds",
                "org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog");

    if (useDoubleForDecimal) {
      builder.config("spark.sql.catalog.tpcds.useDoubleForDecimal", "true");
    }

    SparkSession spark = builder.getOrCreate();

    try {
      System.out.println("=== TPC-DS Data Generation ===");
      System.out.println("Scale factor:  " + scaleFactor);
      System.out.println("Formats:       " + formatsStr);
      System.out.println("Data dir:      " + dataDir);
      System.out.println();
      System.out.flush();

      String catalogDb = "tpcds.sf" + scaleFactor;

      for (String format : formats) {
        format = format.trim();
        System.out.println("--- Generating " + format + " tables ---");
        System.out.flush();

        for (String table : TPCDS_TABLES) {
          generateTable(spark, catalogDb, table, format, dataDir);
        }

        System.out.println();
      }

      System.out.println("=== Data generation complete ===");
      System.out.flush();

    } finally {
      spark.stop();
    }
  }

  private static void generateTable(
      SparkSession spark, String catalogDb, String table, String format, String dataDir) {

    boolean isLance = "lance".equalsIgnoreCase(format);
    String tablePath = dataDir + "/" + format + "/" + table;
    if (isLance) {
      tablePath = toLancePath(tablePath) + ".lance";
    }

    // Check if table already exists using Hadoop filesystem API (avoids noisy Spark warnings)
    try {
      Path hadoopPath = new Path(dataDir + "/" + format + "/" + table + (isLance ? ".lance" : ""));
      FileSystem fs = hadoopPath.getFileSystem(
          spark.sparkContext().hadoopConfiguration());
      if (fs.exists(hadoopPath)) {
        System.out.println("  SKIP " + table + " (already exists at " + hadoopPath + ")");
        System.out.flush();
        return;
      }
    } catch (Exception e) {
      // Could not check — proceed with generation
    }

    System.out.print("  GENERATE " + table + "...");
    System.out.flush();
    long start = System.currentTimeMillis();

    // Read from Kyuubi TPC-DS catalog — data is generated in parallel
    Dataset<Row> df = spark.read().table(catalogDb + "." + table);

    String writeFormat = isLance ? "lance" : format;
    SaveMode mode = isLance ? SaveMode.ErrorIfExists : SaveMode.Overwrite;

    df.write().mode(mode).format(writeFormat).save(tablePath);

    long elapsed = System.currentTimeMillis() - start;
    long count = spark.read().format(writeFormat).load(tablePath).count();
    System.out.println(" " + count + " rows (" + elapsed + "ms)");
    System.out.flush();
  }

  /**
   * Converts Hadoop-style Azure paths to the scheme that Lance's native I/O understands.
   *
   * <p>Lance uses {@code az://} for Azure Blob Storage, while Hadoop/Spark uses
   * {@code abfss://}. The mapping is:
   *
   * <pre>
   *   abfss://container@account.dfs.core.windows.net/path
   *   →  az://container/path
   * </pre>
   *
   * <p>The storage account name must be provided separately via the
   * {@code AZURE_STORAGE_ACCOUNT_NAME} environment variable.
   *
   * <p>Non-Azure paths (local, s3://, gs://, etc.) are returned unchanged.
   */
  static String toLancePath(String path) {
    if (path == null) {
      return null;
    }
    // abfss://container@account.dfs.core.windows.net/path
    if (path.startsWith("abfss://") || path.startsWith("abfs://")) {
      String withoutScheme = path.substring(path.indexOf("://") + 3);
      int atIdx = withoutScheme.indexOf('@');
      if (atIdx < 0) {
        return path;
      }
      String container = withoutScheme.substring(0, atIdx);
      String rest = withoutScheme.substring(atIdx + 1);
      // rest = account.dfs.core.windows.net/path
      int slashIdx = rest.indexOf('/');
      String objectPath = slashIdx >= 0 ? rest.substring(slashIdx + 1) : "";
      // az://container/path — account resolved from AZURE_STORAGE_ACCOUNT_NAME env var
      String suffix = objectPath.isEmpty() ? "" : "/" + objectPath;
      return "az://" + container + suffix;
    }
    return path;
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpcdsDataGenerator"
            + " --data-dir <path>"
            + " [--scale-factor 1]"
            + " [--formats parquet,lance]"
            + " [--use-double-for-decimal]");
  }
}
