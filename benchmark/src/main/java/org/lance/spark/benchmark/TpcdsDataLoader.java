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

import org.apache.spark.sql.SparkSession;

/**
 * Registers pre-generated TPC-DS tables as Spark temp views for querying.
 *
 * <p>Tables must be generated beforehand using {@link TpcdsDataGenerator}.
 */
public class TpcdsDataLoader {

  private final SparkSession spark;
  private final String dataDir;

  public TpcdsDataLoader(SparkSession spark, String dataDir) {
    this.spark = spark;
    this.dataDir = dataDir;
  }

  /**
   * Registers all TPC-DS tables for the given format as temp views.
   *
   * @param format the storage format (e.g. "lance", "parquet")
   */
  public void registerTables(String format) {
    String formatDir = dataDir + "/" + format;
    boolean isLance = "lance".equalsIgnoreCase(format);
    String readFormat = isLance ? "lance" : format;

    int registered = 0;
    for (String tableName : TpcdsDataGenerator.TPCDS_TABLES) {
      String tablePath = formatDir + "/" + tableName;
      if (isLance) {
        tablePath = TpcdsDataGenerator.toLancePath(tablePath) + ".lance";
      }

      try {
        spark.read().format(readFormat).load(tablePath).createOrReplaceTempView(tableName);
        registered++;
      } catch (Exception e) {
        System.out.println("  SKIP " + tableName + " (not found at " + tablePath + ")");
        System.out.flush();
      }
    }

    System.out.println(
        "Registered " + registered + "/" + TpcdsDataGenerator.TPCDS_TABLES.size()
            + " tables for format: " + format);
    System.out.flush();
  }

  /**
   * Drops all TPC-DS temp views (used between format runs).
   */
  public void unregisterTables() {
    for (String tableName : TpcdsDataGenerator.TPCDS_TABLES) {
      spark.catalog().dropTempView(tableName);
    }
  }
}
