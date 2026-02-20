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
package org.lance.spark.update;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseVacuumTest {
  protected String catalogName = "lance_test";
  protected String tableName = "vacuum_test";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-vacuum-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  protected void prepareDataset() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  /**
   * Append rows to the table to produce a new dataset version. The id is offset by offsetBase to
   * avoid duplicates.
   */
  private void appendRows(int count, int offsetBase) {
    String values =
        IntStream.range(0, count)
            .boxed()
            .map(i -> String.format("(%d, 'text_%d')", i + offsetBase, i + offsetBase))
            .collect(Collectors.joining(","));
    spark.sql(String.format("insert into %s (id, text) values %s ;", fullTable, values));
  }

  @Test
  public void testVacuumWithoutArgs() {
    prepareDataset();

    Dataset<Row> result = spark.sql(String.format("vacuum %s", fullTable));

    Row row = result.collectAsList().get(0);
    Assertions.assertTrue(row.getLong(0) >= 0);
    Assertions.assertTrue(row.getLong(1) >= 0);
  }

  @Test
  public void testVacuumRemovesOldVersionsByVersionThreshold() {
    // Prepare initial dataset
    prepareDataset();
    // Append multiple times to generate multiple versions
    for (int round = 1; round <= 4; round++) {
      appendRows(10, round * 100);
    }

    Dataset<Row> result =
        spark.sql(String.format("vacuum %s with (before_version=1000000)", fullTable));

    Row row = result.collectAsList().get(0);
    long bytesRemoved = row.getLong(0);
    long oldVersions = row.getLong(1);
    Assertions.assertTrue(bytesRemoved > 0, "bytes_removed should be > 0");
    Assertions.assertTrue(oldVersions >= 1, "old_versions should be >= 1");
  }

  @Test
  public void testVacuumRemovesOldVersionsByTimestampThreshold() throws InterruptedException {
    // Prepare initial dataset, which becomes the 'old' version after timestamp
    prepareDataset();

    // Ensure a measurable delay to separate old/new versions
    Thread.sleep(50);
    long beforeTs = System.currentTimeMillis();

    // Append multiple times to generate new versions after beforeTs
    for (int round = 1; round <= 3; round++) {
      appendRows(10, round * 200);
    }

    Dataset<Row> result =
        spark.sql(
            String.format("vacuum %s with (before_timestamp_millis=%d)", fullTable, beforeTs));

    Row row = result.collectAsList().get(0);
    long bytesRemoved = row.getLong(0);
    long oldVersions = row.getLong(1);
    Assertions.assertTrue(bytesRemoved > 0, "bytes_removed should be > 0");
    Assertions.assertTrue(oldVersions >= 1, "old_versions should be >= 1");
  }

  @Test
  public void testVacuumAfterOptimizeRemovesBytes() {
    prepareDataset();
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));

    Dataset<Row> optimizeResult =
        spark.sql(String.format("optimize %s with (target_rows_per_fragment=20000)", fullTable));

    Dataset<Row> vacuumResult =
        spark.sql(String.format("vacuum %s with (before_version=1000000)", fullTable));

    Row row = vacuumResult.collectAsList().get(0);
    Assertions.assertTrue(row.getLong(0) > 0);
    Assertions.assertTrue(row.getLong(1) >= 0);
  }
}
