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
package org.lance.spark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test verifying that writes with {@code lance.partition.columns} produce fragments
 * where data is clustered by the partition column. When the number of Spark write tasks exceeds the
 * number of distinct partition values, each fragment contains exactly one partition value.
 */
public abstract class BasePartitionedWriteTest {
  protected String catalogName = "lance_part_write_test";
  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-partitioned-write-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testWriteWithPartitionColumnClustersData() {
    String tableName = "part_write_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    // Create table with partition column property
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance "
                + "TBLPROPERTIES ('lance.partition.columns' = 'region')",
            fullTable));

    // Insert data with multiple regions in a single INSERT — the write distribution
    // should cluster rows by region so each fragment gets one region value.
    String[] regions = {"east", "west", "north", "south", "central"};
    StringBuilder values = new StringBuilder();
    for (int r = 0; r < regions.length; r++) {
      for (int i = 0; i < 10; i++) {
        if (values.length() > 0) {
          values.append(",");
        }
        int id = r * 10 + i;
        values.append(String.format("(%d, '%s', %f)", id, regions[r], id * 1.5));
      }
    }
    spark.sql(String.format("INSERT INTO %s (id, region, value) VALUES %s", fullTable, values));

    // Verify all data was written correctly
    Dataset<Row> result = spark.sql("SELECT * FROM " + fullTable);
    assertEquals(50, result.count());

    // Verify that data is clustered: within each fragment, there should be
    // at most one distinct region value. We check this by reading with _fragid
    // and verifying the count of distinct regions per fragment.
    Dataset<Row> fragRegions =
        spark.sql(
            String.format(
                "SELECT _fragid, COUNT(DISTINCT region) as distinct_regions "
                    + "FROM %s GROUP BY _fragid",
                fullTable));

    List<Row> rows = fragRegions.collectAsList();
    assertFalse(rows.isEmpty(), "Should have at least one fragment");

    for (Row row : rows) {
      long distinctRegions = row.getLong(1);
      assertEquals(
          1,
          distinctRegions,
          String.format(
              "Fragment %d has %d distinct regions; expected 1 for partition-clustered write",
              row.getInt(0), distinctRegions));
    }
  }

  @Test
  public void testWriteWithoutPartitionColumnNoDistribution() {
    String tableName = "no_part_write_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    // Create table WITHOUT partition column property
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance", fullTable));

    // Insert data
    String values =
        IntStream.range(0, 20)
            .mapToObj(
                i -> {
                  String region = i % 2 == 0 ? "east" : "west";
                  return String.format("(%d, '%s', %f)", i, region, i * 1.5);
                })
            .collect(Collectors.joining(","));
    spark.sql(String.format("INSERT INTO %s (id, region, value) VALUES %s", fullTable, values));

    // Just verify data was written correctly — no clustering guarantee
    Dataset<Row> result = spark.sql("SELECT * FROM " + fullTable);
    assertEquals(20, result.count());
  }
}
