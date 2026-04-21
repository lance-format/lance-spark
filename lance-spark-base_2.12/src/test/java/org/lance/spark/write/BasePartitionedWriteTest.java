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
  public void testHashCollisionsBreakSinglePartitionPerFragment() {
    // Force only 2 shuffle partitions so many distinct partition values MUST hash-collide
    // into the same Spark write task. ClusteredDistribution is typically satisfied by hash
    // partitioning, so with (distinct values) > (shuffle partitions) collisions are
    // unavoidable. AQE coalesce is disabled to keep the 2-partition shape deterministic.
    spark.conf().set("spark.sql.shuffle.partitions", "2");
    spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "false");

    String tableName = "part_collide_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    // TODO: collapse back to `CREATE TABLE ... TBLPROPERTIES(...)` once the catalog
    // persists TBLPROPERTIES on create. Today only ALTER TABLE goes through
    // dataset.updateConfig, so properties set on CREATE are dropped before INSERT.
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance", fullTable));
    spark.sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES ('lance.partition.columns' = 'region')", fullTable));

    // 20 distinct regions into 2 buckets => some task will receive multiple regions.
    // Rows-per-region is tiny so all rows in a task comfortably fit in a single fragment —
    // that fragment will therefore span several distinct region values, violating the
    // "one value per fragment" guarantee that SPJ relies on.
    StringBuilder values = new StringBuilder();
    int numRegions = 20;
    int rowsPerRegion = 2;
    for (int r = 0; r < numRegions; r++) {
      String region = "region_" + r;
      for (int i = 0; i < rowsPerRegion; i++) {
        if (values.length() > 0) {
          values.append(",");
        }
        int id = r * rowsPerRegion + i;
        values.append(String.format("(%d, '%s', %f)", id, region, id * 1.5));
      }
    }
    spark.sql(String.format("INSERT INTO %s (id, region, value) VALUES %s", fullTable, values));

    Dataset<Row> fragRegions =
        spark.sql(
            String.format(
                "SELECT _fragid, COUNT(DISTINCT region) AS distinct_regions "
                    + "FROM %s GROUP BY _fragid",
                fullTable));

    List<Row> rows = fragRegions.collectAsList();
    assertFalse(rows.isEmpty(), "Should have at least one fragment");

    // Correctness claim: each fragment holds exactly one region — even under forced
    // hash collisions. This only holds because the writer rolls a fresh fragment at
    // every partition-value transition in the sorted stream; `Distributions.clustered`
    // alone does not imply one-value-per-fragment.
    for (Row row : rows) {
      long distinctRegions = row.getLong(1);
      assertEquals(
          1,
          distinctRegions,
          String.format(
              "Fragment %d contains %d distinct regions — writer failed to roll on "
                  + "partition-value boundary",
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
