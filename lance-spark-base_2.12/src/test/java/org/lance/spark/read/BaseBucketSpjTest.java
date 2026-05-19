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
package org.lance.spark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test verifying that bucket-partitioned Lance tables enable storage-partitioned joins
 * (SPJ) — joining without shuffle when both sides share the same bucket(N, col) partitioning.
 */
public abstract class BaseBucketSpjTest {
  protected String catalogName = "lance_bucket_spj_test";
  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-bucket-spj-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            // Enable V2 bucketing (required for SPJ)
            .config("spark.sql.sources.v2.bucketing.enabled", "true")
            .config("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
            .config("spark.sql.requireAllClusterKeysForCoPartition", "false")
            // Disable broadcast to force SortMergeJoin path
            // so we can verify SPJ eliminates the shuffle
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  /**
   * Creates a bucketed table with region-by-region inserts so each fragment contains a single
   * region value (zonemap min==max).
   */
  private void createBucketedTable(String tableName, int numBuckets) {
    String fullTable = catalogName + ".default." + tableName;
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance "
                + "PARTITIONED BY (bucket(%d, region))",
            fullTable, numBuckets));

    // Insert region-by-region to create one fragment per region
    // (required for zonemap min==max per fragment)
    String[] regions = {"east", "west", "north"};
    for (int r = 0; r < regions.length; r++) {
      String region = regions[r];
      int baseId = r * 10;
      StringBuilder values = new StringBuilder();
      for (int i = baseId; i < baseId + 10; i++) {
        if (values.length() > 0) values.append(",");
        values.append(String.format("(%d, '%s', %f)", i, region, i * 1.5));
      }
      spark.sql(String.format("INSERT INTO %s (id, region, value) VALUES %s", fullTable, values));
    }

    // Create zonemap index for bucket partition detection
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX region_idx" + " USING zonemap (region)", fullTable));
  }

  @Test
  public void testBucketJoinProducesCorrectResults() {
    String tableA = "bkt_a_" + UUID.randomUUID().toString().replace("-", "");
    String tableB = "bkt_b_" + UUID.randomUUID().toString().replace("-", "");
    createBucketedTable(tableA, 4);
    createBucketedTable(tableB, 4);

    String fullA = catalogName + ".default." + tableA;
    String fullB = catalogName + ".default." + tableB;

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT a.id, a.region, b.value "
                    + "FROM %s a JOIN %s b "
                    + "ON a.region = b.region",
                fullA, fullB));

    long count = joined.count();
    // 3 regions x 10 rows each side = 10*10 per region * 3
    assertEquals(3 * 10 * 10, count, "Bucket join should produce correct row count");
  }

  @Test
  public void testBucketJoinPlanShowsSpj() {
    String tableA = "bkt_spj_a_" + UUID.randomUUID().toString().replace("-", "");
    String tableB = "bkt_spj_b_" + UUID.randomUUID().toString().replace("-", "");
    createBucketedTable(tableA, 4);
    createBucketedTable(tableB, 4);

    String fullA = catalogName + ".default." + tableA;
    String fullB = catalogName + ".default." + tableB;

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT a.id, a.region, b.value "
                    + "FROM %s a JOIN %s b "
                    + "ON a.region = b.region",
                fullA, fullB));

    // Force execution so AQE finalizes the plan
    long count = joined.count();
    assertTrue(count > 0, "Join should produce results");

    String plan = joined.queryExecution().executedPlan().toString();

    boolean hasShuffleExchange = plan.contains("Exchange");

    assertFalse(
        hasShuffleExchange,
        "SPJ should eliminate shuffle — bucket(4, region)"
            + " partitioning must be recognized. Plan: "
            + plan);
  }
}
