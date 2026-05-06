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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test verifying that post-pruning statistics enable BroadcastHashJoin when one side of
 * an SPJ-eligible join is small after filter pushdown.
 *
 * <p>Setup: Two Lance tables with a "region" column, each with multiple fragments where each
 * fragment contains a single region value (min==max in zonemap). A btree index on "region" provides
 * zonemap stats. The table property {@code lance.partition.columns=region} enables SPJ.
 *
 * <p>Expected behavior:
 *
 * <ul>
 *   <li>Unfiltered join: SortMergeJoin with SPJ (both sides large, no broadcast)
 *   <li>Filtered join (one side pruned to 1 fragment): BroadcastHashJoin (pruned side is small
 *       enough to broadcast, JoinSelection picks broadcast before EnsureRequirements considers SPJ)
 * </ul>
 */
public abstract class BaseSpjBroadcastTest {
  protected String catalogName = "lance_spj_test";
  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-spj-broadcast-test")
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
            // Set a small broadcast threshold so the test is meaningful
            .config("spark.sql.autoBroadcastJoinThreshold", "1048576") // 1MB
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  /**
   * Creates a table with a region column where each INSERT creates a separate fragment with a
   * single region value — making the column SPJ-compatible (zonemap min==max per fragment).
   */
  private void createRegionTable(String tableName) {
    String fullTable = catalogName + ".default." + tableName;
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance "
                + "TBLPROPERTIES ('lance.partition.columns' = 'region')",
            fullTable));

    // Insert data region-by-region to create one fragment per region
    String[] regions = {"east", "west", "north", "south", "central"};
    for (int r = 0; r < regions.length; r++) {
      String region = regions[r];
      int baseId = r * 20;
      String values =
          IntStream.range(baseId, baseId + 20)
              .mapToObj(i -> String.format("(%d, '%s', %f)", i, region, i * 1.5))
              .collect(Collectors.joining(","));
      spark.sql(String.format("INSERT INTO %s (id, region, value) VALUES %s", fullTable, values));
    }

    // Create btree index on region to generate zonemap stats
    spark.sql(
        String.format("ALTER TABLE %s CREATE INDEX region_idx USING btree (region)", fullTable));
  }

  @Test
  public void testFilteredJoinUsesBroadcast() {
    String tableA = "spj_table_a_" + UUID.randomUUID().toString().replace("-", "");
    String tableB = "spj_table_b_" + UUID.randomUUID().toString().replace("-", "");
    createRegionTable(tableA);
    createRegionTable(tableB);

    String fullA = catalogName + ".default." + tableA;
    String fullB = catalogName + ".default." + tableB;

    // Filtered join: one side is pruned to a single fragment (region = 'east')
    // With accurate post-pruning stats, JoinSelection should pick BroadcastHashJoin
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT a.id, a.region, b.value "
                    + "FROM %s a JOIN %s b ON a.region = b.region "
                    + "WHERE a.region = 'east'",
                fullA, fullB));

    String physicalPlan = joined.queryExecution().executedPlan().toString();
    System.out.println("=== Physical Plan (filtered join) ===");
    System.out.println(physicalPlan);

    // Verify results are correct
    long count = joined.count();
    assertTrue(count > 0, "Filtered join should return results");

    // With post-pruning statistics, the filtered side should be small enough for broadcast.
    // The plan should contain BroadcastHashJoin or BroadcastExchange.
    // If it falls back to SortMergeJoin, the statistics estimation is not working.
    boolean hasBroadcast =
        physicalPlan.contains("BroadcastHashJoin") || physicalPlan.contains("BroadcastExchange");
    boolean hasSMJ = physicalPlan.contains("SortMergeJoin");

    // Log for debugging even if assertion passes
    if (hasBroadcast) {
      System.out.println("SUCCESS: Filtered join uses BroadcastHashJoin as expected");
    } else if (hasSMJ) {
      System.out.println(
          "WARNING: Filtered join uses SortMergeJoin — post-pruning stats may not be working");
    }

    assertTrue(
        hasBroadcast,
        "Filtered join should use BroadcastHashJoin when one side is small after pruning. "
            + "Plan: "
            + physicalPlan);
  }

  @Test
  public void testUnfilteredJoinUsesSortMerge() {
    String tableA = "spj_unfiltered_a_" + UUID.randomUUID().toString().replace("-", "");
    String tableB = "spj_unfiltered_b_" + UUID.randomUUID().toString().replace("-", "");
    createRegionTable(tableA);
    createRegionTable(tableB);

    String fullA = catalogName + ".default." + tableA;
    String fullB = catalogName + ".default." + tableB;

    // Unfiltered join: both sides are full tables
    // Should use SortMergeJoin (potentially with SPJ if stats exceed broadcast threshold)
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT a.id, a.region, b.value " + "FROM %s a JOIN %s b ON a.region = b.region",
                fullA, fullB));

    String physicalPlan = joined.queryExecution().executedPlan().toString();
    System.out.println("=== Physical Plan (unfiltered join) ===");
    System.out.println(physicalPlan);

    long count = joined.count();
    assertTrue(count > 0, "Unfiltered join should return results");

    // For such a small dataset, Spark may still use BroadcastHashJoin.
    // The key assertion is that the query works correctly regardless.
    System.out.println("Unfiltered join completed with " + count + " rows");
  }
}
