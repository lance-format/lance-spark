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
package org.lance.spark.join;

import org.lance.spark.LanceConfig;
import org.lance.spark.LanceDataSource;
import org.lance.spark.TestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for fragment-aware join optimization.
 *
 * <p>These tests verify that joins on _rowaddr and _rowid columns are optimized using
 * fragment-based partitioning.
 */
public class FragmentAwareJoinTest {
  private static SparkSession spark;
  private static String dbPath;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("fragment-aware-join-test")
            .master("local[4]") // Use multiple cores to test parallelism
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .getOrCreate();
    dbPath = TestUtils.TestTable1Config.dbPath;
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testJoinOnRowAddress() {
    // Load the Lance table with row addresses
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Create a DataFrame that will join on row addresses
    // In a real scenario, this would be another table with references to row addresses
    Dataset<Row> lanceWithRowAddr =
        lanceTable
            .selectExpr("x", "y", "_rowaddr")
            .withColumnRenamed("x", "orig_x")
            .withColumnRenamed("y", "orig_y");

    // Perform a self-join on row address
    // This should trigger fragment-aware join optimization
    Dataset<Row> joined =
        lanceTable
            .alias("a")
            .join(
                lanceWithRowAddr.alias("b"),
                lanceTable.col("_rowaddr").equalTo(lanceWithRowAddr.col("_rowaddr")))
            .select("a.x", "a.y", "b.orig_x", "b.orig_y");

    // CRITICAL: Verify that optimizer is actually applied
    String queryPlan = joined.queryExecution().optimizedPlan().toString();
    assertTrue(
        queryPlan.contains("RepartitionByExpression") || queryPlan.contains("_lance_frag_id"),
        "Query plan should contain RepartitionByExpression or fragment ID column, "
            + "indicating fragment-aware optimization was applied. Plan: "
            + queryPlan);

    // Verify results
    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join should return results");

    // Verify that joined rows match
    for (Row row : results) {
      long x = row.getLong(0);
      long y = row.getLong(1);
      long origX = row.getLong(2);
      long origY = row.getLong(3);

      assertEquals(x, origX, "Joined x values should match");
      assertEquals(y, origY, "Joined y values should match");
    }
  }

  @Test
  public void testJoinOnRowAddressWithFilter() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Create a filtered subset
    Dataset<Row> subset = lanceTable.filter("x > 1").selectExpr("x", "_rowaddr as ref_rowaddr");

    // Join back to the original table
    Dataset<Row> joined =
        lanceTable
            .alias("a")
            .join(subset.alias("b"), lanceTable.col("_rowaddr").equalTo(subset.col("ref_rowaddr")))
            .select("a.x", "a.y", "b.x as subset_x");

    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join should return results");

    // All results should have x > 1 (from the filter)
    for (Row row : results) {
      long x = row.getLong(0);
      assertTrue(x > 1, "All joined rows should satisfy filter x > 1");
    }
  }

  @Test
  public void testFragmentIdExtraction() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Manually extract fragment ID using SQL expression
    Dataset<Row> withFragId =
        lanceTable.selectExpr("x", "y", "_rowaddr", "_rowaddr >> 32 as frag_id");

    List<Row> results = withFragId.collectAsList();
    assertFalse(results.isEmpty(), "Should have results");

    // Verify fragment ID extraction
    for (Row row : results) {
      long rowAddr = row.getLong(2);
      long fragId = row.getLong(3);

      int expectedFragId = FragmentAwareJoinUtils.extractFragmentId(rowAddr);
      assertEquals(expectedFragId, fragId, "Fragment ID extraction should match");
    }
  }

  @Test
  public void testMetadataColumnsAvailable() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Verify that metadata columns are available
    Dataset<Row> withMetadata = lanceTable.selectExpr("x", "_rowid", "_rowaddr", "_fragid");

    List<Row> results = withMetadata.collectAsList();
    assertFalse(results.isEmpty(), "Should have results with metadata columns");

    // Verify that metadata columns contain valid values
    for (Row row : results) {
      long rowId = row.getLong(1);
      long rowAddr = row.getLong(2);
      int fragId = row.getInt(3);

      assertTrue(rowAddr >= 0, "Row address should be non-negative");
      assertTrue(fragId >= 0, "Fragment ID should be non-negative");
    }
  }

  @Test
  public void testJoinWithHint() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    lanceTable.createOrReplaceTempView("lance_table");

    Dataset<Row> subset = lanceTable.filter("x > 1").selectExpr("x", "_rowaddr");
    subset.createOrReplaceTempView("subset_table");

    // Use SQL with explicit hint
    Dataset<Row> joined =
        spark.sql(
            "SELECT /*+ FRAGMENT_AWARE_JOIN(b) */ a.x, a.y, b.x as subset_x "
                + "FROM lance_table a "
                + "JOIN subset_table b ON a._rowaddr = b._rowaddr");

    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join with hint should return results");

    // Verify results
    for (Row row : results) {
      long x = row.getLong(0);
      assertTrue(x > 1, "All joined rows should satisfy filter");
    }
  }

  @Test
  public void testMultiFragmentJoin() {
    // This test verifies that joins work correctly across multiple fragments
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Get count of distinct fragments
    Dataset<Row> fragmentCount = lanceTable.selectExpr("_fragid").distinct();

    long numFragments = fragmentCount.count();
    assertTrue(numFragments > 0, "Should have at least one fragment");

    // Self-join should work across all fragments
    Dataset<Row> joined =
        lanceTable
            .alias("a")
            .join(
                lanceTable.alias("b"),
                lanceTable.col("_rowaddr").equalTo(lanceTable.col("_rowaddr")))
            .select("a.x", "b.x");

    long joinedCount = joined.count();
    long originalCount = lanceTable.count();

    assertEquals(
        originalCount, joinedCount, "Self-join on _rowaddr should return same number of rows");
  }

  @Test
  public void testFragmentAwareOptimizerIsApplied() {
    // This test specifically verifies that the fragment-aware optimizer rule is applied
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    Dataset<Row> subset = lanceTable.filter("x > 1").selectExpr("x", "_rowaddr as ref_rowaddr");

    // Join on _rowaddr (physical address)
    Dataset<Row> joined =
        lanceTable
            .alias("a")
            .join(subset.alias("b"), lanceTable.col("_rowaddr").equalTo(subset.col("ref_rowaddr")))
            .select("a.x", "a.y");

    // Check the optimized logical plan
    String optimizedPlan = joined.queryExecution().optimizedPlan().toString();
    System.out.println("=== Optimized Plan for _rowaddr join ===");
    System.out.println(optimizedPlan);

    // The plan should contain evidence of fragment-aware optimization:
    // 1. RepartitionByExpression nodes
    // 2. Fragment ID column (_lance_frag_id)
    // 3. ShiftRight expression (>>> 32)
    boolean hasRepartition = optimizedPlan.contains("RepartitionByExpression");
    boolean hasFragmentId =
        optimizedPlan.contains("_lance_frag_id") || optimizedPlan.contains("shiftright");

    assertTrue(
        hasRepartition || hasFragmentId,
        "Optimized plan should show fragment-aware optimization was applied. "
            + "Expected RepartitionByExpression or fragment ID extraction. Plan: "
            + optimizedPlan);

    // Verify the physical plan also shows co-located execution
    String physicalPlan = joined.queryExecution().executedPlan().toString();
    System.out.println("=== Physical Plan for _rowaddr join ===");
    System.out.println(physicalPlan);

    // Execute and verify correctness
    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join should return results");
  }

  @Test
  public void testStableRowIdJoin() {
    // Test join on stable _rowid (logical ID) instead of _rowaddr (physical address)
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Create a subset with stable row IDs
    Dataset<Row> subset = lanceTable.filter("x > 1").selectExpr("x", "y", "_rowid as ref_rowid");

    // Join on _rowid (stable logical ID)
    Dataset<Row> joined =
        lanceTable
            .alias("a")
            .join(subset.alias("b"), lanceTable.col("_rowid").equalTo(subset.col("ref_rowid")))
            .select("a.x", "a.y", "b.x as subset_x", "b.y as subset_y");

    // Check if optimizer detects _rowid join
    String optimizedPlan = joined.queryExecution().optimizedPlan().toString();
    System.out.println("=== Optimized Plan for _rowid join ===");
    System.out.println(optimizedPlan);

    // Note: For stable _rowid, optimization requires manifest lookup
    // The current implementation focuses on _rowaddr optimization
    // This test documents the behavior for future enhancement

    // Verify results are correct
    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join on _rowid should return results");

    // Verify data integrity
    for (Row row : results) {
      long x = row.getLong(0);
      long subsetX = row.getLong(2);
      assertEquals(x, subsetX, "Joined x values should match");
      assertTrue(x > 1, "All results should satisfy filter x > 1");
    }
  }

  @Test
  public void testNonRowAddressJoinNotOptimized() {
    // Verify that joins on regular columns are NOT optimized with fragment-aware strategy
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    Dataset<Row> subset = lanceTable.filter("x > 1").selectExpr("x", "y");

    // Join on regular column 'x', not on _rowaddr or _rowid
    Dataset<Row> joined =
        lanceTable
            .alias("a")
            .join(subset.alias("b"), lanceTable.col("x").equalTo(subset.col("x")))
            .select("a.x", "a.y");

    String optimizedPlan = joined.queryExecution().optimizedPlan().toString();
    System.out.println("=== Optimized Plan for regular column join ===");
    System.out.println(optimizedPlan);

    // This should NOT contain fragment-aware optimization
    assertFalse(
        optimizedPlan.contains("_lance_frag_id"),
        "Regular column join should NOT trigger fragment-aware optimization");

    // Verify results are still correct (regular join still works)
    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Regular join should return results");
  }

  @Test
  public void testFragmentIdExtractionExpression() {
    // Test that fragment ID extraction expression works correctly
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Manually extract fragment ID and compare with _fragid
    Dataset<Row> withExtractedFragId =
        lanceTable.selectExpr(
            "_rowaddr", "_fragid", "shiftright(_rowaddr, 32) as extracted_frag_id");

    List<Row> results = withExtractedFragId.collectAsList();
    assertFalse(results.isEmpty(), "Should have results");

    // Verify that extracted fragment ID matches the native _fragid
    for (Row row : results) {
      long rowAddr = row.getLong(0);
      int nativeFragId = row.getInt(1);
      long extractedFragId = row.getLong(2);

      assertEquals(
          nativeFragId,
          extractedFragId,
          String.format(
              "Extracted fragment ID (%d) should match native _fragid (%d) for rowaddr %d",
              extractedFragId, nativeFragId, rowAddr));

      // Also verify using the utility method
      int utilFragId = FragmentAwareJoinUtils.extractFragmentId(rowAddr);
      assertEquals(
          nativeFragId,
          utilFragId,
          String.format(
              "Utility extracted fragment ID (%d) should match native _fragid (%d)",
              utilFragId, nativeFragId));
    }
  }
}
