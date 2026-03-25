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

import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceSparkReadOptions;
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
 * <p>Target join condition: A.origin_row_id = B._rowid
 *
 * <p>Two scenarios:
 *
 * <ol>
 *   <li>Non-stable rowid (default): _rowid = _rowaddr, fragment ID extracted via rowid >>> 32
 *   <li>Stable rowid: Requires RowIdIndex lookup (TODO: not yet implemented)
 * </ol>
 */
public class FragmentAwareJoinTest {
  private static SparkSession spark;
  private static String dbPath;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("fragment-aware-join-test")
            .master("local[4]")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
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

  /**
   * Test join on _rowid column.
   *
   * <p>Join condition: A.origin_row_id = B._rowid
   *
   * <p>For non-stable rowid datasets, _rowid = _rowaddr, so fragment ID can be extracted directly.
   */
  @Test
  public void testJoinOnRowId() {
    // Load the Lance table
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Create a reference table with origin_row_id column that stores _rowid values
    // This simulates the use case: A.origin_row_id = B._rowid
    lanceTable.selectExpr("x", "y", "_rowid").createOrReplaceTempView("lance_b");

    // Create table A with a reference column (origin_row_id = B._rowid)
    Dataset<Row> tableA =
        lanceTable.selectExpr("x as orig_x", "y as orig_y", "_rowid as origin_row_id");
    tableA.createOrReplaceTempView("table_a");

    // Perform join: A.origin_row_id = B._rowid
    Dataset<Row> joined =
        spark.sql(
            "SELECT a.orig_x, a.orig_y, b.x, b.y "
                + "FROM table_a a "
                + "JOIN lance_b b ON a.origin_row_id = b._rowid");

    // Verify that optimizer is applied (for non-stable rowid)
    String queryPlan = joined.queryExecution().optimizedPlan().toString();
    System.out.println("=== Optimized Plan for origin_row_id = _rowid join ===");
    System.out.println(queryPlan);

    assertTrue(
        queryPlan.contains("RepartitionByExpression") || queryPlan.contains("_lance_frag_id"),
        "Query plan should contain RepartitionByExpression or fragment ID column. Plan: "
            + queryPlan);

    // Verify results
    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join should return results");

    // Verify data integrity
    for (Row row : results) {
      assertEquals(row.getLong(0), row.getLong(2), "orig_x should equal x");
      assertEquals(row.getLong(1), row.getLong(3), "orig_y should equal y");
    }
  }

  /**
   * Test join on _rowid with filter.
   *
   * <p>Join condition: A.origin_row_id = B._rowid WHERE some_filter
   */
  @Test
  public void testJoinOnRowIdWithFilter() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Create views
    lanceTable.selectExpr("x", "y", "_rowid").createOrReplaceTempView("lance_filter_b");

    // Create table A with filtered subset
    Dataset<Row> tableA =
        lanceTable.filter("x > 1").selectExpr("x as orig_x", "_rowid as origin_row_id");
    tableA.createOrReplaceTempView("table_filter_a");

    // Join
    Dataset<Row> joined =
        spark.sql(
            "SELECT a.orig_x, b.x, b.y "
                + "FROM table_filter_a a "
                + "JOIN lance_filter_b b ON a.origin_row_id = b._rowid");

    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join should return results");

    // All results should satisfy filter
    for (Row row : results) {
      assertTrue(row.getLong(0) > 1, "orig_x should be > 1");
    }
  }

  /** Test fragment ID extraction from _rowid. */
  @Test
  public void testFragmentIdExtraction() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Manually extract fragment ID using SQL expression
    Dataset<Row> withFragId =
        lanceTable.selectExpr("x", "y", "_rowid", "shiftright(_rowid, 32) as frag_id");

    List<Row> results = withFragId.collectAsList();
    assertFalse(results.isEmpty(), "Should have results");

    // Verify fragment ID extraction
    for (Row row : results) {
      long rowId = row.getLong(2);
      long fragId = row.getLong(3);

      int expectedFragId = FragmentAwareJoinUtils.extractFragmentId(rowId);
      assertEquals(expectedFragId, fragId, "Fragment ID extraction should match");
    }
  }

  /** Test that metadata columns are available. */
  @Test
  public void testMetadataColumnsAvailable() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
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

  /** Test join with explicit FRAGMENT_AWARE_JOIN hint. */
  @Test
  public void testJoinWithHint() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Create views
    lanceTable.selectExpr("x", "y", "_rowid").createOrReplaceTempView("lance_hint_b");

    Dataset<Row> tableA = lanceTable.filter("x > 1").selectExpr("x", "_rowid as origin_row_id");
    tableA.createOrReplaceTempView("table_hint_a");

    // Use SQL with explicit hint
    Dataset<Row> joined =
        spark.sql(
            "SELECT /*+ FRAGMENT_AWARE_JOIN(b) */ a.x, b.x as bx, b.y "
                + "FROM table_hint_a a "
                + "JOIN lance_hint_b b ON a.origin_row_id = b._rowid");

    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Join with hint should return results");
  }

  /** Test that non-rowid joins are NOT optimized. */
  @Test
  public void testNonRowIdJoinNotOptimized() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    lanceTable.createOrReplaceTempView("lance_noopt_a");
    lanceTable.filter("x > 1").selectExpr("x", "y").createOrReplaceTempView("lance_noopt_b");

    // Join on regular column 'x', not on _rowid
    Dataset<Row> joined =
        spark.sql(
            "SELECT a.x, a.y " + "FROM lance_noopt_a a " + "JOIN lance_noopt_b b ON a.x = b.x");

    String optimizedPlan = joined.queryExecution().optimizedPlan().toString();
    System.out.println("=== Optimized Plan for regular column join ===");
    System.out.println(optimizedPlan);

    // This should NOT contain fragment-aware optimization
    assertFalse(
        optimizedPlan.contains("_lance_frag_id"),
        "Regular column join should NOT trigger fragment-aware optimization");

    List<Row> results = joined.collectAsList();
    assertFalse(results.isEmpty(), "Regular join should return results");
  }

  /** Test fragment ID extraction expression matches native _fragid. */
  @Test
  public void testFragmentIdExtractionExpression() {
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    // Manually extract fragment ID and compare with _fragid
    Dataset<Row> withExtractedFragId =
        lanceTable.selectExpr("_rowid", "_fragid", "shiftright(_rowid, 32) as extracted_frag_id");

    List<Row> results = withExtractedFragId.collectAsList();
    assertFalse(results.isEmpty(), "Should have results");

    // Verify that extracted fragment ID matches the native _fragid
    for (Row row : results) {
      long rowId = row.getLong(0);
      int nativeFragId = row.getInt(1);
      long extractedFragId = row.getLong(2);

      assertEquals(
          nativeFragId,
          extractedFragId,
          String.format(
              "Extracted fragment ID (%d) should match native _fragid (%d) for rowid %d",
              extractedFragId, nativeFragId, rowId));

      // Also verify using the utility method
      int utilFragId = FragmentAwareJoinUtils.extractFragmentId(rowId);
      assertEquals(
          nativeFragId,
          utilFragId,
          String.format(
              "Utility extracted fragment ID (%d) should match native _fragid (%d)",
              utilFragId, nativeFragId));
    }
  }

  /** Test that _rowid based fragment ID extraction works for non-stable rowid datasets. */
  @Test
  public void testRowIdEqualsRowAddrForNonStableRowId() {
    // For non-stable rowid datasets (default), _rowid should equal _rowaddr
    Dataset<Row> lanceTable =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();

    Dataset<Row> withBoth = lanceTable.selectExpr("x", "_rowid", "_rowaddr");

    List<Row> results = withBoth.collectAsList();
    assertFalse(results.isEmpty(), "Should have results");

    // For non-stable rowid, _rowid should equal _rowaddr
    // Note: If dataset has stable rowid enabled, this assertion may fail
    for (Row row : results) {
      long rowId = row.getLong(1);
      long rowAddr = row.getLong(2);

      // For default (non-stable) rowid mode, _rowid = _rowaddr
      assertEquals(
          rowId,
          rowAddr,
          "For non-stable rowid datasets, _rowid should equal _rowaddr. "
              + "If this fails, the test dataset may have stable rowid enabled.");
    }
  }
}
