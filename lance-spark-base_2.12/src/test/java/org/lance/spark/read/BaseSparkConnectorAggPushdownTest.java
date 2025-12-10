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

import org.lance.ReadOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.spark.internal.LanceDatasetAdapter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseSparkConnectorAggPushdownTest {
  private static SparkSession spark;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("LanceAggregatePushDownTest")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.root", tempDir.toString())
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCountStarPushDown() throws Exception {
    String tableName = "lance.default.count_test_dataset";
    spark.range(0, 100).toDF("id").repartition(4).writeTo(tableName).create();

    Dataset<Row> lanceDataset = spark.table(tableName);
    lanceDataset.selectExpr("count(*)").explain(true);
    Dataset<Row> countDataset = lanceDataset.selectExpr("count(*)");
    Row countRow = countDataset.first();
    long countFromSelectExpr = countRow.getLong(0);
    long count = lanceDataset.count();
    assertEquals(100L, countFromSelectExpr, "Count(*) should return 100");
    assertEquals(100L, count, "Count should return 100 rows");
  }

  @Test
  public void testCountStarWithFilter() throws Exception {
    String tableName = "lance.default.count_filter_test_dataset";

    // Create test data using catalog table
    spark
        .range(0, 100)
        .selectExpr("id", "id % 10 as category", "id * 2 as value")
        .repartition(4)
        .writeTo(tableName)
        .create();

    Dataset<Row> lanceDataset = spark.table(tableName);

    long filteredCount = lanceDataset.filter("category = 5").count();
    lanceDataset.explain(true);
    assertEquals(10, filteredCount, "Filtered count should return 10 rows");

    long complexFilteredCount = lanceDataset.filter("category > 5 AND value < 150").count();
    // category > 5 means 6,7,8,9 (4 categories)
    // value < 150 means id < 75 (since value = id * 2)
    // Each category has 7 values < 75, so 4 * 7 = 28
    assertEquals(28, complexFilteredCount, "Complex filtered count should return 28 rows");
  }

  @Test
  public void testMultipleAggregates() throws Exception {
    String tableName = "lance.default.multiple_agg_test_dataset";

    // Create test data using catalog table
    spark
        .range(1, 101)
        .selectExpr("id", "id * 10 as value")
        .repartition(4)
        .writeTo(tableName)
        .create();

    Dataset<Row> lanceDataset = spark.table(tableName);

    Dataset<Row> aggregates =
        lanceDataset.selectExpr("count(*) as cnt", "sum(value) as total", "avg(value) as average");

    Row result = aggregates.first();
    assertEquals(100L, result.getLong(0), "Count should be 100");
    assertEquals(50500L, result.getLong(1), "Sum should be 50500");
    assertEquals(505.0, result.getDouble(2), 0.001, "Average should be 505");
  }

  @Test
  public void testCountColumnNotPushedDown() throws Exception {
    String tableName = "lance.default.count_column_test_dataset";

    // Create test data with some nulls
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "a"),
                RowFactory.create(2L, null),
                RowFactory.create(3L, "c"),
                RowFactory.create(4L, null),
                RowFactory.create(5L, "e")),
            new StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.LongType)
                .add("name", org.apache.spark.sql.types.DataTypes.StringType))
        .writeTo(tableName)
        .create();

    // Force a refresh of the catalog
    spark.catalog().refreshTable(tableName);

    Dataset<Row> lanceDataset = spark.table(tableName);

    // COUNT(column) should not be pushed down (it excludes nulls)
    long countName = lanceDataset.selectExpr("count(name)").first().getLong(0);
    assertEquals(3L, countName, "Count(name) should be 3 (excluding nulls)");

    // COUNT(*) should still be pushed down
    long countStar = lanceDataset.selectExpr("count(*)").first().getLong(0);
    assertEquals(5L, countStar, "Count(*) should be 5");
  }

  @Test
  public void testCountDistinctNotPushedDown() throws Exception {
    String tableName = "lance.default.count_distinct_test_dataset";

    // Create test data with duplicates
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "a"),
                RowFactory.create(2L, "b"),
                RowFactory.create(3L, "a"),
                RowFactory.create(4L, "b"),
                RowFactory.create(5L, "c")),
            new StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.LongType)
                .add("category", org.apache.spark.sql.types.DataTypes.StringType))
        .writeTo(tableName)
        .create();

    // Force a refresh of the catalog
    spark.catalog().refreshTable(tableName);

    Dataset<Row> lanceDataset = spark.table(tableName);

    // COUNT(DISTINCT column) should not be pushed down
    long countDistinct = lanceDataset.selectExpr("count(distinct category)").first().getLong(0);
    assertEquals(3L, countDistinct, "Count(distinct category) should be 3");
  }

  @Test
  public void testCountStarWithoutFilterUsesLocalScan() throws Exception {
    String tableName = "lance.default.count_local_scan_test_dataset";
    spark.range(0, 50).toDF("id").repartition(4).writeTo(tableName).create();

    Dataset<Row> lanceDataset = spark.table(tableName);
    Dataset<Row> countDataset = lanceDataset.selectExpr("count(*)");

    // Get the query plan as string
    String plan = countDataset.queryExecution().executedPlan().toString();

    // Verify LocalScan is used (not BatchScan with partitions)
    assertTrue(
        plan.contains("LocalTableScan") || plan.contains("LanceLocalScan"),
        "COUNT(*) without filter should use LocalScan. Plan: " + plan);

    // Verify the count is correct
    long count = countDataset.first().getLong(0);
    assertEquals(50L, count, "Count should be 50");
  }

  @Test
  public void testCountStarWithFilterUsesBatchScan() throws Exception {
    String tableName = "lance.default.count_batch_scan_test_dataset";
    spark.range(0, 50).toDF("id").repartition(4).writeTo(tableName).create();

    Dataset<Row> lanceDataset = spark.table(tableName);
    Dataset<Row> countDataset = lanceDataset.filter("id > 10").selectExpr("count(*)");

    // Get the query plan as string
    String plan = countDataset.queryExecution().executedPlan().toString();

    // Verify BatchScan is used (not LocalScan) because of the filter
    assertTrue(
        plan.contains("BatchScan") || plan.contains("LanceScan"),
        "COUNT(*) with filter should use BatchScan. Plan: " + plan);

    // Verify LocalTableScan is NOT used (that's for metadata-only counts)
    assertFalse(
        plan.contains("LocalTableScan"),
        "COUNT(*) with filter should NOT use LocalTableScan. Plan: " + plan);

    // Verify SplitCountScan is NOT used (no index exists)
    assertFalse(
        plan.contains("LanceSplitCountScan"),
        "COUNT(*) with filter but no index should NOT use LanceSplitCountScan. Plan: " + plan);

    // Verify the count is correct (ids 11 to 49 = 39 rows)
    long count = countDataset.first().getLong(0);
    assertEquals(39L, count, "Filtered count should be 39");
  }

  @Test
  public void testCountStarWithIndexedColumnUsesLocalScan() throws Exception {
    String tableName = "lance.default.count_indexed_local_scan_test";
    String datasetPath = tempDir.resolve("count_indexed_local_scan_test.lance").toString();

    // Create dataset with multiple fragments
    spark.range(0, 100).toDF("id").repartition(4).writeTo(tableName).create();

    // Create a BTREE index on the 'id' column using Lance Java SDK
    BufferAllocator allocator = LanceDatasetAdapter.allocator;
    try (org.lance.Dataset lanceDataset =
        org.lance.Dataset.open(allocator, datasetPath, new ReadOptions.Builder().build())) {

      ScalarIndexParams scalarParams = ScalarIndexParams.create("btree", "{}");
      IndexParams indexParams = IndexParams.builder().setScalarIndexParams(scalarParams).build();

      lanceDataset.createIndex(
          Collections.singletonList("id"),
          IndexType.BTREE,
          Optional.of("id_btree_index"),
          indexParams,
          true);

      // Verify index was created
      assertTrue(lanceDataset.listIndexes().contains("id_btree_index"), "Index should be created");
    }

    // Refresh the table to pick up the new index
    spark.catalog().refreshTable(tableName);

    // Query with filter on indexed column
    Dataset<Row> lanceDataset = spark.table(tableName);
    Dataset<Row> countDataset = lanceDataset.filter("id > 50").selectExpr("count(*)");

    // Get the query plan as string
    String plan = countDataset.queryExecution().executedPlan().toString();

    // When all fragments are indexed, it should use LocalTableScan (direct index count)
    assertTrue(
        plan.contains("LocalTableScan"),
        "COUNT(*) with filter on fully indexed column should use LocalTableScan. Plan: " + plan);

    // Verify the count is correct (ids 51 to 99 = 49 rows)
    long count = countDataset.first().getLong(0);
    assertEquals(49L, count, "Filtered count should be 49");
  }

  @Test
  public void testCountStarWithPartialIndexUsesSplitScan() throws Exception {
    String tableName = "lance.default.count_split_scan_test";
    String datasetPath = tempDir.resolve("count_split_scan_test.lance").toString();

    // Create initial dataset with 2 fragments
    spark.range(0, 50).toDF("id").repartition(2).writeTo(tableName).create();

    // Create a BTREE index on 'id' column (covers initial fragments)
    BufferAllocator allocator = LanceDatasetAdapter.allocator;
    try (org.lance.Dataset lanceDataset =
        org.lance.Dataset.open(allocator, datasetPath, new ReadOptions.Builder().build())) {

      ScalarIndexParams scalarParams = ScalarIndexParams.create("btree", "{}");
      IndexParams indexParams = IndexParams.builder().setScalarIndexParams(scalarParams).build();

      lanceDataset.createIndex(
          Collections.singletonList("id"),
          IndexType.BTREE,
          Optional.of("id_partial_index"),
          indexParams,
          true);

      assertTrue(
          lanceDataset.listIndexes().contains("id_partial_index"), "Index should be created");
    }

    // Append more data (creates new unindexed fragments)
    spark.range(50, 100).toDF("id").repartition(2).writeTo(tableName).append();

    // Refresh the table
    spark.catalog().refreshTable(tableName);

    // Verify total row count first
    Dataset<Row> lanceDataset = spark.table(tableName);
    long totalCount = lanceDataset.count();
    assertEquals(100L, totalCount, "Total count should be 100");

    // Query with filter on partially indexed column
    Dataset<Row> countDataset = lanceDataset.filter("id > 25").selectExpr("count(*)");

    // Get the query plan as string
    String plan = countDataset.queryExecution().executedPlan().toString();

    // With partial index, it could use SplitCountScan (optimization) or BatchScan (fallback)
    // The key is that the query should still return correct results
    assertTrue(
        plan.contains("LanceSplitCountScan") || plan.contains("BatchScan"),
        "COUNT(*) with filter on partially indexed column should use SplitCountScan or BatchScan. Plan: "
            + plan);

    // Verify the count is correct (ids 26 to 99 = 74 rows)
    // Note: If SplitCountScan optimization is used but countIndexedRows JNI is incomplete,
    // this may fail. In that case, we'd fall back to BatchScan which should be correct.
    long count = countDataset.first().getLong(0);
    assertEquals(74L, count, "Filtered count should be 74");
  }
}
