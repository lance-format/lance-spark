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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

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
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.root", tempDir.toString())
            .getOrCreate();
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lance.default");
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

    // Verify the count is correct (ids 11 to 49 = 39 rows)
    long count = countDataset.first().getLong(0);
    assertEquals(39L, count, "Filtered count should be 39");
  }

  @Test
  public void testCountStarWithExactScalarIndexUsesSingleTask() throws Exception {
    String tableName = "lance.default.count_indexed_single_partition_test_dataset";
    spark
        .range(0, 100)
        .selectExpr("id", "id % 10 as category")
        .repartition(4)
        .writeTo(tableName)
        .create();

    spark
        .sql(
            "ALTER TABLE "
                + tableName
                + " CREATE INDEX category_bitmap USING BITMAP(category) WITH (num_segments = 2)")
        .collectAsList();
    spark.catalog().refreshTable(tableName);

    Dataset<Row> indexedCount =
        spark.table(tableName).filter("category = 5").selectExpr("count(*)");

    org.apache.spark.sql.execution.SparkPlan executed =
        indexedCount.queryExecution().executedPlan();
    String plan = executed.toString();
    assertTrue(
        plan.contains(LanceIndexedCountScan.PLAN_MARKER),
        "A fully indexed equality COUNT(*) should be one indexed-count task. Plan: " + plan);
    assertFalse(plan.contains("LocalTableScan"), plan);
    assertEquals(1, indexedCountPartitions(executed), plan);
    assertEquals(10L, indexedCount.first().getLong(0));
  }

  @Test
  public void testCountStarWithoutScalarIndexKeepsFragmentParallelism() throws Exception {
    String tableName = "lance.default.count_unindexed_parallel_test_dataset";
    spark
        .range(0, 100)
        .selectExpr("id", "id % 10 as category")
        .repartition(4)
        .writeTo(tableName)
        .create();

    Dataset<Row> unindexedCount =
        spark.table(tableName).filter("category = 5").selectExpr("count(*)");

    assertEquals(10L, unindexedCount.first().getLong(0));
    String plan = unindexedCount.queryExecution().executedPlan().toString();
    assertTrue(
        plan.contains("BatchScan"),
        "An unindexed filtered COUNT(*) should retain a distributed scan. Plan: " + plan);
    assertFalse(plan.contains(LanceIndexedCountScan.PLAN_MARKER), plan);
  }

  @Test
  public void testCountStarOnEmptyTableUsesLocalScan() throws Exception {
    String tableName = "lance.default.count_empty_filtered_test_dataset";
    spark.range(0, 0).selectExpr("id", "id % 10 as category").writeTo(tableName).create();

    Dataset<Row> emptyCount = spark.table(tableName).filter("category = 5").selectExpr("count(*)");

    assertEquals(0L, emptyCount.first().getLong(0));
    String plan = emptyCount.queryExecution().executedPlan().toString();
    assertTrue(plan.contains("LocalTableScan"), plan);
    assertFalse(
        plan.contains("BatchScan"),
        "An empty filtered COUNT(*) should not plan a distributed scan. Plan: " + plan);
  }

  @Test
  public void testCountStarWithPartialScalarIndexKeepsDistributedScan() throws Exception {
    String tableName = "lance.default.count_partial_index_test_dataset";
    spark
        .range(0, 40)
        .selectExpr("id", "id % 10 as category")
        .repartition(2)
        .writeTo(tableName)
        .create();

    spark
        .sql(
            "ALTER TABLE "
                + tableName
                + " CREATE INDEX category_bitmap USING BITMAP(category) WITH (num_segments = 1)")
        .collectAsList();
    spark
        .range(40, 100)
        .selectExpr("id", "id % 10 as category")
        .repartition(2)
        .writeTo(tableName)
        .append();
    spark.catalog().refreshTable(tableName);

    Dataset<Row> partiallyIndexedCount =
        spark.table(tableName).filter("category = 5").selectExpr("count(*)");

    assertEquals(10L, partiallyIndexedCount.first().getLong(0));
    String plan = partiallyIndexedCount.queryExecution().executedPlan().toString();
    assertTrue(
        plan.contains("BatchScan"),
        "The partially indexed COUNT(*) plan should stay on BatchScan. Plan: " + plan);
    assertFalse(plan.contains(LanceIndexedCountScan.PLAN_MARKER), plan);
  }

  /** Partitions of the indexed-count scan, or -1 when that scan is absent. */
  private static int indexedCountPartitions(org.apache.spark.sql.execution.SparkPlan plan) {
    if (plan instanceof org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec) {
      return indexedCountPartitions(
          ((org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec) plan).inputPlan());
    }
    if (plan instanceof org.apache.spark.sql.execution.datasources.v2.BatchScanExec) {
      org.apache.spark.sql.connector.read.Scan scan =
          ((org.apache.spark.sql.execution.datasources.v2.BatchScanExec) plan).scan();
      if (scan instanceof org.apache.spark.sql.connector.read.Batch
          && scan.description().contains(LanceIndexedCountScan.PLAN_MARKER)) {
        return ((org.apache.spark.sql.connector.read.Batch) scan).planInputPartitions().length;
      }
    }
    scala.collection.Iterator<org.apache.spark.sql.execution.SparkPlan> children =
        plan.children().iterator();
    while (children.hasNext()) {
      int found = indexedCountPartitions(children.next());
      if (found >= 0) {
        return found;
      }
    }
    return -1;
  }
}
