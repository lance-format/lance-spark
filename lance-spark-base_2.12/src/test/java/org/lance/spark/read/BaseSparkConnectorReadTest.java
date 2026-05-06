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

import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class BaseSparkConnectorReadTest {
  private static SparkSession spark;
  private static String dbPath;
  private static Dataset<Row> data;
  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("spark-lance-connector-test")
            .master("local")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .getOrCreate();
    dbPath = TestUtils.TestTable1Config.dbPath;
    data =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
            .load();
    data.createOrReplaceTempView("test_dataset1");
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private void validateData(Dataset<Row> data, List<List<Long>> expectedValues) {
    List<Row> rows = data.collectAsList();
    assertEquals(expectedValues.size(), rows.size());

    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      List<Long> expectedRow = expectedValues.get(i);
      assertEquals(expectedRow.size(), row.size());

      for (int j = 0; j < expectedRow.size(); j++) {
        long expectedValue = expectedRow.get(j);
        long actualValue = row.getLong(j);
        assertEquals(expectedValue, actualValue, "Mismatch at row " + i + " column " + j);
      }
    }
  }

  @Test
  public void readAll() {
    validateData(data, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void filter() {
    validateData(
        data.filter("x > 1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .collect(Collectors.toList()));
    validateData(
        data.filter("y == 4"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(1) == 4)
            .collect(Collectors.toList()));
    validateData(
        data.filter("b >= 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(2) >= 6)
            .collect(Collectors.toList()));
    validateData(
        data.filter("c < -1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(3) < -1)
            .collect(Collectors.toList()));
    validateData(
        data.filter("c <= -1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(3) <= -1)
            .collect(Collectors.toList()));
    validateData(
        data.filter("c == -2"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(3) == -2)
            .collect(Collectors.toList()));
    validateData(
        data.filter("x > 1").filter("y < 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .filter(row -> row.get(1) < 6)
            .collect(Collectors.toList()));
    validateData(
        data.filter("x > 1 and y < 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .filter(row -> row.get(1) < 6)
            .collect(Collectors.toList()));
    validateData(
        data.filter("x > 1 or y < 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> (row.get(0) > 1) || (row.get(1) < 6))
            .collect(Collectors.toList()));
    validateData(
        data.filter("(x >= 1 and x <= 2) or (c >= -2 and c < 0)"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(
                row -> (row.get(0) >= 1 && row.get(0) <= 2) || (row.get(3) >= -2 && row.get(3) < 0))
            .collect(Collectors.toList()));
  }

  @Test
  public void select() {
    validateData(
        data.select("y", "b"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .collect(Collectors.toList()));
  }

  @Test
  public void filterSelect() {
    validateData(
        data.select("y", "b").filter("y > 3"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(
                row ->
                    Arrays.asList(row.get(1), row.get(2))) // "y" is at index 1, "b" is at index 2
            .filter(row -> row.get(0) > 3)
            .collect(Collectors.toList()));
  }

  @Test
  public void supportDataSourceLoadPath() {
    Dataset<Row> df =
        spark
            .read()
            .format("lance")
            .load(TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName));
    validateData(df, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void supportBroadcastJoin() {
    Dataset<Row> df =
        spark.read().format("lance").load(TestUtils.getDatasetUri(dbPath, "test_dataset3"));
    df.createOrReplaceTempView("test_dataset3");
    List<Row> desc =
        spark
            .sql("explain select t1.* from test_dataset1 t1 join test_dataset3 t3 on t1.x = t3.x")
            .collectAsList();
    assertEquals(1, desc.size());
    assertTrue(desc.get(0).getString(0).contains("BroadcastHashJoin"));
  }

  @Test
  public void readWithInvalidBatchSizeFails() {
    try {
      spark
          .read()
          .format(LanceDataSource.name)
          .option(
              LanceSparkReadOptions.CONFIG_DATASET_URI,
              TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
          .option("batch_size", "-1")
          .load();
      fail("Expected exception for invalid batch_size");
    } catch (Exception e) {
      Throwable cause = e;
      boolean found = false;
      while (cause != null) {
        if (cause.getMessage() != null
            && cause.getMessage().contains("batch_size must be positive")) {
          found = true;
          break;
        }
        cause = cause.getCause();
      }
      assertTrue(found, "Expected batch_size validation error, got: " + e.getMessage());
    }
  }

  @Test
  public void emptySchemaQueryShouldNotReadAllColumns() {
    // SELECT 1 FROM table needs zero data columns from the scan.
    // The scan should NOT project all table columns (x, y, b, c).
    Dataset<Row> result = spark.sql("SELECT 1 FROM test_dataset1");
    assertEquals(TestUtils.TestTable1Config.expectedValues.size(), result.collectAsList().size());

    // The BatchScan should have an empty output (no data columns projected)
    String physicalPlan = result.queryExecution().executedPlan().toString();
    assertFalse(
        physicalPlan.contains("x#"),
        "Column 'x' should not be projected for SELECT 1: " + physicalPlan);
    assertFalse(
        physicalPlan.contains("y#"),
        "Column 'y' should not be projected for SELECT 1: " + physicalPlan);
    assertFalse(
        physicalPlan.contains("b#"),
        "Column 'b' should not be projected for SELECT 1: " + physicalPlan);
    assertFalse(
        physicalPlan.contains("c#"),
        "Column 'c' should not be projected for SELECT 1: " + physicalPlan);
  }

  /**
   * Regression test for <a href="https://github.com/lance-format/lance-spark/issues/334">#334</a>:
   * nested subquery with COUNT(*) causes all columns to be scanned.
   *
   * <p>The optimizer rewrites COUNT(*) to COUNT(1), which needs zero columns from the data source.
   * Before the fix, {@code pruneColumns} rejected the empty schema, causing a full-table scan.
   */
  @Test
  public void nestedCountStarSubqueryShouldPruneColumns() {
    // Exact pattern from issue #334 — nested query with COUNT(*) over a filtered subquery
    Dataset<Row> result =
        spark.sql(
            "SELECT COUNT(*) FROM ("
                + "  SELECT x FROM test_dataset1 WHERE rand(42) < 1.0"
                + ") as subquery");

    // Verify correct result — all 4 rows pass the filter (rand < 1.0 is always true)
    assertEquals(4L, result.collectAsList().get(0).getLong(0));

    // The BatchScan should NOT project all table columns
    String physicalPlan = result.queryExecution().executedPlan().toString();
    assertFalse(
        physicalPlan.contains("y#"),
        "Column 'y' should not be projected in nested COUNT(*): " + physicalPlan);
    assertFalse(
        physicalPlan.contains("b#"),
        "Column 'b' should not be projected in nested COUNT(*): " + physicalPlan);
    assertFalse(
        physicalPlan.contains("c#"),
        "Column 'c' should not be projected in nested COUNT(*): " + physicalPlan);
  }

  /**
   * Regression test for <a href="https://github.com/lance-format/lance-spark/issues/334">#334</a>:
   * nested subquery selecting a single column should prune the rest.
   *
   * <p>The outer query references {@code x} from the subquery, so only {@code x} should be scanned
   * — not the full table schema.
   */
  @Test
  public void nestedSubqueryShouldPruneUnusedColumns() {
    Dataset<Row> result =
        spark.sql(
            "SELECT x FROM (" + "  SELECT x, y FROM test_dataset1 WHERE x > 1" + ") as subquery");

    // x > 1 keeps rows: (2,4,6,-2) and (3,6,9,-3)
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());

    // The BatchScan should only project 'x' (the only column the outer query needs),
    // not 'b' or 'c'
    String physicalPlan = result.queryExecution().executedPlan().toString();
    assertFalse(
        physicalPlan.contains("b#"),
        "Column 'b' should not be projected in nested subquery: " + physicalPlan);
    assertFalse(
        physicalPlan.contains("c#"),
        "Column 'c' should not be projected in nested subquery: " + physicalPlan);
  }

  /**
   * Regression test for <a href="https://github.com/lance-format/lance-spark/issues/334">#334</a>:
   * DataFrame drop() followed by a non-pushdown filter should prune the dropped column.
   *
   * <p>This is the original pattern reported in the issue: {@code
   * spark.table("my_table").drop("col").filter("rand(42) < 0.01")}
   */
  @Test
  public void dropColumnWithFilterShouldPruneDroppedColumn() {
    // Drop 'x' and apply a non-pushdown filter (rand is evaluated by Spark, not Lance)
    Dataset<Row> result = data.drop("x").filter("rand(42) < 1.0");

    // Schema should NOT contain 'x'
    assertEquals(3, result.schema().fields().length);
    assertTrue(result.schema().getFieldIndex("x").isEmpty());

    // All 4 rows pass the filter (rand < 1.0 is always true)
    List<Row> rows = result.collectAsList();
    assertEquals(4, rows.size());
    // Each row should have only 3 columns (y, b, c)
    assertEquals(3, rows.get(0).size());

    // Physical plan should NOT contain the dropped column 'x'
    String physicalPlan = result.queryExecution().executedPlan().toString();
    assertFalse(
        physicalPlan.contains("x#"),
        "Dropped column 'x' should not appear in the physical plan scan: " + physicalPlan);
  }

  /**
   * Regression test for <a href="https://github.com/lance-format/lance-spark/issues/334">#334</a>:
   * nested COUNT(agg_col) subquery should only scan the aggregated column.
   *
   * <p>Unlike COUNT(*)/COUNT(1), COUNT(col) actually needs the column to check for nulls. The scan
   * should project only that column, not the full table.
   */
  @Test
  public void nestedCountColumnSubqueryShouldOnlyScanAggColumn() {
    Dataset<Row> result =
        spark.sql(
            "SELECT COUNT(x) FROM ("
                + "  SELECT x FROM test_dataset1 WHERE rand(42) < 1.0"
                + ") as subquery");

    assertEquals(4L, result.collectAsList().get(0).getLong(0));

    // The BatchScan should only contain 'x' — not y, b, or c
    String physicalPlan = result.queryExecution().executedPlan().toString();
    assertFalse(
        physicalPlan.contains("y#"),
        "Column 'y' should not be projected for COUNT(x): " + physicalPlan);
    assertFalse(
        physicalPlan.contains("b#"),
        "Column 'b' should not be projected for COUNT(x): " + physicalPlan);
    assertFalse(
        physicalPlan.contains("c#"),
        "Column 'c' should not be projected for COUNT(x): " + physicalPlan);
  }

  /**
   * Regression test: filtering on string values containing single quotes (e.g., O'Brien) produced
   * malformed pushed-down SQL: {@code name == 'O'Brien'} — the unescaped quote broke the string
   * literal. Without the fix, lance-core's DataFusion tokenizer throws {@code
   * IllegalArgumentException: Unterminated string literal}, crashing the query. The fix escapes
   * single quotes as {@code ''} per SQL standard.
   */
  @Test
  public void testFilterWithSingleQuoteInStringValue() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);
    List<Row> testData =
        Arrays.asList(
            RowFactory.create(1, "O'Brien"),
            RowFactory.create(2, "Smith"),
            RowFactory.create(3, "D'Angelo"));
    Dataset<Row> df = spark.createDataFrame(testData, schema);

    String datasetPath = tempDir.toString() + "/quote_filter_test";
    df.write().format(LanceDataSource.name).save(datasetPath);

    Dataset<Row> lanceData = spark.read().format(LanceDataSource.name).load(datasetPath);

    Dataset<Row> result = lanceData.filter("name = \"O'Brien\"");

    // Without the fix, this crashes with:
    //   IllegalArgumentException: Unterminated string literal
    // because the pushed SQL contains unescaped quotes: name == 'O'Brien'
    List<Row> rows = result.collectAsList();
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("O'Brien", rows.get(0).getString(1));
  }

  @Test
  public void testArrayMaxOnNestedStructField() {
    // Create a schema with nested struct containing an array:
    // features: struct
    //   feature_1: struct
    //     feature_values: array<double>  (ordinal 0)
    //     feature_version: int           (ordinal 1)
    org.apache.spark.sql.types.StructType featureValuesStruct =
        new org.apache.spark.sql.types.StructType()
            .add(
                "feature_values",
                org.apache.spark.sql.types.DataTypes.createArrayType(
                    org.apache.spark.sql.types.DataTypes.DoubleType),
                true)
            .add("feature_version", org.apache.spark.sql.types.DataTypes.IntegerType, true);

    org.apache.spark.sql.types.StructType featuresStruct =
        new org.apache.spark.sql.types.StructType().add("feature_1", featureValuesStruct, true);

    org.apache.spark.sql.types.StructType schema =
        new org.apache.spark.sql.types.StructType()
            .add("id", org.apache.spark.sql.types.DataTypes.IntegerType, false)
            .add("features", featuresStruct, true);

    // Create test data (swapped order: array first, then int)
    List<Row> testData =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(
                1,
                org.apache.spark.sql.RowFactory.create(
                    org.apache.spark.sql.RowFactory.create(
                        JavaConverters.asScalaBuffer(Arrays.asList(0.5, 0.8, 0.3)).toSeq(), 1))),
            org.apache.spark.sql.RowFactory.create(
                2,
                org.apache.spark.sql.RowFactory.create(
                    org.apache.spark.sql.RowFactory.create(
                        JavaConverters.asScalaBuffer(Arrays.asList(0.9, 0.7, 0.6)).toSeq(), 2))),
            org.apache.spark.sql.RowFactory.create(
                3,
                org.apache.spark.sql.RowFactory.create(
                    org.apache.spark.sql.RowFactory.create(
                        JavaConverters.asScalaBuffer(Arrays.asList(0.2, 0.4, 0.1)).toSeq(), 3))));

    Dataset<Row> df = spark.createDataFrame(testData, schema);

    // Write to Lance
    String datasetPath = tempDir.toString() + "/nested_array_test";
    df.write().format(LanceDataSource.name).save(datasetPath);

    // Read from Lance
    Dataset<Row> lanceData = spark.read().format(LanceDataSource.name).load(datasetPath);
    lanceData.createOrReplaceTempView("nested_array_test");

    // Test array_max on nested struct field
    Dataset<Row> result =
        spark.sql(
            "SELECT id FROM nested_array_test WHERE array_max(features.feature_1.feature_values) > 0.8");
    List<Row> resultRows = result.collectAsList();
    assertEquals(1, resultRows.size());
    assertEquals(2, resultRows.get(0).getInt(0));

    // Also test count(*) query similar to the user's query
    Dataset<Row> countResult =
        spark.sql(
            "SELECT count(*) FROM nested_array_test WHERE array_max(features.feature_1.feature_values) > 0.8");
    assertEquals(1L, countResult.collectAsList().get(0).getLong(0));
  }

  @Test
  public void testFixedSizeArrayInNestedStruct() {
    // Create a schema with nested struct containing a fixed-size array:
    // features: struct
    //   embedding: array<double> (fixed size 3)
    //   version: int
    org.apache.spark.sql.types.Metadata fixedSizeMetadata =
        new org.apache.spark.sql.types.MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 3)
            .build();

    org.apache.spark.sql.types.StructType innerStruct =
        new org.apache.spark.sql.types.StructType()
            .add(
                new org.apache.spark.sql.types.StructField(
                    "embedding",
                    org.apache.spark.sql.types.DataTypes.createArrayType(
                        org.apache.spark.sql.types.DataTypes.DoubleType),
                    true,
                    fixedSizeMetadata))
            .add("version", org.apache.spark.sql.types.DataTypes.IntegerType, true);

    org.apache.spark.sql.types.StructType featuresStruct =
        new org.apache.spark.sql.types.StructType().add("data", innerStruct, true);

    org.apache.spark.sql.types.StructType schema =
        new org.apache.spark.sql.types.StructType()
            .add("id", org.apache.spark.sql.types.DataTypes.IntegerType, false)
            .add("features", featuresStruct, true);

    // Create test data with fixed-size arrays (exactly 3 elements each)
    List<Row> testData =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(
                1,
                org.apache.spark.sql.RowFactory.create(
                    org.apache.spark.sql.RowFactory.create(
                        JavaConverters.asScalaBuffer(Arrays.asList(0.5, 0.8, 0.3)).toSeq(), 1))),
            org.apache.spark.sql.RowFactory.create(
                2,
                org.apache.spark.sql.RowFactory.create(
                    org.apache.spark.sql.RowFactory.create(
                        JavaConverters.asScalaBuffer(Arrays.asList(0.9, 0.7, 0.6)).toSeq(), 2))),
            org.apache.spark.sql.RowFactory.create(
                3,
                org.apache.spark.sql.RowFactory.create(
                    org.apache.spark.sql.RowFactory.create(
                        JavaConverters.asScalaBuffer(Arrays.asList(0.2, 0.4, 0.1)).toSeq(), 3))));

    Dataset<Row> df = spark.createDataFrame(testData, schema);

    // Write to Lance
    String datasetPath = tempDir.toString() + "/nested_fixed_array_test";
    df.write().format(LanceDataSource.name).save(datasetPath);

    // Read from Lance
    Dataset<Row> lanceData = spark.read().format(LanceDataSource.name).load(datasetPath);
    lanceData.createOrReplaceTempView("nested_fixed_array_test");

    // Test array_max on nested struct field with fixed-size array
    Dataset<Row> result =
        spark.sql(
            "SELECT id FROM nested_fixed_array_test WHERE array_max(features.data.embedding) > 0.8");
    List<Row> resultRows = result.collectAsList();
    assertEquals(1, resultRows.size());
    assertEquals(2, resultRows.get(0).getInt(0));

    // Also test that we can read the array values directly
    Dataset<Row> selectResult =
        spark.sql("SELECT features.data.embedding FROM nested_fixed_array_test WHERE id = 1");
    List<Row> selectRows = selectResult.collectAsList();
    assertEquals(1, selectRows.size());
    // The array should have exactly 3 elements
    // Use scala.collection.Seq for Scala 2.12/2.13 compatibility
    scala.collection.Seq<?> arr = (scala.collection.Seq<?>) selectRows.get(0).get(0);
    assertEquals(3, arr.size());
  }

  /** Test filter pushdown with deeply nested struct > list&lt;struct&gt; > struct schema. */
  @Test
  public void testFilterWithArrayOfStructColumn() {
    // third_sub_obj: struct<int_val: int>
    StructType thirdSubObj = new StructType().add("int_val", DataTypes.IntegerType, true);

    // list element: struct<str_val: string, third_sub_obj: struct<int_val: int>>
    StructType listElement =
        new StructType()
            .add("str_val", DataTypes.StringType, true)
            .add("third_sub_obj", thirdSubObj, true);

    // sub_obj: struct<second_sub_obj: array<listElement>>
    StructType subObj =
        new StructType().add("second_sub_obj", DataTypes.createArrayType(listElement), true);

    StructType schema =
        new StructType().add("item", DataTypes.StringType, false).add("sub_obj", subObj, true);

    List<Row> testData =
        Arrays.asList(
            RowFactory.create(
                "test",
                RowFactory.create(
                    JavaConverters.asScalaBuffer(
                            Arrays.asList(RowFactory.create("val1", RowFactory.create(1))))
                        .toSeq())),
            RowFactory.create(
                "other",
                RowFactory.create(
                    JavaConverters.asScalaBuffer(
                            Arrays.asList(
                                RowFactory.create("val2", RowFactory.create(2)),
                                RowFactory.create("val3", RowFactory.create(3))))
                        .toSeq())),
            RowFactory.create(
                "test",
                RowFactory.create(
                    JavaConverters.asScalaBuffer(
                            Arrays.asList(RowFactory.create("val4", RowFactory.create(4))))
                        .toSeq())));

    Dataset<Row> df = spark.createDataFrame(testData, schema);

    String datasetPath = tempDir.toString() + "/nested_array_struct_filter_test";
    df.write().format(LanceDataSource.name).save(datasetPath);

    // Read without filter — baseline
    Dataset<Row> lanceData = spark.read().format(LanceDataSource.name).load(datasetPath);
    assertEquals(3, lanceData.count());

    // Read with filter on top-level "item" column
    List<Row> filtered = lanceData.filter("item = 'test'").collectAsList();
    assertEquals(2, filtered.size());

    // Verify nested struct fields are NOT dropped after filter pushdown.
    for (Row row : filtered) {
      assertEquals("test", row.getString(0));
      Row subObjRow = row.getStruct(1);
      scala.collection.Seq<?> arr = (scala.collection.Seq<?>) subObjRow.get(0);
      assertTrue(arr.size() > 0, "second_sub_obj array should not be empty");
      Row element = (Row) arr.apply(0);
      // str_val must be present and non-null
      assertFalse(element.isNullAt(0), "str_val should not be null");
      assertTrue(
          element.getString(0).startsWith("val"),
          "str_val should start with 'val', got: " + element.getString(0));
      // third_sub_obj.int_val must be present
      Row thirdRow = element.getStruct(1);
      assertFalse(thirdRow.isNullAt(0), "third_sub_obj.int_val should not be null");
    }

    // Verify specific values to ensure correct rows are returned
    filtered.sort(
        (a, b) -> {
          Row aEl = (Row) ((scala.collection.Seq<?>) a.getStruct(1).get(0)).apply(0);
          Row bEl = (Row) ((scala.collection.Seq<?>) b.getStruct(1).get(0)).apply(0);
          return Integer.compare(aEl.getStruct(1).getInt(0), bEl.getStruct(1).getInt(0));
        });
    Row first = filtered.get(0);
    Row firstElement = (Row) ((scala.collection.Seq<?>) first.getStruct(1).get(0)).apply(0);
    assertEquals("val1", firstElement.getString(0));
    assertEquals(1, firstElement.getStruct(1).getInt(0));

    Row second = filtered.get(1);
    Row secondElement = (Row) ((scala.collection.Seq<?>) second.getStruct(1).get(0)).apply(0);
    assertEquals("val4", secondElement.getString(0));
    assertEquals(4, secondElement.getStruct(1).getInt(0));
  }
}
