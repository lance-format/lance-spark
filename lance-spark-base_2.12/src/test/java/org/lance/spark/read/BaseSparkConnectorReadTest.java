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
import org.apache.spark.sql.SparkSession;
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
}
