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
package org.lance.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base test class for reading/writing Lance tables using spark.read.format("lance").load() and
 * spark.write.format("lance").save() without configuring a catalog. This verifies that the
 * DataSource works directly via path-based access (auto-registered default catalog).
 */
public abstract class BaseLanceFormatTest {
  private static SparkSession spark;
  private static String datasetUri;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    // Create SparkSession WITHOUT configuring any lance catalog
    spark = SparkSession.builder().appName("lance-format-read-test").master("local").getOrCreate();
    datasetUri =
        TestUtils.getDatasetUri(
            TestUtils.TestTable1Config.dbPath, TestUtils.TestTable1Config.datasetName);
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
  public void testReadWithLoad() {
    // Test reading using spark.read.format("lance").load(path)
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(df, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void testReadWithPathOption() {
    // Test reading using spark.read.format("lance").option("path", ...).load()
    Dataset<Row> df =
        spark
            .read()
            .format("lance")
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, datasetUri)
            .load();
    validateData(df, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void testFilterPushdown() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.filter("x > 1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .collect(Collectors.toList()));
  }

  @Test
  public void testColumnProjection() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.select("y", "b"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .collect(Collectors.toList()));
  }

  @Test
  public void testFilterAndSelect() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.select("y", "b").filter("y > 3"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .filter(row -> row.get(0) > 3)
            .collect(Collectors.toList()));
  }

  @Test
  public void testWriteAndReadWithFormat() {
    // Test writing and reading using spark.write.format("lance").save(path)
    String outputPath = tempDir.resolve("test_write_format.lance").toString();

    // Create test data
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("name", DataTypes.StringType)
            .add("value", DataTypes.DoubleType);

    List<Row> data =
        Arrays.asList(
            RowFactory.create(1L, "Alice", 100.0),
            RowFactory.create(2L, "Bob", 200.0),
            RowFactory.create(3L, "Charlie", 300.0));

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write using format("lance").save(path) - use ErrorIfExists for creating new table
    df.write().format("lance").mode(SaveMode.ErrorIfExists).save(outputPath);

    // Read back and verify
    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    List<Row> result = readDf.orderBy("id").collectAsList();

    assertEquals(3, result.size());
    assertEquals(1L, result.get(0).getLong(0));
    assertEquals("Alice", result.get(0).getString(1));
    assertEquals(100.0, result.get(0).getDouble(2), 0.001);
    assertEquals(2L, result.get(1).getLong(0));
    assertEquals("Bob", result.get(1).getString(1));
    assertEquals(200.0, result.get(1).getDouble(2), 0.001);
    assertEquals(3L, result.get(2).getLong(0));
    assertEquals("Charlie", result.get(2).getString(1));
    assertEquals(300.0, result.get(2).getDouble(2), 0.001);
  }

  @Test
  public void testAppendMode() {
    // Test appending data using format("lance")
    String outputPath = tempDir.resolve("test_append_format.lance").toString();

    StructType schema =
        new StructType().add("id", DataTypes.LongType).add("value", DataTypes.LongType);

    // Create table first using ErrorIfExists mode
    List<Row> data1 = Arrays.asList(RowFactory.create(1L, 100L), RowFactory.create(2L, 200L));
    Dataset<Row> df1 = spark.createDataFrame(data1, schema);
    df1.write().format("lance").mode(SaveMode.ErrorIfExists).save(outputPath);

    // Append more data
    List<Row> data2 = Arrays.asList(RowFactory.create(3L, 300L), RowFactory.create(4L, 400L));
    Dataset<Row> df2 = spark.createDataFrame(data2, schema);
    df2.write().format("lance").mode(SaveMode.Append).save(outputPath);

    // Verify all data is present
    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    assertEquals(4, readDf.count());

    List<Row> result = readDf.orderBy("id").collectAsList();
    assertEquals(1L, result.get(0).getLong(0));
    assertEquals(2L, result.get(1).getLong(0));
    assertEquals(3L, result.get(2).getLong(0));
    assertEquals(4L, result.get(3).getLong(0));
  }
}
