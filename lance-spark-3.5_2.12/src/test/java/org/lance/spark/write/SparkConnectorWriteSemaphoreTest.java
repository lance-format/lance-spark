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

import org.lance.spark.LanceConfig;
import org.lance.spark.LanceDataSource;
import org.lance.spark.TestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the Spark write path with the semaphore-based ArrowBatchWriteBuffer
 * (use_queued_write_buffer=false, the default). This ensures backward compatibility with the
 * original writer implementation.
 */
public class SparkConnectorWriteSemaphoreTest {
  private static SparkSession spark;
  private static Dataset<Row> testData;
  @TempDir static Path dbPath;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("spark-lance-connector-semaphore-test")
            .master("local")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceCatalog")
            .getOrCreate();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, false),
            });

    Row row1 = RowFactory.create(1, "Alice");
    Row row2 = RowFactory.create(2, "Bob");
    Row row3 = RowFactory.create(3, "Charlie");
    List<Row> data = Arrays.asList(row1, row2, row3);

    testData = spark.createDataFrame(data, schema);
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void writeWithSemaphoreBuffer(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    testData
        .write()
        .format(LanceDataSource.name)
        .option(
            LanceConfig.CONFIG_DATASET_URI, TestUtils.getDatasetUri(dbPath.toString(), datasetName))
        .option("use_queued_write_buffer", "false") // Explicitly use semaphore-based writer
        .save();

    Dataset<Row> result =
        spark
            .read()
            .format("lance")
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath.toString(), datasetName))
            .load();

    assertEquals(3, result.count());
    assertEquals(1, result.filter(col("id").equalTo(1)).count());
    assertEquals(1, result.filter(col("id").equalTo(2)).count());
    assertEquals(1, result.filter(col("id").equalTo(3)).count());
  }

  @Test
  public void appendWithSemaphoreBuffer(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    // Initial write
    testData
        .write()
        .format(LanceDataSource.name)
        .option(
            LanceConfig.CONFIG_DATASET_URI, TestUtils.getDatasetUri(dbPath.toString(), datasetName))
        .option("use_queued_write_buffer", "false")
        .save();

    // Append
    testData
        .write()
        .format(LanceDataSource.name)
        .option(
            LanceConfig.CONFIG_DATASET_URI, TestUtils.getDatasetUri(dbPath.toString(), datasetName))
        .option("use_queued_write_buffer", "false")
        .mode("append")
        .save();

    Dataset<Row> result =
        spark
            .read()
            .format("lance")
            .option(
                LanceConfig.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath.toString(), datasetName))
            .load();

    assertEquals(6, result.count()); // 3 + 3
  }
}
