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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class BaseSparkConnectorReadWithCdfVersionColumnsTest {
  private static SparkSession spark;
  private static String dbPath;
  private static Dataset<Row> data;

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
  public void readAllWithoutVersionColumns() {
    validateData(data, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void readAllWithBothVersionColumns() {
    validateData(
        data.select("x", "y", "b", "c", "_row_created_at_version", "_row_last_updated_at_version"),
        TestUtils.TestTable1Config.expectedValuesWithCdfVersionColumns);
  }

  @Test
  public void readWithCreatedAtVersionOnly() {
    validateData(
        data.select("x", "y", "_row_created_at_version"),
        TestUtils.TestTable1Config.expectedValuesWithCdfVersionColumns.stream()
            .map(row -> Arrays.asList(row.get(0), row.get(1), row.get(4)))
            .collect(Collectors.toList()));
  }

  @Test
  public void readWithLastUpdatedAtVersionOnly() {
    validateData(
        data.select("x", "y", "_row_last_updated_at_version"),
        TestUtils.TestTable1Config.expectedValuesWithCdfVersionColumns.stream()
            .map(row -> Arrays.asList(row.get(0), row.get(1), row.get(5)))
            .collect(Collectors.toList()));
  }

  @Test
  public void selectWithVersionColumns() {
    validateData(
        data.select("y", "b", "_row_created_at_version", "_row_last_updated_at_version"),
        TestUtils.TestTable1Config.expectedValuesWithCdfVersionColumns.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2), row.get(4), row.get(5)))
            .collect(Collectors.toList()));
  }

  @Test
  public void filterByDataColumnWithVersionColumns() {
    validateData(
        data.select("y", "b", "_row_created_at_version", "_row_last_updated_at_version")
            .filter("y > 3"),
        TestUtils.TestTable1Config.expectedValuesWithCdfVersionColumns.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2), row.get(4), row.get(5)))
            .filter(row -> row.get(0) > 3)
            .collect(Collectors.toList()));
  }

  @Test
  public void filterByCreatedAtVersion() {
    validateData(
        data.select("x", "y", "_row_created_at_version").filter("_row_created_at_version = 1"),
        TestUtils.TestTable1Config.expectedValuesWithCdfVersionColumns.stream()
            .map(row -> Arrays.asList(row.get(0), row.get(1), row.get(4)))
            .filter(row -> row.get(2) == 1)
            .collect(Collectors.toList()));
  }

  @Test
  public void filterByLastUpdatedAtVersion() {
    validateData(
        data.select("x", "y", "_row_last_updated_at_version")
            .filter("_row_last_updated_at_version = 1"),
        TestUtils.TestTable1Config.expectedValuesWithCdfVersionColumns.stream()
            .map(row -> Arrays.asList(row.get(0), row.get(1), row.get(5)))
            .filter(row -> row.get(2) == 1)
            .collect(Collectors.toList()));
  }

  @Test
  public void selectVersionColumnsWithRowIdAndRowAddress() {
    // Test that version columns work correctly alongside _rowid and _rowaddr
    Dataset<Row> result =
        data.select(
            "x",
            "y",
            "_rowid",
            "_rowaddr",
            "_row_created_at_version",
            "_row_last_updated_at_version");

    List<Row> rows = result.collectAsList();
    assertEquals(TestUtils.TestTable1Config.expectedValues.size(), rows.size());

    // Verify that all columns are present and have expected types
    for (Row row : rows) {
      assertEquals(6, row.size(), "Row should have 6 columns");
      // x, y should be data columns
      row.getLong(0); // x
      row.getLong(1); // y
      // _rowid, _rowaddr should be metadata columns
      row.getLong(2); // _rowid
      row.getLong(3); // _rowaddr
      // version columns
      row.getLong(4); // _row_created_at_version
      row.getLong(5); // _row_last_updated_at_version
    }
  }

  @Test
  public void testColumnOrdering() {
    // Test that version columns appear in the correct order in the output
    // Regular columns should come first, then metadata columns in order:
    // _rowid, _rowaddr, _row_last_updated_at_version, _row_created_at_version
    Dataset<Row> result =
        data.select(
            "x",
            "_row_last_updated_at_version",
            "y",
            "_row_created_at_version",
            "_rowid",
            "_rowaddr");

    List<String> columnNames = Arrays.asList(result.columns());
    assertEquals(6, columnNames.size());
    assertEquals("x", columnNames.get(0));
    assertEquals("_row_last_updated_at_version", columnNames.get(1));
    assertEquals("y", columnNames.get(2));
    assertEquals("_row_created_at_version", columnNames.get(3));
    assertEquals("_rowid", columnNames.get(4));
    assertEquals("_rowaddr", columnNames.get(5));
  }
}
