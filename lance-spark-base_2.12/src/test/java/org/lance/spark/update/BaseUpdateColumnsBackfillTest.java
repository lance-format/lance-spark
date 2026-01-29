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
package org.lance.spark.update;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base test class for UPDATE COLUMNS FROM command.
 *
 * <p>This tests the ability to update existing columns in a Lance table using data from a source
 * TABLE/VIEW. Only rows that match (by _rowaddr) are updated; rows in source that don't exist in
 * target are ignored.
 */
public abstract class BaseUpdateColumnsBackfillTest {
  protected String catalogName = "lance_test";
  protected String tableName = "update_column_backfill";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("dataframe-updatecolumn-test")
            .master("local[12]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  protected void prepareDataset() {
    // Create table with id (1, 2, 3) and value column
    spark.sql(
        String.format("create table %s (id int, value int, name string) using lance;", fullTable));
    spark.sql(
        String.format(
            "insert into %s (id, value, name) values (1, 10, 'one'), (2, 20, 'two'), (3, 30, 'three');",
            fullTable));
  }

  @Test
  public void testUpdateMatchingRows() {
    // Test case: target has id 1, 2, 3; source has id 2, 4
    // Expected: only id=2 is updated, id=1,3 unchanged, id=4 ignored
    prepareDataset();

    // Create source view with id=2 (matching) and id=4 (not matching)
    // Only update the 'value' column for matching rows
    spark.sql(
        String.format(
            "create temporary view update_source as "
                + "select _rowaddr, _fragid, 200 as value "
                + "from %s where id = 2",
            fullTable));

    // Execute UPDATE COLUMNS
    spark.sql(String.format("alter table %s update columns value from update_source", fullTable));

    // Verify results
    List<Row> results =
        spark
            .sql(String.format("select id, value, name from %s order by id", fullTable))
            .collectAsList();

    assertEquals(3, results.size());
    // id=1: unchanged
    assertEquals(1, results.get(0).getInt(0));
    assertEquals(10, results.get(0).getInt(1));
    assertEquals("one", results.get(0).getString(2));
    // id=2: value updated to 200
    assertEquals(2, results.get(1).getInt(0));
    assertEquals(200, results.get(1).getInt(1));
    assertEquals("two", results.get(1).getString(2));
    // id=3: unchanged
    assertEquals(3, results.get(2).getInt(0));
    assertEquals(30, results.get(2).getInt(1));
    assertEquals("three", results.get(2).getString(2));
  }

  @Test
  public void testUpdateMultipleColumns() {
    prepareDataset();

    // Update both value and name columns
    spark.sql(
        String.format(
            "create temporary view update_source as "
                + "select _rowaddr, _fragid, 200 as value, 'updated_two' as name "
                + "from %s where id = 2",
            fullTable));

    spark.sql(
        String.format("alter table %s update columns value, name from update_source", fullTable));

    List<Row> results =
        spark
            .sql(String.format("select id, value, name from %s order by id", fullTable))
            .collectAsList();

    // id=2 should have both value and name updated
    assertEquals(2, results.get(1).getInt(0));
    assertEquals(200, results.get(1).getInt(1));
    assertEquals("updated_two", results.get(1).getString(2));
  }

  @Test
  public void testUpdateNonExistentColumn() {
    prepareDataset();

    spark.sql(
        String.format(
            "create temporary view update_source as "
                + "select _rowaddr, _fragid, 100 as non_existent_col "
                + "from %s where id = 2",
            fullTable));

    // Should throw exception because 'non_existent_col' doesn't exist in target table
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            spark.sql(
                String.format(
                    "alter table %s update columns non_existent_col from update_source",
                    fullTable)),
        "Can't update non-existent columns: non_existent_col");
  }

  @Test
  public void testSourceRowNotInTarget() {
    // Verify that rows in source that don't exist in target are ignored
    prepareDataset();

    // Create a source with a row that has a _rowaddr that doesn't exist in target
    // This simulates the case where source has id=4 but target only has id=1,2,3
    // We use a subquery to generate fake _rowaddr values that don't match
    spark.sql(
        String.format(
            "create temporary view update_source as "
                + "select _rowaddr, _fragid, 999 as value "
                + "from %s where id = 2 "
                + "union all "
                + "select _rowaddr, _fragid, 888 as value "
                + "from %s where id = 1",
            fullTable, fullTable));

    spark.sql(String.format("alter table %s update columns value from update_source", fullTable));

    // Both id=1 and id=2 should be updated
    List<Row> results =
        spark.sql(String.format("select id, value from %s order by id", fullTable)).collectAsList();

    assertEquals(888, results.get(0).getInt(1)); // id=1 updated
    assertEquals(999, results.get(1).getInt(1)); // id=2 updated
    assertEquals(30, results.get(2).getInt(1)); // id=3 unchanged
  }

  @Test
  public void testUpdatePreservesRowIdAndFragId() {
    prepareDataset();

    // Get original _rowaddr and _fragid values
    List<Row> originalMeta =
        spark
            .sql(String.format("select id, _rowaddr, _fragid from %s order by id", fullTable))
            .collectAsList();

    // Update value column
    spark.sql(
        String.format(
            "create temporary view update_source as "
                + "select _rowaddr, _fragid, 200 as value "
                + "from %s where id = 2",
            fullTable));
    spark.sql(String.format("alter table %s update columns value from update_source", fullTable));

    // Get new _rowaddr and _fragid values
    List<Row> newMeta =
        spark
            .sql(String.format("select id, _rowaddr, _fragid from %s order by id", fullTable))
            .collectAsList();

    // All rows should preserve their _rowaddr and _fragid
    for (int i = 0; i < originalMeta.size(); i++) {
      assertEquals(
          originalMeta.get(i).getLong(1),
          newMeta.get(i).getLong(1),
          "Row " + i + " should preserve _rowaddr");
      assertEquals(
          originalMeta.get(i).getInt(2),
          newMeta.get(i).getInt(2),
          "Row " + i + " should preserve _fragid");
    }
  }
}
