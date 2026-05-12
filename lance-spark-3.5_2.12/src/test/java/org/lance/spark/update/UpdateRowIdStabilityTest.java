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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies that SQL UPDATE on a Lance table with stable row IDs preserves _rowid values. Uses the
 * native DeltaWriter.update() path with task-level RowIdMeta attachment.
 */
public class UpdateRowIdStabilityTest {
  private SparkSession spark;
  private String catalogName = "lance_ns";
  private String fullTableName;

  @TempDir private Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("update-rowid-stability-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");

    String tableName = "rowid_stability_" + System.nanoTime();
    fullTableName = catalogName + ".default." + tableName;
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testUpdatePreservesRowIdSingleFragment() {
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT NOT NULL, name STRING, value INT)"
            + " TBLPROPERTIES ('enable_stable_row_ids' = 'true')");

    spark.sql(
        "INSERT INTO "
            + fullTableName
            + " VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

    Map<Integer, Long> rowIdsBefore = readRowIds();
    assertFalse(rowIdsBefore.isEmpty());

    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE value >= 200");

    Map<Integer, Long> rowIdsAfter = readRowIds();

    for (Map.Entry<Integer, Long> entry : rowIdsBefore.entrySet()) {
      assertEquals(
          entry.getValue(),
          rowIdsAfter.get(entry.getKey()),
          "rowId changed for id=" + entry.getKey());
    }

    verifyDataValues(Map.of(1, 100, 2, 201, 3, 301));
  }

  @Test
  public void testUpdatePreservesRowIdMultipleFragments() {
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT NOT NULL, name STRING, value INT)"
            + " TBLPROPERTIES ('enable_stable_row_ids' = 'true')");

    // Fragment 1
    spark.sql(
        "INSERT INTO "
            + fullTableName
            + " VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");
    // Fragment 2
    spark.sql(
        "INSERT INTO "
            + fullTableName
            + " VALUES (4, 'Dave', 400), (5, 'Eve', 500), (6, 'Frank', 600)");
    // Fragment 3
    spark.sql(
        "INSERT INTO "
            + fullTableName
            + " VALUES (7, 'Grace', 700), (8, 'Heidi', 800), (9, 'Ivan', 900), (10, 'Judy', 1000)");

    Map<Integer, Long> rowIdsBefore = readRowIds();
    assertEquals(10, rowIdsBefore.size());

    // Update rows spanning all three fragments
    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE value >= 200");

    Map<Integer, Long> rowIdsAfter = readRowIds();

    for (Map.Entry<Integer, Long> entry : rowIdsBefore.entrySet()) {
      assertEquals(
          entry.getValue(),
          rowIdsAfter.get(entry.getKey()),
          "rowId changed for id=" + entry.getKey());
    }

    verifyDataValues(
        Map.of(1, 100, 2, 201, 3, 301, 4, 401, 5, 501, 6, 601, 7, 701, 8, 801, 9, 901, 10, 1001));
  }

  @Test
  public void testUpdatePreservesRowIdNonContiguous() {
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT NOT NULL, name STRING, value INT)"
            + " TBLPROPERTIES ('enable_stable_row_ids' = 'true')");

    spark.sql(
        "INSERT INTO "
            + fullTableName
            + " VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30), (4, 'D', 40), (5, 'E', 50)");
    spark.sql(
        "INSERT INTO "
            + fullTableName
            + " VALUES (6, 'F', 60), (7, 'G', 70), (8, 'H', 80), (9, 'I', 90), (10, 'J', 100)");

    Map<Integer, Long> rowIdsBefore = readRowIds();

    // Update non-contiguous rows: odd IDs only, spanning both fragments
    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE id % 2 = 1");

    Map<Integer, Long> rowIdsAfter = readRowIds();

    for (Map.Entry<Integer, Long> entry : rowIdsBefore.entrySet()) {
      assertEquals(
          entry.getValue(),
          rowIdsAfter.get(entry.getKey()),
          "rowId changed for id=" + entry.getKey());
    }
  }

  @Test
  public void testUpdateAllRowsPreservesRowId() {
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT NOT NULL, name STRING, value INT)"
            + " TBLPROPERTIES ('enable_stable_row_ids' = 'true')");

    spark.sql("INSERT INTO " + fullTableName + " VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
    spark.sql("INSERT INTO " + fullTableName + " VALUES (4, 'D', 40), (5, 'E', 50)");

    Map<Integer, Long> rowIdsBefore = readRowIds();

    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE id > 0");

    Map<Integer, Long> rowIdsAfter = readRowIds();

    for (Map.Entry<Integer, Long> entry : rowIdsBefore.entrySet()) {
      assertEquals(
          entry.getValue(),
          rowIdsAfter.get(entry.getKey()),
          "rowId changed for id=" + entry.getKey());
    }
  }

  @Test
  public void testSequentialUpdatesPreserveRowId() {
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT NOT NULL, name STRING, value INT)"
            + " TBLPROPERTIES ('enable_stable_row_ids' = 'true')");

    spark.sql("INSERT INTO " + fullTableName + " VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");

    Map<Integer, Long> rowIdsBefore = readRowIds();

    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE id = 1");
    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE id = 2");
    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE id = 3");

    Map<Integer, Long> rowIdsAfter = readRowIds();

    for (Map.Entry<Integer, Long> entry : rowIdsBefore.entrySet()) {
      assertEquals(
          entry.getValue(),
          rowIdsAfter.get(entry.getKey()),
          "rowId changed for id=" + entry.getKey());
    }

    verifyDataValues(Map.of(1, 11, 2, 21, 3, 31));
  }

  /**
   * After an update, rows move to new fragments so _rowaddr changes while _rowid is preserved via
   * RowIdMeta. A second update must use the NEW _rowaddr (for fragment deletion) and the ORIGINAL
   * _rowid (for RowIdMeta attachment). This test exercises the case where _rowid != _rowaddr,
   * verifying the id InternalRow column ordering (ID_COL_ROW_ID vs ID_COL_ROW_ADDR) is correct.
   */
  @Test
  public void testRepeatedUpdatesMultiFragmentPreservesRowId() {
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT NOT NULL, name STRING, value INT)"
            + " TBLPROPERTIES ('enable_stable_row_ids' = 'true')");

    // Two separate inserts create two fragments with different fragment IDs.
    // Fragment 0: rows (1,2,3) with _rowaddr = (0<<32)|{0,1,2} and _rowid = {0,1,2}
    // Fragment 1: rows (4,5,6) with _rowaddr = (1<<32)|{0,1,2} and _rowid = {3,4,5}
    spark.sql("INSERT INTO " + fullTableName + " VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
    spark.sql("INSERT INTO " + fullTableName + " VALUES (4, 'D', 40), (5, 'E', 50), (6, 'F', 60)");

    Map<Integer, Long> originalRowIds = readRowIds();
    assertEquals(6, originalRowIds.size());

    // First update: moves rows to new fragments. After this, _rowaddr values change
    // (rows are in new fragments) but _rowid values are preserved via RowIdMeta.
    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE id IN (2, 5)");

    Map<Integer, Long> afterFirstUpdate = readRowIds();
    for (Map.Entry<Integer, Long> entry : originalRowIds.entrySet()) {
      assertEquals(
          entry.getValue(),
          afterFirstUpdate.get(entry.getKey()),
          "rowId changed after first update for id=" + entry.getKey());
    }

    // Second update on the SAME rows: now _rowid != _rowaddr for the previously updated rows.
    // If the id column ordering is wrong (e.g. using _rowid as _rowaddr for deletion), this
    // would attempt to delete from the wrong fragment and either corrupt data or fail.
    spark.sql("UPDATE " + fullTableName + " SET value = value + 1 WHERE id IN (2, 5)");

    Map<Integer, Long> afterSecondUpdate = readRowIds();
    for (Map.Entry<Integer, Long> entry : originalRowIds.entrySet()) {
      assertEquals(
          entry.getValue(),
          afterSecondUpdate.get(entry.getKey()),
          "rowId changed after second update for id=" + entry.getKey());
    }

    verifyDataValues(Map.of(1, 10, 2, 22, 3, 30, 4, 40, 5, 52, 6, 60));
  }

  private Map<Integer, Long> readRowIds() {
    Dataset<Row> df = spark.sql("SELECT id, _rowid FROM " + fullTableName + " ORDER BY id");
    List<Row> rows = df.collectAsList();
    return rows.stream().collect(Collectors.toMap(r -> r.getInt(0), r -> r.getLong(1)));
  }

  private void verifyDataValues(Map<Integer, Integer> expected) {
    Dataset<Row> df = spark.sql("SELECT id, value FROM " + fullTableName + " ORDER BY id");
    List<Row> rows = df.collectAsList();
    Map<Integer, Integer> actual =
        rows.stream().collect(Collectors.toMap(r -> r.getInt(0), r -> r.getInt(1)));
    assertEquals(expected, actual);
  }
}
