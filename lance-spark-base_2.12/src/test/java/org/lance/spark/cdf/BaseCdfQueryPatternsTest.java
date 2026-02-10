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
package org.lance.spark.cdf;

import org.lance.spark.cdf.CdfTestHelper.CdfRow;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for common Change Data Feed (CDF) query patterns using version tracking columns. These
 * tests demonstrate typical CDC use cases.
 *
 * <p>Version numbering: CREATE TABLE = v1 (empty), first INSERT = v2, etc.
 */
public abstract class BaseCdfQueryPatternsTest {
  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-cdf-query-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testIdentifyAllChangesInLatestVersion() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    // v3: Update some rows
    helper.update("value = value + 10", "id IN (1, 2)");

    // v4: Insert new row
    helper.insert(Collections.singletonList(CdfRow.of(4, "David", 400)));

    // Query: Find all changes in the latest version (v4)
    String sql =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s "
                + "WHERE _row_created_at_version = 4 OR _row_last_updated_at_version = 4 "
                + "ORDER BY id",
            helper.getTableName());

    List<CdfRow> changes = helper.query(sql);

    // Only David was added in v4
    assertEquals(1, changes.size());
    assertEquals(CdfRow.ofWithVersions(4, "David", 400, 4L, 4L), changes.get(0));
  }

  @Test
  public void testIdentifyChangesBetweenVersionRange() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial data
    helper.insert(Arrays.asList(CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200)));

    // v3: Update one row
    helper.update("value = 150", "id = 1");

    // v4: Insert new row
    helper.insert(Collections.singletonList(CdfRow.of(3, "Charlie", 300)));

    // v5: Update multiple rows
    helper.update("value = value + 50", "id IN (2, 3)");

    // v6: Insert another row
    helper.insert(Collections.singletonList(CdfRow.of(4, "David", 400)));

    // Query: Find all changes between versions 3 and 5 (inclusive)
    String sql =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s "
                + "WHERE (_row_created_at_version BETWEEN 3 AND 5) "
                + "   OR (_row_last_updated_at_version BETWEEN 3 AND 5) "
                + "ORDER BY id",
            helper.getTableName());

    List<CdfRow> changes = helper.query(sql);

    // Alice (updated v3), Bob (updated v5), Charlie (updated v5)
    assertEquals(3, changes.size());
    assertEquals(CdfRow.ofWithVersions(1, "Alice", 150, 1L, 3L), changes.get(0));
    assertEquals(CdfRow.ofWithVersions(2, "Bob", 250, 1L, 5L), changes.get(1));
    assertEquals(CdfRow.ofWithVersions(3, "Charlie", 350, 1L, 5L), changes.get(2));
  }

  @Test
  public void testIdentifyRowsModifiedMultipleTimes() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    // v3: Update Alice
    helper.update("value = value + 1", "id = 1");

    // v4: Update Alice again
    helper.update("value = value + 1", "id = 1");

    // v5: Update Bob
    helper.update("value = value + 1", "id = 2");

    // Query: Find rows where created_at_version != last_updated_at_version
    // (i.e., rows that have been modified after creation)
    String sql =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s "
                + "WHERE _row_created_at_version != _row_last_updated_at_version "
                + "ORDER BY id",
            helper.getTableName());

    List<CdfRow> modifiedRows = helper.query(sql);

    assertEquals(2, modifiedRows.size());
    assertEquals(CdfRow.ofWithVersions(1, "Alice", 102, 1L, 4L), modifiedRows.get(0));
    assertEquals(CdfRow.ofWithVersions(2, "Bob", 201, 1L, 5L), modifiedRows.get(1));

    // Query: Find rows modified more recently than version 2
    sql =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s "
                + "WHERE _row_last_updated_at_version > 2 "
                + "ORDER BY id",
            helper.getTableName());

    List<CdfRow> recentlyModified = helper.query(sql);

    assertEquals(2, recentlyModified.size());
    assertEquals(CdfRow.ofWithVersions(1, "Alice", 102, 1L, 4L), recentlyModified.get(0));
    assertEquals(CdfRow.ofWithVersions(2, "Bob", 201, 1L, 5L), recentlyModified.get(1));
  }

  @Test
  public void testIncrementalProcessingCdcPipeline() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // Simulate a CDC pipeline that processes changes incrementally

    // v2: Initial load
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    // CDC Pipeline: Process batch 1 (everything since v1=CREATE TABLE)
    long lastProcessedVersion = 1L;
    List<CdfRow> batch1 = helper.getChangesSince(lastProcessedVersion);
    assertEquals(3, batch1.size());
    lastProcessedVersion = 2L;

    // v3: Update, v4: Insert
    helper.update("value = value + 50", "id = 1");
    helper.insert(Collections.singletonList(CdfRow.of(4, "David", 400)));

    // CDC Pipeline: Process batch 2
    List<CdfRow> batch2 = helper.getChangesSince(lastProcessedVersion);
    assertEquals(2, batch2.size());
    assertTrue(batch2.stream().anyMatch(r -> r.id == 1 && r.lastUpdatedAtVersion == 3L));
    assertTrue(batch2.stream().anyMatch(r -> r.id == 4 && r.createdAtVersion == 4L));
    lastProcessedVersion = 4L;

    // v5: More updates
    helper.update("value = value + 100", "id IN (2, 3)");

    // CDC Pipeline: Process batch 3
    List<CdfRow> batch3 = helper.getChangesSince(lastProcessedVersion);
    assertEquals(2, batch3.size());
    assertTrue(batch3.stream().allMatch(r -> r.lastUpdatedAtVersion == 5L));
    assertTrue(batch3.stream().anyMatch(r -> r.id == 2));
    assertTrue(batch3.stream().anyMatch(r -> r.id == 3));
  }

  @Test
  public void testIdentifyNewRowsVsUpdatedRows() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(Arrays.asList(CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200)));

    // v3: Update existing row
    helper.update("value = value + 10", "id = 1");

    // v4: Insert new row
    helper.insert(Collections.singletonList(CdfRow.of(3, "Charlie", 300)));

    // Query: Find rows created in v4 (new inserts)
    String sqlNewRows =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s "
                + "WHERE _row_created_at_version = 4 "
                + "ORDER BY id",
            helper.getTableName());

    List<CdfRow> newRows = helper.query(sqlNewRows);
    assertEquals(1, newRows.size());
    assertEquals(CdfRow.ofWithVersions(3, "Charlie", 300, 4L, 4L), newRows.get(0));

    // Query: Find rows updated in v3 (modifications only, not new inserts)
    String sqlUpdatedRows =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s "
                + "WHERE _row_last_updated_at_version = 3 "
                + "  AND _row_created_at_version != 3 "
                + "ORDER BY id",
            helper.getTableName());

    List<CdfRow> updatedRows = helper.query(sqlUpdatedRows);
    assertEquals(1, updatedRows.size());
    assertEquals(CdfRow.ofWithVersions(1, "Alice", 110, 1L, 3L), updatedRows.get(0));
  }

  @Test
  public void testTrackMostRecentlyModifiedRows() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100),
            CdfRow.of(2, "Bob", 200),
            CdfRow.of(3, "Charlie", 300),
            CdfRow.of(4, "David", 400)));

    // v3: Update some rows
    helper.update("value = value + 1", "id IN (1, 3)");

    // v4: Update different rows
    helper.update("value = value + 1", "id IN (2, 4)");

    // v5: Update one row
    helper.update("value = value + 1", "id = 1");

    // Query all rows with version info and verify ordering in Java
    // (avoids ORDER BY on metadata columns which may not be supported in all scan paths)
    List<CdfRow> allRows = helper.getAllWithVersions();

    // Sort by lastUpdatedAtVersion descending, take top 2
    allRows.sort((a, b) -> Long.compare(b.lastUpdatedAtVersion, a.lastUpdatedAtVersion));
    List<CdfRow> mostRecent = allRows.subList(0, 2);

    // Alice was updated in v5
    assertEquals(5L, mostRecent.get(0).lastUpdatedAtVersion.longValue());
    // Bob or David were updated in v4
    assertEquals(4L, mostRecent.get(1).lastUpdatedAtVersion.longValue());
  }
}
