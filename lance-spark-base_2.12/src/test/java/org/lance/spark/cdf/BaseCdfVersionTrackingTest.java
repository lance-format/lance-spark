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

/**
 * Tests for Change Data Feed (CDF) version tracking columns: _row_created_at_version and
 * _row_last_updated_at_version.
 *
 * <p>Version numbering: CREATE TABLE = v1 (empty), first INSERT = v2, etc. After any row-level
 * operation (UPDATE/DELETE), rows in affected fragments get their version metadata recalculated by
 * the Lance engine.
 */
public abstract class BaseCdfVersionTrackingTest {
  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-cdf-test")
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
  public void testInsertSetsBothVersionColumns() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert (v1 = CREATE TABLE)
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    helper.checkWithVersions(
        Arrays.asList(
            CdfRow.ofWithVersions(1, "Alice", 100, 2L, 2L),
            CdfRow.ofWithVersions(2, "Bob", 200, 2L, 2L),
            CdfRow.ofWithVersions(3, "Charlie", 300, 2L, 2L)));
  }

  @Test
  public void testUpdateIncrementsLastUpdatedVersion() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(Arrays.asList(CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200)));

    // v3: Update one row
    helper.update("value = value + 10", "id = 1");

    helper.checkWithVersions(
        Arrays.asList(
            CdfRow.ofWithVersions(1, "Alice", 110, 1L, 3L),
            CdfRow.ofWithVersions(2, "Bob", 200, 1L, 1L)));
  }

  @Test
  public void testMultipleUpdatesIncrementLastUpdatedVersion() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(Collections.singletonList(CdfRow.of(1, "Alice", 100)));

    // v3: First update
    helper.update("value = value + 10", "id = 1");
    helper.checkWithVersions(
        Collections.singletonList(CdfRow.ofWithVersions(1, "Alice", 110, 1L, 3L)));

    // v4: Second update on same row
    helper.update("value = value + 10", "id = 1");
    helper.checkWithVersions(
        Collections.singletonList(CdfRow.ofWithVersions(1, "Alice", 120, 1L, 4L)));

    // v5: Third update on same row
    helper.update("value = value + 10", "id = 1");
    helper.checkWithVersions(
        Collections.singletonList(CdfRow.ofWithVersions(1, "Alice", 130, 1L, 5L)));
  }

  @Test
  public void testPartialUpdateOnlyAffectsUpdatedRows() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    // v3: Update only rows where value >= 200
    helper.update("value = value + 1", "value >= 200");

    helper.checkWithVersions(
        Arrays.asList(
            CdfRow.ofWithVersions(1, "Alice", 100, 1L, 1L),
            CdfRow.ofWithVersions(2, "Bob", 201, 1L, 3L),
            CdfRow.ofWithVersions(3, "Charlie", 301, 1L, 3L)));
  }

  @Test
  public void testInsertIntoExistingTableUsesCurrentVersion() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(Collections.singletonList(CdfRow.of(1, "Alice", 100)));

    // v3: Insert more rows
    helper.insert(Arrays.asList(CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    helper.checkWithVersions(
        Arrays.asList(
            CdfRow.ofWithVersions(1, "Alice", 100, 2L, 2L),
            CdfRow.ofWithVersions(2, "Bob", 200, 3L, 3L),
            CdfRow.ofWithVersions(3, "Charlie", 300, 3L, 3L)));
  }

  @Test
  public void testDeleteAffectsVersionColumnsInSameFragment() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert (all rows in same fragment)
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    // v3: Delete one row - remaining rows in the same fragment get version metadata recalculated
    helper.delete("id = 2");

    helper.checkWithVersions(
        Arrays.asList(
            CdfRow.ofWithVersions(1, "Alice", 100, 1L, 1L),
            CdfRow.ofWithVersions(3, "Charlie", 300, 1L, 1L)));
  }

  @Test
  public void testComplexWorkflowWithMultipleOperations() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(Arrays.asList(CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200)));

    // v3: Update Alice += 50 -> Alice(150)
    helper.update("value = value + 50", "id = 1");

    // v4: Insert new row
    helper.insert(Collections.singletonList(CdfRow.of(3, "Charlie", 300)));

    // v5: Update rows where value >= 200 -> Bob(300), Charlie(400); Alice(150) unaffected
    helper.update("value = value + 100", "value >= 200");

    // v6: Delete Bob
    helper.delete("id = 2");

    // v7: Update remaining rows += 1
    helper.update("value = value + 1", "id IN (1, 3)");

    helper.checkWithVersions(
        Arrays.asList(
            CdfRow.ofWithVersions(1, "Alice", 151, 1L, 7L),
            CdfRow.ofWithVersions(3, "Charlie", 401, 1L, 7L)));
  }

  @Test
  public void testUpdateAfterDelete() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Initial insert
    helper.insert(
        Arrays.asList(
            CdfRow.of(1, "Alice", 100), CdfRow.of(2, "Bob", 200), CdfRow.of(3, "Charlie", 300)));

    // v3: Delete middle row
    helper.delete("id = 2");

    // v4: Update remaining rows
    helper.update("value = value * 2", "id IN (1, 3)");

    helper.checkWithVersions(
        Arrays.asList(
            CdfRow.ofWithVersions(1, "Alice", 200, 1L, 4L),
            CdfRow.ofWithVersions(3, "Charlie", 600, 1L, 4L)));
  }

  @Test
  public void testInsertUpdateDeleteInsert() {
    CdfTestHelper helper = new CdfTestHelper(spark, catalogName);
    helper.create();

    // v2: Insert
    helper.insert(Collections.singletonList(CdfRow.of(1, "Alice", 100)));

    // v3: Update
    helper.update("value = 150", "id = 1");

    // v4: Delete
    helper.delete("id = 1");

    // v5: Insert new row with same id
    helper.insert(Collections.singletonList(CdfRow.of(1, "Alice", 200)));

    // Should be a new row with versions from insert time
    helper.checkWithVersions(
        Collections.singletonList(CdfRow.ofWithVersions(1, "Alice", 200, 5L, 5L)));
  }
}
