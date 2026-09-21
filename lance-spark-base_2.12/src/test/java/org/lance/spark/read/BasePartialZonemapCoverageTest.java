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

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** End-to-end reads against a zonemap index that does not cover every fragment. */
public abstract class BasePartialZonemapCoverageTest {
  protected String catalogName = "lance_partial_zonemap_test";
  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    spark =
        SparkSession.builder()
            .appName("lance-partial-zonemap-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", rootPath.toString())
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  /** Each INSERT lands in its own fragment, so dates map cleanly onto fragment boundaries. */
  private static final String[] INDEXED_DATES = {"2026-02-14", "2026-02-15", "2026-02-16"};

  private static final String[] UNINDEXED_DATES = {"2026-02-17", "2026-02-18"};
  private static final int ROWS_PER_DATE = 4;

  private String createPartiallyIndexedTable() {
    String table = catalogName + ".default.pzm_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(String.format("CREATE TABLE %s (id INT, ingestion_date STRING) USING lance", table));

    for (String date : INDEXED_DATES) {
      insertDate(table, date);
    }
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX ingestion_date_idx USING zonemap (ingestion_date)",
            table));
    for (String date : UNINDEXED_DATES) {
      insertDate(table, date);
    }
    return table;
  }

  private void insertDate(String table, String date) {
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < ROWS_PER_DATE; i++) {
      if (i > 0) {
        values.append(",");
      }
      values.append(String.format("(%d,'%s')", i, date));
    }
    spark.sql(String.format("INSERT INTO %s VALUES %s", table, values));
  }

  private long filteredCount(String table, String date) {
    return spark
        .sql(String.format("SELECT * FROM %s WHERE ingestion_date = '%s'", table, date))
        .count();
  }

  @Test
  public void testFilterOnUnindexedDateReturnsItsRows() {
    String table = createPartiallyIndexedTable();

    for (String date : UNINDEXED_DATES) {
      assertEquals(
          ROWS_PER_DATE,
          filteredCount(table, date),
          "date '" + date + "' was appended after the index build and must still be readable");
    }
  }

  @Test
  public void testFilterOnIndexedDateStillReturnsItsRows() {
    String table = createPartiallyIndexedTable();

    for (String date : INDEXED_DATES) {
      assertEquals(ROWS_PER_DATE, filteredCount(table, date), "date '" + date + "' is indexed");
    }
  }

  @Test
  public void testFilteredCountsSumToTableTotal() {
    String table = createPartiallyIndexedTable();
    long expectedTotal = (long) (INDEXED_DATES.length + UNINDEXED_DATES.length) * ROWS_PER_DATE;
    assertEquals(expectedTotal, spark.sql("SELECT * FROM " + table).count());

    long summed = 0;
    for (String date : INDEXED_DATES) {
      summed += filteredCount(table, date);
    }
    for (String date : UNINDEXED_DATES) {
      summed += filteredCount(table, date);
    }
    assertEquals(expectedTotal, summed, "per-date counts must reconcile with the full scan");
  }

  @Test
  public void testRangeFilterSpanningIndexedAndUnindexedDates() {
    String table = createPartiallyIndexedTable();

    long count =
        spark
            .sql(
                String.format(
                    "SELECT * FROM %s WHERE ingestion_date >= '2026-02-16'"
                        + " AND ingestion_date <= '2026-02-18'",
                    table))
            .count();

    assertEquals(3L * ROWS_PER_DATE, count);
  }

  @Test
  public void testFilterOnDateAbsentEverywhereReturnsNothing() {
    // Pruning must still happen: a value in no fragment returns nothing.
    String table = createPartiallyIndexedTable();
    assertEquals(0, filteredCount(table, "2025-01-01"));
  }

  @Test
  public void testFullyIndexedTableStillPrunesCorrectly() {
    String table = catalogName + ".default.fzm_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(String.format("CREATE TABLE %s (id INT, ingestion_date STRING) USING lance", table));
    for (String date : INDEXED_DATES) {
      insertDate(table, date);
    }
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX ingestion_date_idx USING zonemap (ingestion_date)",
            table));

    for (String date : INDEXED_DATES) {
      assertEquals(ROWS_PER_DATE, filteredCount(table, date));
    }
    assertEquals(0, filteredCount(table, "2025-01-01"));
  }

  @Test
  public void testFilterOnTwoColumnsWithDifferentIndexCoverage() {
    String table = catalogName + ".default.mzm_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(
        String.format("CREATE TABLE %s (region STRING, ingestion_date STRING) USING lance", table));

    spark.sql(String.format("INSERT INTO %s VALUES ('east','2026-02-14')", table));
    // region's index covers only the fragment written so far.
    spark.sql(
        String.format("ALTER TABLE %s CREATE INDEX region_idx USING zonemap (region)", table));

    spark.sql(String.format("INSERT INTO %s VALUES ('west','2026-02-15')", table));
    // ingestion_date's index is built later, so it covers both fragments.
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX ingestion_date_idx USING zonemap (ingestion_date)",
            table));

    assertEquals(
        1,
        spark.sql(String.format("SELECT * FROM %s WHERE region = 'west'", table)).count(),
        "single-column filter on the row outside region's index");

    assertEquals(
        1,
        spark
            .sql(
                String.format(
                    "SELECT * FROM %s WHERE region = 'west' AND ingestion_date = '2026-02-15'",
                    table))
            .count(),
        "fragment 1 is outside region_idx, so the region predicate cannot rule it out");
  }

  @Test
  public void testTwoZonemapIndexesOnTheSameColumn() {
    String table = catalogName + ".default.szm_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(String.format("CREATE TABLE %s (id INT, x INT) USING lance", table));

    spark.sql(String.format("INSERT INTO %s VALUES (0, 0)", table));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX a_partial USING zonemap (x)", table));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 100)", table));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX z_full USING zonemap (x)", table));

    assertEquals(
        1,
        spark.sql(String.format("SELECT * FROM %s WHERE x = 100", table)).count(),
        "fragment 1 is absent from the loaded zones, so it must stay in the scan");
  }
}
