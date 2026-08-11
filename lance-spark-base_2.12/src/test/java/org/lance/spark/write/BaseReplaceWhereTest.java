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

import org.lance.Dataset;
import org.lance.spark.LanceDataset;

import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests for the {@code REPLACE <table> WHERE <predicate> AS <query>} command, which atomically
 * replaces the rows matching the predicate with the result of the query in a single table version.
 */
public abstract class BaseReplaceWhereTest {
  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-replace-where-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", getNsImpl())
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .getOrCreate();

    Map<String, String> additionalConfigs = getAdditionalNsConfigs();
    for (Map.Entry<String, String> entry : additionalConfigs.entrySet()) {
      spark.conf().set("spark.sql.catalog." + catalogName + "." + entry.getKey(), entry.getValue());
    }

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  protected String getNsImpl() {
    return "dir";
  }

  protected Map<String, String> getAdditionalNsConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put("root", tempDir.toString());
    return configs;
  }

  /** Replacing a partition that lives in its own fragment removes it and appends the new rows. */
  @Test
  public void testReplaceSinglePartition() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    // One INSERT per dt → one fragment per dt.
    op.insert(Arrays.asList(Row.of(1, "2026-08-01", 100), Row.of(2, "2026-08-01", 200)));
    op.insert(Arrays.asList(Row.of(3, "2026-08-02", 300)));

    op.replace("dt = '2026-08-01'", "SELECT 10 AS id, '2026-08-01' AS dt, 999 AS value");

    op.check(Arrays.asList(Row.of(3, "2026-08-02", 300), Row.of(10, "2026-08-01", 999)));
  }

  /** REPLACE must not touch partitions outside the predicate. */
  @Test
  public void testReplaceLeavesOtherPartitionsUntouched() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(Arrays.asList(Row.of(1, "2026-08-01", 100)));
    op.insert(Arrays.asList(Row.of(2, "2026-08-02", 200)));
    op.insert(Arrays.asList(Row.of(3, "2026-08-03", 300)));

    op.replace("dt = '2026-08-02'", "SELECT 20 AS id, '2026-08-02' AS dt, 222 AS value");

    op.check(
        Arrays.asList(
            Row.of(1, "2026-08-01", 100),
            Row.of(3, "2026-08-03", 300),
            Row.of(20, "2026-08-02", 222)));
  }

  /**
   * When a single fragment straddles the predicate boundary (holds both matching and non-matching
   * rows), only the matching rows are removed; the rest survive via a deletion vector.
   */
  @Test
  public void testReplacePartiallyMatchingFragment() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    // A single INSERT → single fragment containing two different dt values.
    op.insert(
        Arrays.asList(
            Row.of(1, "2026-08-01", 100),
            Row.of(2, "2026-08-02", 200),
            Row.of(3, "2026-08-01", 300)));

    op.replace("dt = '2026-08-01'", "SELECT 9 AS id, '2026-08-01' AS dt, 900 AS value");

    op.check(Arrays.asList(Row.of(2, "2026-08-02", 200), Row.of(9, "2026-08-01", 900)));
  }

  /** Replacing a partition that has no existing rows is a plain append. */
  @Test
  public void testReplaceNonExistingPartitionAppends() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(Arrays.asList(Row.of(1, "2026-08-01", 100)));

    op.replace("dt = '2026-08-09'", "SELECT 5 AS id, '2026-08-09' AS dt, 500 AS value");

    op.check(Arrays.asList(Row.of(1, "2026-08-01", 100), Row.of(5, "2026-08-09", 500)));
  }

  /**
   * A predicate may itself contain {@code AS} (e.g. inside a {@code CAST}); the command must split
   * on the top-level {@code AS} separator, not the first {@code AS} token.
   */
  @Test
  public void testReplacePredicateWithCast() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(Arrays.asList(Row.of(1, "2026-08-01", 100), Row.of(2, "2026-08-02", 200)));

    op.replace(
        "CAST(dt AS STRING) = '2026-08-01'", "SELECT 3 AS id, '2026-08-01' AS dt, 300 AS value");

    op.check(Arrays.asList(Row.of(2, "2026-08-02", 200), Row.of(3, "2026-08-01", 300)));
  }

  /** The replacement is a single atomic commit: exactly one new table version is produced. */
  @Test
  public void testReplaceIsSingleAtomicCommit() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(Arrays.asList(Row.of(1, "2026-08-01", 100)));
    long versionBefore = op.latestVersion();

    op.replace("dt = '2026-08-01'", "SELECT 2 AS id, '2026-08-01' AS dt, 200 AS value");

    Assertions.assertEquals(
        versionBefore + 1,
        op.latestVersion(),
        "REPLACE ... WHERE must bump the table version exactly once (atomic delete + append)");
    op.check(Arrays.asList(Row.of(2, "2026-08-01", 200)));
  }

  private class TableOperator {
    private final SparkSession spark;
    private final String catalogName;
    private final String tableName;

    TableOperator(SparkSession spark, String catalogName) {
      this.spark = spark;
      this.catalogName = catalogName;
      this.tableName = "replace_test_" + UUID.randomUUID().toString().replace("-", "");
    }

    String fullName() {
      return catalogName + ".default." + tableName;
    }

    void create() {
      spark.sql("CREATE TABLE " + fullName() + " (id INT NOT NULL, dt STRING, value INT)");
    }

    void insert(List<Row> rows) {
      spark.sql(
          String.format(
              "INSERT INTO %s VALUES %s",
              fullName(), rows.stream().map(Row::insertSql).collect(Collectors.joining(", "))));
    }

    void replace(String predicate, String query) {
      spark.sql(String.format("REPLACE %s WHERE %s AS %s", fullName(), predicate, query));
    }

    long latestVersion() {
      // Resolve the table's dataset URI through the catalog, then open it to read the current
      // manifest version. This mirrors how other connector tests read a table's version.
      try {
        String datasetUri =
            ((LanceDataset)
                    ((TableCatalog) spark.sessionState().catalogManager().catalog(catalogName))
                        .loadTable(Identifier.of(new String[] {"default"}, tableName)))
                .readOptions()
                .getDatasetUri();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open(datasetUri, allocator)) {
          return dataset.version();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    void check(List<Row> expected) {
      List<Row> actual =
          spark
              .sql("SELECT id, dt, value FROM " + fullName() + " ORDER BY id")
              .collectAsList()
              .stream()
              .map(row -> Row.of(row.getInt(0), row.getString(1), row.getInt(2)))
              .collect(Collectors.toList());
      Assertions.assertEquals(expected, actual);
    }
  }

  private static class Row {
    int id;
    String dt;
    int value;

    static Row of(int id, String dt, int value) {
      Row row = new Row();
      row.id = id;
      row.dt = dt;
      row.value = value;
      return row;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Row row = (Row) o;
      return id == row.id && value == row.value && Objects.equals(dt, row.dt);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, dt, value);
    }

    @Override
    public String toString() {
      return String.format("Row(id=%s, dt=%s, value=%s)", id, dt, value);
    }

    private String insertSql() {
      return String.format("(%d, '%s', %d)", id, dt, value);
    }
  }
}
