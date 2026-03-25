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
package org.lance.spark.delete;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class BaseDeleteTableTest {
  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-namespace-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", getNsImpl())
            .getOrCreate();

    Map<String, String> additionalConfigs = getAdditionalNsConfigs();
    for (Map.Entry<String, String> entry : additionalConfigs.entrySet()) {
      spark.conf().set("spark.sql.catalog." + catalogName + "." + entry.getKey(), entry.getValue());
    }

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    // Create default namespace for multi-level namespace mode
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

  @Test
  public void testDeleteNoRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.delete("value > 400");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));
  }

  @Test
  public void testDeleteSomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.delete("value >= 200");
    op.check(Collections.singletonList(Row.of(1, "Alice", 100)));
  }

  @Test
  public void testDeleteAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.delete("value > 0");
    op.check(Collections.emptyList());
  }

  @Test
  public void testDeleteMultipleTimes() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    // Delete one row
    op.delete("value = 100");
    op.check(Arrays.asList(Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    // Delete with same condition
    op.delete("value = 100");
    op.check(Arrays.asList(Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    // Delete other row
    op.delete("value = 200");
    op.check(Collections.singletonList(Row.of(3, "Charlie", 300)));
  }

  @Test
  public void testDeleteOnMultiFragments() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.insert(Arrays.asList(Row.of(4, "Tom", 100), Row.of(5, "Frank", 200)));

    op.insert(Collections.singletonList(Row.of(6, "Penny", 200)));

    op.delete("value = 200");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(3, "Charlie", 300), Row.of(4, "Tom", 100)));
  }

  /**
   * Verifies that DELETE with a {@code _rowaddr} filter only affects the target row (issue #117).
   *
   * <p>Without fragment pruning, lance-core's {@code Fragment.newScan()} incorrectly returns rows
   * from non-matching fragments, causing a {@code DELETE WHERE _rowaddr = X} to delete the entire
   * dataset. This test validates both the correctness fix and the fragment ID mapping CONTRACT used
   * by {@code RowAddressFilterAnalyzer}: {@code LanceSplit.getFragments()} returns Integer values
   * that match {@code (int)(rowAddr >>> 32)}. If lance-core ever changes the {@code _rowaddr}
   * encoding or fragment ID assignment, this test will fail.
   *
   * <p>Setup: 3 separate INSERTs create 3 fragments. We read back all rows with {@code _rowaddr},
   * pick the {@code _rowaddr} of a specific row, delete by that address, and verify only that row
   * is removed while all other rows (including those in other fragments) remain.
   */
  @Test
  public void testDeleteByRowAddrOnlyAffectsTargetFragment() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    // Lance creates one fragment per append operation, so 3 INSERTs → 3 fragments.
    // The assertion below verifies this assumption holds at runtime.
    op.insert(Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200)));
    op.insert(Arrays.asList(Row.of(3, "Charlie", 300), Row.of(4, "Dave", 400)));
    op.insert(Collections.singletonList(Row.of(5, "Eve", 500)));

    // Read back _rowaddr for each row
    String fullTable = catalogName + ".default." + op.tableName;
    List<org.apache.spark.sql.Row> rowsWithAddr =
        spark.sql("SELECT id, _rowaddr FROM " + fullTable + " ORDER BY id").collectAsList();
    Assertions.assertEquals(5, rowsWithAddr.size());

    // Verify that rows span at least 2 distinct fragments (fragment ID = upper 32 bits)
    long distinctFragments =
        rowsWithAddr.stream().map(r -> (int) (r.getLong(1) >>> 32)).distinct().count();
    Assertions.assertTrue(
        distinctFragments >= 2,
        "Expected rows to span at least 2 fragments, but got " + distinctFragments);

    // Pick the _rowaddr of the row with id=1 (in the first fragment) and delete it
    long targetAddr =
        rowsWithAddr.stream()
            .filter(r -> r.getInt(0) == 1)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Row id=1 not found"))
            .getLong(1);
    spark.sql("DELETE FROM " + fullTable + " WHERE _rowaddr = " + targetAddr);

    // Verify: only id=1 is deleted, all other rows (across all fragments) remain
    op.check(
        Arrays.asList(
            Row.of(2, "Bob", 200),
            Row.of(3, "Charlie", 300),
            Row.of(4, "Dave", 400),
            Row.of(5, "Eve", 500)));
  }

  private static class TableOperator {
    private final SparkSession spark;
    private final String catalogName;
    private final String tableName;

    public TableOperator(SparkSession spark, String catalogName) {
      this.spark = spark;

      this.catalogName = catalogName;

      String baseName = "sql_test_table";
      this.tableName = baseName + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public void create() {
      // Create a table using SQL DDL
      spark.sql(
          "CREATE TABLE "
              + catalogName
              + ".default."
              + tableName
              + " (id INT NOT NULL, name STRING, value INT)");
    }

    public void insert(List<Row> rows) {
      String sql =
          String.format(
              "INSERT INTO %s.default.%s VALUES %s",
              catalogName,
              tableName,
              rows.stream().map(Row::insertSql).collect(Collectors.joining(", ")));
      spark.sql(sql);
    }

    public void delete(String condition) {
      String sql =
          String.format("Delete from %s.default.%s where %s", catalogName, tableName, condition);
      spark.sql(sql);
    }

    public void check(List<Row> expected) {
      String sql = String.format("Select * from %s.default.%s order by id", catalogName, tableName);
      List<Row> actual =
          spark.sql(sql).collectAsList().stream()
              .map(row -> Row.of(row.getInt(0), row.getString(1), row.getInt(2)))
              .collect(Collectors.toList());
      Assertions.assertEquals(expected, actual);
    }
  }

  private static class Row {
    int id;
    String name;
    int value;

    private static Row of(int id, String name, int value) {
      Row row = new Row();
      row.id = id;
      row.name = name;
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
      return id == row.id && value == row.value && Objects.equals(name, row.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, value);
    }

    @Override
    public String toString() {
      return String.format("Row(id=%s, name=%s, value=%s)", id, name, value);
    }

    private String insertSql() {
      return String.format("(%d, '%s', %d)", id, name, value);
    }
  }
}
