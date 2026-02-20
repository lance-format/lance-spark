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

public class DeleteTableTest {
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
