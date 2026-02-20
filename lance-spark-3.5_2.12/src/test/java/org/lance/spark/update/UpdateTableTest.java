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

public class UpdateTableTest {
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
  public void testUpdateNoRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateBasicValue("value > 400");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateSomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateBasicValue("value >= 200");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 201, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 301, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateBasicValue("value > 0");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 101, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 201, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 301, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateMultipleTimes() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    // Update one row
    op.updateBasicValue("value = 100");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 101, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    // Update the same row multiple times
    op.updateBasicValue("value = 101");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 102, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    // Update other row
    op.updateBasicValue("value = 200");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 102, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 201, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateBasicValue("value >= 200");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 102, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 202, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 301, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateOnMultiFragments() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.insert(
        Arrays.asList(
            Row.of(4, "Tom", 100, "Tom", 100, Arrays.asList(100, 101)),
            Row.of(5, "Frank", 200, "Frank", 200, Arrays.asList(200, 201))));

    op.insert(
        Collections.singletonList(Row.of(6, "Penny", 200, "Penny", 200, Arrays.asList(200, 201))));

    op.updateBasicValue("value = 200");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 201, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301)),
            Row.of(4, "Tom", 100, "Tom", 100, Arrays.asList(100, 101)),
            Row.of(5, "Frank", 201, "Frank", 200, Arrays.asList(200, 201)),
            Row.of(6, "Penny", 201, "Penny", 200, Arrays.asList(200, 201))));
  }

  @Test
  public void testUpdateDeletedRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    // Delete some rows
    op.delete("id = 1 or id = 2");
    op.check(
        Collections.singletonList(
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    // Update all rows
    op.updateBasicValue("value > 0");
    op.check(
        Collections.singletonList(
            Row.of(3, "Charlie", 301, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateWholeStructSomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateStruct("AliceNew", 101, "id = 1");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "AliceNew", 101, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateWholeStructAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateStruct("New", 0, "id > 0");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "New", 0, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "New", 0, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "New", 0, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateChildSomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateStructValue(101, "id = 1");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 101, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateChildAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateStructValue(0, "id > 0");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 0, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 0, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 0, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateArraySomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateArray(Arrays.asList(100, 200), "id = 1 or id = 2");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 200)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(100, 200)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testUpdateArrayAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateArray(Arrays.asList(100, 200), "id > 0");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 200)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(100, 200)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(100, 200))));
  }

  @Test
  public void testTransformArraySomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.transformArray("transform(values, x -> x + 1)", "id = 1 or id = 2");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(101, 102)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(201, 202)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testTransformArrayAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 102)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.transformArray("transform(values, x -> x + 1)", "id > 0");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(101, 103)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(201, 202)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(301, 302))));
  }

  private static class TableOperator {
    private final SparkSession spark;
    private final String catalogName;
    private final String tableName;

    public TableOperator(SparkSession spark, String catalogName) {
      this.spark = spark;
      this.catalogName = catalogName;
      String baseName = "unified_test_table";
      this.tableName = baseName + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public void create() {
      spark.sql(
          "CREATE TABLE "
              + catalogName
              + ".default."
              + tableName
              + " (id INT NOT NULL, name STRING, value INT, meta STRUCT<name: STRING, value: INT>, values ARRAY<INT>)");
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

    public void updateBasicValue(String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set value = value + 1 where %s",
              catalogName, tableName, condition);
      spark.sql(sql);
    }

    public void updateStructValue(int value, String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set meta=named_struct('name', meta.name, 'value', %d) where %s",
              catalogName, tableName, value, condition);
      spark.sql(sql);
    }

    public void updateStruct(String name, int value, String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set meta=named_struct('name', '%s', 'value', %d) where %s",
              catalogName, tableName, name, value, condition);
      spark.sql(sql);
    }

    public void updateArray(List<Integer> values, String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set values=ARRAY(%s) where %s",
              catalogName,
              tableName,
              values.stream().map(String::valueOf).collect(Collectors.joining(",")),
              condition);
      spark.sql(sql);
    }

    public void transformArray(String transform, String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set values=%s where %s",
              catalogName, tableName, transform, condition);
      spark.sql(sql);
    }

    public void check(List<Row> expected) {
      String sql = String.format("Select * from %s.default.%s order by id", catalogName, tableName);
      List<Row> actual =
          spark.sql(sql).collectAsList().stream()
              .map(
                  row ->
                      Row.of(
                          row.getInt(0),
                          row.getString(1),
                          row.getInt(2),
                          row.getStruct(3).getString(0),
                          row.getStruct(3).getInt(1),
                          row.getList(4)))
              .collect(Collectors.toList());
      Assertions.assertEquals(expected, actual);
    }
  }

  private static class Row {
    int id;
    String name;
    int value;
    String metaName;
    int metaValue;
    List<Integer> values;

    private static Row of(
        int id, String name, int value, String metaName, int metaValue, List<Integer> values) {
      Row row = new Row();
      row.id = id;
      row.name = name;
      row.value = value;
      row.metaName = metaName;
      row.metaValue = metaValue;
      row.values = values;
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
      return id == row.id
          && value == row.value
          && metaValue == row.metaValue
          && Objects.equals(name, row.name)
          && Objects.equals(metaName, row.metaName)
          && Objects.deepEquals(values, row.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, value, metaName, metaValue, values);
    }

    @Override
    public String toString() {
      return String.format(
          "Row(id=%s, name=%s, value=%s, metaName=%s, metaValue=%s, values=%s)",
          id, name, value, metaName, metaValue, values);
    }

    private String insertSql() {
      return String.format(
          "(%d, '%s', %d, NAMED_STRUCT('name', '%s', 'value', %d), ARRAY(%s))",
          id,
          name,
          value,
          metaName,
          metaValue,
          values.stream().map(String::valueOf).collect(Collectors.joining(",")));
    }
  }
}
