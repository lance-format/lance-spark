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
package org.lance.spark;

import org.lance.spark.utils.Utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public abstract class BaseDropColumnTest {
  private static final String CATALOG = "lance_ns";
  private static final String DROP_ALL_COLUMNS =
      "Cannot drop all columns from a Lance table. At least one column must remain.";

  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("drop-column-test")
            .master("local[1]")
            .config("spark.sql.catalog." + CATALOG, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testDropColumnRemovesFieldDataAndEncodingProperty() throws Exception {
    createTable("kept", "id INT, name STRING");
    spark.sql("INSERT INTO " + full("kept") + " VALUES (1, 'alpha')");
    setProperties("kept", "'name.lance.compression' = 'zstd', 'team' = 'search'");
    spark.sql("ALTER TABLE " + full("kept") + " DROP COLUMN name");
    refresh("kept");

    List<Row> rows = spark.sql("SELECT * FROM " + full("kept")).collectAsList();
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(List.of("id"), fieldNames("kept"));
    assertEquals("search", config("kept").get("team"));
    assertFalse(config("kept").containsKey("name.lance.compression"));
  }

  @Test
  public void testDropTwoColumnsRemovesBothEncodingProperties() throws Exception {
    createTable("both", "id INT, name STRING, payload STRING");
    spark.sql("INSERT INTO " + full("both") + " VALUES (1, 'alpha', 'blob')");
    setProperties("both", "'name.lance.compression' = 'zstd', 'payload.lance.encoding' = 'blob'");
    spark.sql("ALTER TABLE " + full("both") + " DROP COLUMNS name, payload");
    refresh("both");

    List<Row> rows = spark.sql("SELECT id FROM " + full("both")).collectAsList();
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(List.of("id"), fieldNames("both"));
    Map<String, String> config = config("both");
    assertFalse(config.containsKey("name.lance.compression"));
    assertFalse(config.containsKey("payload.lance.encoding"));
  }

  @Test
  public void testDropStructRemovesChildEncodingProperty() throws Exception {
    createTable("nested", "id INT, s STRUCT<d: INT>");
    setProperties("nested", "'s.d.lance.compression' = 'zstd', 'team' = 'search'");
    spark.sql("ALTER TABLE " + full("nested") + " DROP COLUMN s");
    refresh("nested");

    assertEquals(List.of("id"), fieldNames("nested"));
    Map<String, String> config = config("nested");
    assertFalse(config.containsKey("s.d.lance.compression"));
    assertEquals("search", config.get("team"));
  }

  @Test
  public void testDropMissingColumnLeavesTableUnchanged() throws Exception {
    createTable("missing", "id INT, name STRING");
    setProperties("missing", "'name.lance.compression' = 'zstd'");
    Map<String, String> before = config("missing");
    assertSqlFails(() -> spark.sql("ALTER TABLE " + full("missing") + " DROP COLUMN absent"));
    refresh("missing");
    assertEquals(List.of("id", "name"), fieldNames("missing"));
    assertEquals(before, config("missing"));
  }

  @Test
  public void testDropMissingColumnIfExistsKeepsFutureEncodingProperty() throws Exception {
    createTable("future", "id INT, name STRING");
    setProperties("future", "'content.lance.encoding' = 'blob'");
    spark.sql("ALTER TABLE " + full("future") + " DROP COLUMN IF EXISTS content");
    refresh("future");
    assertEquals(List.of("id", "name"), fieldNames("future"));
    assertEquals("blob", config("future").get("content.lance.encoding"));
  }

  @Test
  public void testDropLastColumnReportsReason() throws Exception {
    createTable("only", "id INT");
    spark.sql("INSERT INTO " + full("only") + " VALUES (1)");
    Map<String, String> before = config("only");
    assertSqlFails(
        () -> spark.sql("ALTER TABLE " + full("only") + " DROP COLUMN id"), DROP_ALL_COLUMNS);
    refresh("only");
    assertEquals(List.of("id"), fieldNames("only"));
    assertEquals(before, config("only"));
    assertEquals(1, spark.sql("SELECT id FROM " + full("only")).collectAsList().get(0).getInt(0));
  }

  @Test
  public void testDropEveryRemainingColumnReportsReason() throws Exception {
    createTable("all", "id INT, name STRING");
    Map<String, String> before = config("all");
    assertSqlFails(
        () -> spark.sql("ALTER TABLE " + full("all") + " DROP COLUMNS id, name"), DROP_ALL_COLUMNS);
    refresh("all");
    assertEquals(List.of("id", "name"), fieldNames("all"));
    assertEquals(before, config("all"));
  }

  @Test
  public void testDropIndexedColumnIsRejected() throws Exception {
    createTable("indexed", "id INT, region STRING");
    spark.sql("INSERT INTO " + full("indexed") + " VALUES (1, 'east')");
    setProperties("indexed", "'region.lance.compression' = 'zstd'");
    spark.sql("ALTER TABLE " + full("indexed") + " CREATE INDEX region_idx USING btree (region)");
    Map<String, String> before = config("indexed");
    assertSqlFails(
        () -> spark.sql("ALTER TABLE " + full("indexed") + " DROP COLUMN region"),
        "region_idx",
        "region");
    refresh("indexed");
    assertEquals(List.of("id", "region"), fieldNames("indexed"));
    assertEquals(before, config("indexed"));
    assertTrue(indexNames("indexed").contains("region_idx"));
  }

  @Test
  public void testDropQuotedFieldDoesNotRemoveNestedPath() throws Exception {
    createTable("collision", "id INT, s STRUCT<`a.b`: INT, a: STRUCT<b: INT>, keep: INT>");
    spark.sql(
        "INSERT INTO "
            + full("collision")
            + " VALUES (1, named_struct('a.b', 11, 'a', named_struct('b', 22), 'keep', 33))");
    setProperties(
        "collision",
        "'s.`a.b`.lance.compression' = 'zstd', 's.a.b.lance.compression' = 'lz4', 'team' = 'search'");
    spark.sql("ALTER TABLE " + full("collision") + " DROP COLUMN s.`a.b`");
    refresh("collision");

    Row row =
        spark
            .sql("SELECT s.a.b AS nested, s.keep AS keep FROM " + full("collision"))
            .collectAsList()
            .get(0);
    assertEquals(22, row.getInt(0));
    assertEquals(33, row.getInt(1));
    StructType struct = (StructType) spark.table(full("collision")).schema().apply("s").dataType();
    assertFalse(Arrays.asList(struct.fieldNames()).contains("a.b"));
    assertTrue(Arrays.asList(struct.fieldNames()).contains("a"));
    Map<String, String> config = config("collision");
    assertFalse(config.containsKey("s.`a.b`.lance.compression"));
    assertEquals("lz4", config.get("s.a.b.lance.compression"));
    assertEquals("search", config.get("team"));
  }

  @Test
  public void testDropPrimaryKeyColumnIsRejected() throws Exception {
    createTable("pk", "id INT NOT NULL, other INT NOT NULL");
    spark.sql("INSERT INTO " + full("pk") + " VALUES (1, 2)");
    spark.sql("ALTER TABLE " + full("pk") + " SET UNENFORCED PRIMARY KEY (id)");
    setProperties("pk", "'id.lance.compression' = 'zstd'");
    Map<String, String> before = config("pk");
    assertSqlFails(
        () -> spark.sql("ALTER TABLE " + full("pk") + " DROP COLUMN id"), "unenforced primary key");
    refresh("pk");
    assertEquals(List.of("id", "other"), fieldNames("pk"));
    assertEquals(before, config("pk"));
    assertEquals(1, spark.sql("SELECT id FROM " + full("pk")).collectAsList().get(0).getInt(0));
    assertSqlFails(
        () -> spark.sql("ALTER TABLE " + full("pk") + " SET UNENFORCED PRIMARY KEY (other)"),
        "already has unenforced primary key");
  }

  private void createTable(String name, String columns) {
    spark.sql("CREATE TABLE " + full(name) + " (" + columns + ") USING lance");
  }

  private void setProperties(String name, String properties) {
    spark.sql("ALTER TABLE " + full(name) + " SET TBLPROPERTIES (" + properties + ")");
  }

  private void refresh(String name) {
    spark.sql("REFRESH TABLE " + full(name));
  }

  private String full(String name) {
    return CATALOG + ".default." + name;
  }

  private List<String> fieldNames(String name) {
    return Arrays.asList(spark.table(full(name)).schema().fieldNames());
  }

  private Map<String, String> config(String name) throws Exception {
    TableCatalog catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG);
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, name));
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      return new HashMap<>(dataset.getConfig());
    }
  }

  private List<String> indexNames(String name) throws Exception {
    TableCatalog catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG);
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, name));
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      List<String> names = new ArrayList<>();
      dataset.getIndexes().forEach(index -> names.add(index.name()));
      return names;
    }
  }

  private static void assertSqlFails(Executable statement, String... fragments) {
    Exception error = assertThrows(Exception.class, statement);
    String message = messages(error);
    for (String fragment : fragments) {
      assertTrue(message.contains(fragment), message);
    }
  }

  private static String messages(Throwable error) {
    StringBuilder message = new StringBuilder();
    while (error != null) {
      if (error.getMessage() != null) {
        message.append(error.getMessage()).append('\n');
      }
      error = error.getCause();
    }
    return message.toString();
  }
}
