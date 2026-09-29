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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BaseLanceNamespaceSparkSessionCatalog} — validates that the session catalog
 * correctly routes operations between Lance and the delegate catalog based on provider.
 */
public abstract class BaseTestSparkSessionCatalog {
  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "spark_catalog";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-session-catalog-test")
            .master("local")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.lance.spark.LanceNamespaceSparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.impl", "dir")
            .config("spark.sql.catalog.spark_catalog.root", tempDir.toString())
            .config("spark.sql.catalog.spark_catalog.single_level_ns", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .getOrCreate();

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.stop();
    }
  }

  protected String generateTableName(String baseName) {
    return baseName + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  // ---------------------------------------------------------------------------
  // Basic routing: Lance tables
  // ---------------------------------------------------------------------------

  @Test
  public void testCreateLanceTableExplicitly() {
    String tableName = generateTableName("lance_explicit");
    spark.sql("CREATE TABLE default." + tableName + " (id BIGINT, name STRING) USING lance");

    assertTrue(
        catalog.tableExists(Identifier.of(new String[] {"default"}, tableName)),
        "Lance table should exist after creation");

    Dataset<Row> result = spark.sql("SELECT * FROM default." + tableName);
    assertEquals(0, result.count());
  }

  @Test
  public void testInsertAndSelectLanceTable() {
    String tableName = generateTableName("lance_insert");
    spark.sql("CREATE TABLE default." + tableName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO default." + tableName + " VALUES (1, 'alice'), (2, 'bob')");

    Dataset<Row> result = spark.sql("SELECT * FROM default." + tableName + " ORDER BY id");
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals("alice", rows.get(0).getString(1));
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals("bob", rows.get(1).getString(1));
  }

  @Test
  public void testDropLanceTableCatalogApi() throws Exception {
    String tableName = generateTableName("lance_drop");
    Identifier ident = Identifier.of(new String[] {"default"}, tableName);

    spark.sql("CREATE TABLE default." + tableName + " (id BIGINT) USING lance");
    assertTrue(catalog.tableExists(ident), "Table should exist after creation");

    boolean dropped = catalog.dropTable(ident);
    assertTrue(dropped, "dropTable should return true for an existing Lance table");
  }

  // ---------------------------------------------------------------------------
  // Non-Lance table routing
  // ---------------------------------------------------------------------------

  @Test
  public void testCreateParquetTableRoutesToDelegate() {
    String tableName = generateTableName("parquet_tbl");
    spark.sql("CREATE TABLE default." + tableName + " (id BIGINT) USING parquet");

    assertTrue(
        catalog.tableExists(Identifier.of(new String[] {"default"}, tableName)),
        "Parquet table should be visible through session catalog");

    Dataset<Row> result = spark.sql("SELECT * FROM default." + tableName);
    assertEquals(0, result.count());
  }

  // ---------------------------------------------------------------------------
  // Config validation
  // ---------------------------------------------------------------------------

  @Test
  public void testInvalidDefaultProviderRejected() {
    BaseLanceNamespaceSparkSessionCatalog sessionCat =
        (BaseLanceNamespaceSparkSessionCatalog) catalog;

    Map<String, String> badOptions = new HashMap<>();
    badOptions.put("impl", "dir");
    badOptions.put("root", tempDir.toString());
    badOptions.put("default-provider", "invalid_value");

    assertThrows(
        IllegalArgumentException.class,
        () -> sessionCat.initialize("test_catalog", new CaseInsensitiveStringMap(badOptions)),
        "Should reject invalid default-provider value");
  }

  // ---------------------------------------------------------------------------
  // CTAS (Create Table As Select) through staging path
  // ---------------------------------------------------------------------------

  @Test
  public void testCTASWithLanceProvider() {
    String srcTable = generateTableName("ctas_src");
    String dstTable = generateTableName("ctas_dst");

    spark.sql("CREATE TABLE default." + srcTable + " (id BIGINT, val STRING) USING lance");
    spark.sql("INSERT INTO default." + srcTable + " VALUES (1, 'a'), (2, 'b')");

    spark.sql(
        "CREATE TABLE default." + dstTable + " USING lance AS SELECT * FROM default." + srcTable);

    Dataset<Row> result = spark.sql("SELECT * FROM default." + dstTable + " ORDER BY id");
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
  }

  // ---------------------------------------------------------------------------
  // Multiple tables coexistence
  // ---------------------------------------------------------------------------

  @Test
  public void testLanceAndParquetTablesCoexist() {
    String lanceTable = generateTableName("lance_coexist");
    String parquetTable = generateTableName("parquet_coexist");

    spark.sql("CREATE TABLE default." + lanceTable + " (id BIGINT, val STRING) USING lance");
    spark.sql("CREATE TABLE default." + parquetTable + " (id BIGINT, val STRING) USING parquet");

    spark.sql("INSERT INTO default." + lanceTable + " VALUES (1, 'lance')");
    spark.sql("INSERT INTO default." + parquetTable + " VALUES (2, 'parquet')");

    List<Row> lanceRows = spark.sql("SELECT * FROM default." + lanceTable).collectAsList();
    List<Row> parquetRows = spark.sql("SELECT * FROM default." + parquetTable).collectAsList();

    assertEquals(1, lanceRows.size());
    assertEquals("lance", lanceRows.get(0).getString(1));
    assertEquals(1, parquetRows.size());
    assertEquals("parquet", parquetRows.get(0).getString(1));
  }

  // ---------------------------------------------------------------------------
  // SQL Extensions: OPTIMIZE (blocking requirement)
  // ---------------------------------------------------------------------------

  @Test
  public void testOptimizeLanceTableThroughSessionCatalog() {
    String tableName = generateTableName("optimize");
    String fullTable = "default." + tableName;

    spark.sql("CREATE TABLE " + fullTable + " (id INT, text STRING) USING lance");
    spark.sql(
        String.format(
            "INSERT INTO %s (id, text) VALUES %s",
            fullTable,
            IntStream.range(0, 5)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));

    // OPTIMIZE should work through session catalog
    Dataset<Row> result =
        spark.sql("OPTIMIZE " + fullTable + " WITH (target_rows_per_fragment=20000)");
    assertNotNull(result, "OPTIMIZE should return a result");

    // Data should still be intact after optimize
    assertEquals(5, spark.sql("SELECT * FROM " + fullTable).count());
  }

  // ---------------------------------------------------------------------------
  // SQL Extensions: VACUUM (blocking requirement)
  // ---------------------------------------------------------------------------

  @Test
  public void testVacuumLanceTableThroughSessionCatalog() {
    String tableName = generateTableName("vacuum");
    String fullTable = "default." + tableName;

    spark.sql("CREATE TABLE " + fullTable + " (id INT, text STRING) USING lance");
    spark.sql("INSERT INTO " + fullTable + " VALUES (1, 'a'), (2, 'b')");

    // VACUUM should work through session catalog
    Dataset<Row> result = spark.sql("VACUUM " + fullTable);
    assertNotNull(result, "VACUUM should return a result");

    // Data should still be intact after vacuum
    assertEquals(2, spark.sql("SELECT * FROM " + fullTable).count());
  }

  // ---------------------------------------------------------------------------
  // default-provider=error mode
  // ---------------------------------------------------------------------------

  @Test
  public void testDefaultProviderErrorModeRejectsNullProvider() {
    BaseLanceNamespaceSparkSessionCatalog sessionCat =
        (BaseLanceNamespaceSparkSessionCatalog) catalog;

    Map<String, String> options = new HashMap<>();
    options.put("impl", "dir");
    options.put("root", tempDir.toString());
    options.put("single_level_ns", "true");
    options.put("default-provider", "error");

    // Re-initialize with error mode
    sessionCat.initialize("spark_catalog", new CaseInsensitiveStringMap(options));

    // Creating a table without USING should throw
    assertThrows(
        RuntimeException.class,
        () ->
            sessionCat.createTable(
                Identifier.of(new String[] {"default"}, "test_table"),
                new org.apache.spark.sql.types.StructType().add("id", "long"),
                new org.apache.spark.sql.connector.expressions.Transform[0],
                new HashMap<>()),
        "Creating table without provider in error mode should throw");
  }

  // Note: there is no SQL-level test for default-provider=lance because CREATE TABLE without
  // USING goes through Spark's v1 Hive code path (LazySimpleSerDe) before reaching the v2
  // catalog's createTable. The default-provider routing is validated via the catalog API in
  // testDefaultProviderErrorModeRejectsNullProvider above.

  // ---------------------------------------------------------------------------
  // Invalidate table — both catalogs called
  // ---------------------------------------------------------------------------

  @Test
  public void testInvalidateTableDoesNotThrow() {
    String tableName = generateTableName("invalidate");
    spark.sql("CREATE TABLE default." + tableName + " (id BIGINT) USING lance");

    // invalidateTable should not throw — it calls both catalogs unconditionally
    Identifier ident = Identifier.of(new String[] {"default"}, tableName);
    catalog.invalidateTable(ident);

    // Table should still be loadable after invalidation
    Dataset<Row> result = spark.sql("SELECT * FROM default." + tableName);
    assertEquals(0, result.count());
  }

  // ---------------------------------------------------------------------------
  // SQL Extensions: CREATE INDEX (blocking requirement)
  // ---------------------------------------------------------------------------

  @Test
  public void testCreateIndexThroughSessionCatalog() {
    String tableName = generateTableName("index");
    String fullTable = "default." + tableName;

    spark.sql("CREATE TABLE " + fullTable + " (id INT, text STRING) USING lance");
    // Need multiple fragments for index creation
    spark.sql(
        String.format(
            "INSERT INTO %s (id, text) VALUES %s",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
    spark.sql(
        String.format(
            "INSERT INTO %s (id, text) VALUES %s",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));

    // CREATE INDEX should work through session catalog
    Dataset<Row> result =
        spark.sql("ALTER TABLE " + fullTable + " CREATE INDEX test_idx USING btree (id)");
    assertNotNull(result, "CREATE INDEX should return a result");
    List<Row> rows = result.collectAsList();
    assertTrue(rows.size() > 0, "CREATE INDEX should return at least one row");

    // Data should still be queryable with the index
    assertEquals(20, spark.sql("SELECT * FROM " + fullTable).count());
  }

  // ---------------------------------------------------------------------------
  // SQL Extensions: graceful failure on non-Lance table
  // ---------------------------------------------------------------------------

  @Test
  public void testSqlExtensionOnNonLanceTableFails() {
    String tableName = generateTableName("parquet_optimize");
    spark.sql("CREATE TABLE default." + tableName + " (id INT, text STRING) USING parquet");
    spark.sql("INSERT INTO default." + tableName + " VALUES (1, 'a')");

    // OPTIMIZE on a non-Lance table should fail gracefully
    assertThrows(
        RuntimeException.class,
        () -> spark.sql("OPTIMIZE default." + tableName),
        "OPTIMIZE on non-Lance table should throw");
  }
}
