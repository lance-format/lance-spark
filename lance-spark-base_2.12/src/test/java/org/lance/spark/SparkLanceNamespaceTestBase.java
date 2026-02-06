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

import org.lance.Version;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class SparkLanceNamespaceTestBase {
  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-namespace-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", getNsImpl())
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    Map<String, String> additionalConfigs = getAdditionalNsConfigs();
    for (Map.Entry<String, String> entry : additionalConfigs.entrySet()) {
      spark.conf().set("spark.sql.catalog." + catalogName + "." + entry.getKey(), entry.getValue());
    }

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.stop();
    }
  }

  protected abstract String getNsImpl();

  protected Map<String, String> getAdditionalNsConfigs() {
    return new HashMap<>();
  }

  /**
   * Override this method to indicate whether the namespace implementation supports namespace
   * operations. Default is false for backward compatibility.
   *
   * @return true if namespace operations are supported, false otherwise
   */
  protected boolean supportsNamespace() {
    return false;
  }

  /**
   * Generates a unique table name with UUID suffix to avoid conflicts.
   *
   * @param baseName the base name for the table
   * @return unique table name with UUID suffix
   */
  protected String generateTableName(String baseName) {
    return baseName + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  @Test
  public void testTimeTravelVersionAsOf() throws Exception {
    String tableName = generateTableName("time_travel_version");
    String fullName = catalogName + ".default." + tableName;

    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");
    assertTrue(checkDataset(0, fullName));

    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'v1')");
    assertTrue(checkDataset(1, fullName));
    spark.sql("INSERT INTO " + fullName + " VALUES (2, 'v2')");
    assertTrue(checkDataset(2, fullName));

    // time travel to version 2 (the second insert)
    Dataset<Row> actual = spark.sql("SELECT * FROM " + fullName + " VERSION AS OF " + "2");
    List<Row> res = actual.collectAsList();
    assertEquals(1, res.size());
  }

  @Test
  public void testTimeTravelTimestampAsOf() throws Exception {
    String tableName = generateTableName("time_travel_version");
    String fullName = catalogName + ".default." + tableName;

    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");
    assertTrue(checkDataset(0, fullName));

    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'v1')");
    assertTrue(checkDataset(1, fullName));

    Thread.sleep(1000);
    spark.sql("INSERT INTO " + fullName + " VALUES (2, 'v2')");
    assertTrue(checkDataset(2, fullName));

    Version version = getLatestVersion(tableName);
    spark.sql("INSERT INTO " + fullName + " VALUES (3, 'v3')");
    assertTrue(checkDataset(3, fullName));

    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String date = version.getDataTime().format(format);

    // time travel to timestamp before second insert
    Dataset<Row> actual =
        spark.sql("SELECT * FROM " + fullName + " TIMESTAMP AS OF '" + date + "'");
    List<Row> res = actual.collectAsList();
    assertEquals(1, res.size());
  }

  @Test
  public void testCreateAndDescribeTable() throws Exception {
    String tableName = generateTableName("test_table");

    // Create table using Spark SQL DDL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName
            + " (id BIGINT NOT NULL, name STRING)");

    // Describe table using Spark SQL
    Dataset<Row> describeResult =
        spark.sql("DESCRIBE TABLE " + catalogName + ".default." + tableName);
    List<Row> columns = describeResult.collectAsList();

    // Verify table structure
    assertEquals(2, columns.size());

    // Check id column
    Row idColumn = columns.get(0);
    assertEquals("id", idColumn.getString(0));
    assertEquals("bigint", idColumn.getString(1));

    // Check name column
    Row nameColumn = columns.get(1);
    assertEquals("name", nameColumn.getString(0));
    assertEquals("string", nameColumn.getString(1));
  }

  @Test
  public void testListTables() throws Exception {
    String tableName1 = generateTableName("list_test_1");
    String tableName2 = generateTableName("list_test_2");

    // Create tables using Spark SQL
    spark.sql("CREATE TABLE " + catalogName + ".default." + tableName1 + " (id BIGINT NOT NULL)");
    spark.sql("CREATE TABLE " + catalogName + ".default." + tableName2 + " (id BIGINT NOT NULL)");

    // Use SHOW TABLES to list tables
    Dataset<Row> tablesResult = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tables = tablesResult.collectAsList();

    assertTrue(tables.size() >= 2);

    boolean foundTable1 = false;
    boolean foundTable2 = false;
    for (Row row : tables) {
      String tableName = row.getString(1); // table name is in the second column
      if (tableName1.equals(tableName)) {
        foundTable1 = true;
      }
      if (tableName2.equals(tableName)) {
        foundTable2 = true;
      }
    }
    assertTrue(foundTable1);
    assertTrue(foundTable2);
  }

  @Test
  public void testDropTable() throws Exception {
    String tableName = generateTableName("drop_test");

    // Create table using Spark SQL
    spark.sql("CREATE TABLE " + catalogName + ".default." + tableName + " (id BIGINT NOT NULL)");

    // Verify table exists by querying it
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertNotNull(result);
    assertEquals(0L, result.collectAsList().get(0).getLong(0));

    // Drop table using Spark SQL
    spark.sql("DROP TABLE " + catalogName + ".default." + tableName);

    // Verify table no longer exists
    assertThrows(
        Exception.class,
        () -> {
          spark
              .sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName)
              .collectAsList();
        });
  }

  @Test
  public void testLoadSparkTable() throws Exception {
    // Test successful case - create table and load it
    String existingTableName = generateTableName("existing_table");

    // Create table using Spark SQL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + existingTableName
            + " (id BIGINT NOT NULL, name STRING)");

    // Insert test data
    spark.sql(
        "INSERT INTO " + catalogName + ".default." + existingTableName + " VALUES (1, 'test')");

    // Successfully load existing table using spark.table()
    Dataset<Row> table = spark.table(catalogName + ".default." + existingTableName);
    assertNotNull(table);
    assertEquals(1, table.count());

    // Test failure case - try to load non-existent table
    String nonExistentTableName = generateTableName("non_existent");

    // Verify loading non-existent table throws exception
    assertThrows(
        Exception.class,
        () -> {
          spark.table(catalogName + ".default." + nonExistentTableName);
        });
  }

  @Test
  public void testSparkSqlSelect() throws Exception {
    String tableName = generateTableName("sql_test_table");

    // Create a table using SQL DDL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName
            + " (id INT NOT NULL, name STRING, value DOUBLE)");

    // Create test data and insert using SQL
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'Alice', 100.0), "
            + "(2, 'Bob', 200.0), "
            + "(3, 'Charlie', 300.0)");

    // Query using Spark SQL with catalog notation
    Dataset<Row> result = spark.sql("SELECT * FROM " + catalogName + ".default." + tableName);
    assertEquals(3, result.count());

    // Test filtering
    Dataset<Row> filtered =
        spark.sql("SELECT * FROM " + catalogName + ".default." + tableName + " WHERE id > 1");
    assertEquals(2, filtered.count());

    // Test aggregation
    Dataset<Row> aggregated =
        spark.sql("SELECT COUNT(*) as cnt FROM " + catalogName + ".default." + tableName);
    assertEquals(3L, aggregated.collectAsList().get(0).getLong(0));

    // Test projection
    Dataset<Row> projected =
        spark.sql(
            "SELECT name, value FROM " + catalogName + ".default." + tableName + " WHERE id = 2");
    Row row = projected.collectAsList().get(0);
    assertEquals("Bob", row.getString(0));
    assertEquals(200.0, row.getDouble(1), 0.001);
  }

  @Test
  public void testSparkSqlJoin() throws Exception {
    String tableName1 = generateTableName("join_table_1");
    String tableName2 = generateTableName("join_table_2");

    // Create first table using SQL DDL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName1
            + " (id INT NOT NULL, name STRING)");

    // Insert data into first table
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName1
            + " VALUES "
            + "(1, 'Alice'), "
            + "(2, 'Bob'), "
            + "(3, 'Charlie')");

    // Create second table using SQL DDL
    spark.sql(
        "CREATE TABLE " + catalogName + ".default." + tableName2 + " (id INT NOT NULL, score INT)");

    // Insert data into second table
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName2
            + " VALUES "
            + "(1, 95), "
            + "(2, 87), "
            + "(3, 92)");

    // Test join query
    Dataset<Row> joined =
        spark.sql(
            "SELECT t1.name, t2.score FROM "
                + catalogName
                + ".default."
                + tableName1
                + " t1 "
                + "JOIN "
                + catalogName
                + ".default."
                + tableName2
                + " t2 ON t1.id = t2.id");
    assertEquals(3, joined.count());

    // Verify join results
    List<Row> results = joined.orderBy("name").collectAsList();
    assertEquals("Alice", results.get(0).getString(0));
    assertEquals(95, results.get(0).getInt(1));
    assertEquals("Bob", results.get(1).getString(0));
    assertEquals(87, results.get(1).getInt(1));
    assertEquals("Charlie", results.get(2).getString(0));
    assertEquals(92, results.get(2).getInt(1));
  }

  @Test
  public void testCreateAndDropNamespace() throws Exception {
    if (!supportsNamespace()) {
      return; // Skip test for implementations that don't support namespaces
    }

    String namespaceName = "test_ns_" + UUID.randomUUID().toString().replace("-", "");

    // Create namespace using Spark SQL
    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespaceName);

    // Verify namespace exists
    Dataset<Row> namespaces = spark.sql("SHOW NAMESPACES IN " + catalogName);
    List<Row> nsList = namespaces.collectAsList();
    boolean found = false;
    for (Row row : nsList) {
      if (namespaceName.equals(row.getString(0))) {
        found = true;
        break;
      }
    }
    assertTrue(found);

    // Drop namespace
    spark.sql("DROP NAMESPACE " + catalogName + "." + namespaceName);

    // Verify namespace no longer exists
    namespaces = spark.sql("SHOW NAMESPACES IN " + catalogName);
    nsList = namespaces.collectAsList();
    found = false;
    for (Row row : nsList) {
      if (namespaceName.equals(row.getString(0))) {
        found = true;
        break;
      }
    }
    assertFalse(found);
  }

  @Test
  public void testListNamespaces() throws Exception {
    if (!supportsNamespace()) {
      return; // Skip test for implementations that don't support namespaces
    }

    String namespace1 = "list_ns_1_" + UUID.randomUUID().toString().replace("-", "");
    String namespace2 = "list_ns_2_" + UUID.randomUUID().toString().replace("-", "");

    // Create namespaces
    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespace1);
    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespace2);

    // List namespaces
    Dataset<Row> namespaces = spark.sql("SHOW NAMESPACES IN " + catalogName);
    List<Row> nsList = namespaces.collectAsList();

    boolean foundNs1 = false;
    boolean foundNs2 = false;
    for (Row row : nsList) {
      String ns = row.getString(0);
      if (namespace1.equals(ns)) {
        foundNs1 = true;
      }
      if (namespace2.equals(ns)) {
        foundNs2 = true;
      }
    }
    assertTrue(foundNs1);
    assertTrue(foundNs2);
  }

  @Test
  public void testNamespaceMetadata() throws Exception {
    if (!supportsNamespace()) {
      return; // Skip test for implementations that don't support namespaces
    }

    String namespaceName = "metadata_ns_" + UUID.randomUUID().toString().replace("-", "");

    // Create namespace with properties
    spark.sql(
        "CREATE NAMESPACE "
            + catalogName
            + "."
            + namespaceName
            + " WITH DBPROPERTIES ('key1'='value1', 'key2'='value2')");

    // Describe namespace
    Dataset<Row> properties =
        spark.sql("DESCRIBE NAMESPACE EXTENDED " + catalogName + "." + namespaceName);
    List<Row> propList = properties.collectAsList();

    // Verify properties are returned (exact format may vary by implementation)
    assertNotNull(propList);
    assertTrue(propList.size() > 0);
  }

  @Test
  public void testNamespaceWithTables() throws Exception {
    if (!supportsNamespace()) {
      return; // Skip test for implementations that don't support namespaces
    }

    String namespaceName = "tables_ns_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = generateTableName("ns_table");

    // Create namespace
    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespaceName);

    // Create table in namespace
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + "."
            + namespaceName
            + "."
            + tableName
            + " (id BIGINT NOT NULL, name STRING)");

    // Insert data
    spark.sql(
        "INSERT INTO "
            + catalogName
            + "."
            + namespaceName
            + "."
            + tableName
            + " VALUES (1, 'test')");

    // Query table
    Dataset<Row> result =
        spark.sql("SELECT * FROM " + catalogName + "." + namespaceName + "." + tableName);
    assertEquals(1, result.count());

    // List tables in namespace
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + "." + namespaceName);
    List<Row> tableList = tables.collectAsList();
    assertEquals(1, tableList.size());
    assertEquals(tableName, tableList.get(0).getString(1));
  }

  @Test
  public void testCascadeDropNamespace() throws Exception {
    if (!supportsNamespace()) {
      return; // Skip test for implementations that don't support namespaces
    }

    String namespaceName = "cascade_ns_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = generateTableName("cascade_table");

    // Create namespace
    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespaceName);

    // Create table in namespace
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + "."
            + namespaceName
            + "."
            + tableName
            + " (id BIGINT NOT NULL)");

    // Try to drop namespace without CASCADE (should fail)
    assertThrows(
        Exception.class,
        () -> {
          spark.sql("DROP NAMESPACE " + catalogName + "." + namespaceName);
        });

    // Drop namespace with CASCADE (should succeed)
    spark.sql("DROP NAMESPACE " + catalogName + "." + namespaceName + " CASCADE");

    // Verify namespace is gone
    Dataset<Row> namespaces = spark.sql("SHOW NAMESPACES IN " + catalogName);
    List<Row> nsList = namespaces.collectAsList();
    boolean found = false;
    for (Row row : nsList) {
      if (namespaceName.equals(row.getString(0))) {
        found = true;
        break;
      }
    }
    assertFalse(found);
  }

  @Test
  public void testTwoPartIdentifier() throws Exception {
    String tableName = generateTableName("two_part_test");

    // Set default catalog to Lance
    spark.sql("SET spark.sql.defaultCatalog=" + catalogName);

    // Create table using namespace.table (2-part identifier)
    spark.sql("CREATE TABLE default." + tableName + " (id BIGINT NOT NULL, name STRING)");

    // Show tables using namespace
    Dataset<Row> tables = spark.sql("SHOW TABLES IN default");
    boolean found =
        tables.collectAsList().stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found);

    // Describe table using namespace.table
    Dataset<Row> description = spark.sql("DESCRIBE TABLE default." + tableName);
    assertEquals(2, description.count());

    // Insert and select using namespace.table
    spark.sql("INSERT INTO default." + tableName + " VALUES (1, 'test')");
    Dataset<Row> result = spark.sql("SELECT * FROM default." + tableName);
    assertEquals(1, result.count());

    Row row = result.collectAsList().get(0);
    assertEquals(1L, row.getLong(0));
    assertEquals("test", row.getString(1));
  }

  @Test
  public void testInsertOverwrite() throws Exception {
    String tableName = generateTableName("insert_overwrite_test");
    String fullName = catalogName + ".default." + tableName;

    // Create table and insert initial data
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'Alice'), (2, 'Bob')");
    assertEquals(2, spark.sql("SELECT * FROM " + fullName).count());

    // Use INSERT OVERWRITE to replace all data
    spark.sql("INSERT OVERWRITE " + fullName + " VALUES (3, 'Charlie'), (4, 'David'), (5, 'Eve')");
    assertEquals(3, spark.sql("SELECT * FROM " + fullName).count());

    // Verify old data is gone and new data exists
    List<Row> rows = spark.sql("SELECT * FROM " + fullName + " ORDER BY id").collectAsList();
    assertEquals(3, rows.get(0).getInt(0));
    assertEquals("Charlie", rows.get(0).getString(1));
    assertEquals(4, rows.get(1).getInt(0));
    assertEquals("David", rows.get(1).getString(1));
    assertEquals(5, rows.get(2).getInt(0));
    assertEquals("Eve", rows.get(2).getString(1));
  }

  @Test
  public void testInsertOverwriteWithSelect() throws Exception {
    String sourceTable = generateTableName("source_table");
    String targetTable = generateTableName("target_table");
    String sourceFullName = catalogName + ".default." + sourceTable;
    String targetFullName = catalogName + ".default." + targetTable;

    // Create source table with data
    spark.sql("CREATE TABLE " + sourceFullName + " (id INT NOT NULL, name STRING)");
    spark.sql("INSERT INTO " + sourceFullName + " VALUES (10, 'New1'), (20, 'New2')");

    // Create target table with initial data
    spark.sql("CREATE TABLE " + targetFullName + " (id INT NOT NULL, name STRING)");
    spark.sql("INSERT INTO " + targetFullName + " VALUES (1, 'Old1'), (2, 'Old2')");
    assertEquals(2, spark.sql("SELECT * FROM " + targetFullName).count());

    // Use INSERT OVERWRITE with SELECT to replace data
    spark.sql("INSERT OVERWRITE " + targetFullName + " SELECT * FROM " + sourceFullName);
    assertEquals(2, spark.sql("SELECT * FROM " + targetFullName).count());

    // Verify data was replaced
    List<Row> rows = spark.sql("SELECT * FROM " + targetFullName + " ORDER BY id").collectAsList();
    assertEquals(10, rows.get(0).getInt(0));
    assertEquals("New1", rows.get(0).getString(1));
    assertEquals(20, rows.get(1).getInt(0));
    assertEquals("New2", rows.get(1).getString(1));
  }

  @Test
  public void testOnePartIdentifier() throws Exception {
    String tableName = generateTableName("one_part_test");

    // Set default catalog and use namespace
    spark.sql("SET spark.sql.defaultCatalog=" + catalogName);
    spark.sql("USE default");

    // Create table using just table name (1-part identifier)
    spark.sql("CREATE TABLE " + tableName + " (id BIGINT NOT NULL, value DOUBLE)");

    // Show tables in current namespace
    Dataset<Row> tables = spark.sql("SHOW TABLES");
    boolean found =
        tables.collectAsList().stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found);

    // Describe table using just table name
    Dataset<Row> description = spark.sql("DESCRIBE TABLE " + tableName);
    assertEquals(2, description.count());

    // Insert and select using just table name
    spark.sql("INSERT INTO " + tableName + " VALUES (42, 3.14)");
    Dataset<Row> result = spark.sql("SELECT * FROM " + tableName);
    assertEquals(1, result.count());

    Row row = result.collectAsList().get(0);
    assertEquals(42L, row.getLong(0));
    assertEquals(3.14, row.getDouble(1), 0.001);
  }

  private boolean checkDataset(int expectedSize, String tableName) {
    Dataset<Row> actual = spark.sql("SELECT * FROM " + tableName);
    List<Row> res = actual.collectAsList();

    return expectedSize == res.size();
  }

  private Version getLatestVersion(String tableName) throws Exception {
    Identifier ident = Identifier.of(new String[] {"default"}, tableName);
    LanceDataset lanceTable = (LanceDataset) catalog.loadTable(ident);
    LanceSparkReadOptions readOptions = lanceTable.readOptions();
    try (org.lance.Dataset dataset = openLatestDataset(readOptions)) {
      return dataset.getVersion();
    }
  }

  private org.lance.Dataset openLatestDataset(LanceSparkReadOptions readOptions) {
    if (readOptions.hasNamespace()) {
      return org.lance.Dataset.open()
          .allocator(LanceRuntime.allocator())
          .namespace(readOptions.getNamespace())
          .tableId(readOptions.getTableId())
          .readOptions(readOptions.toReadOptions())
          .build();
    }
    return org.lance.Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(readOptions.getDatasetUri())
        .readOptions(readOptions.toReadOptions())
        .build();
  }
}
