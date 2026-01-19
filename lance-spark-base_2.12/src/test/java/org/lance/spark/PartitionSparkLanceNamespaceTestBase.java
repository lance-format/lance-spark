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

import org.lance.namespace.model.CreateNamespaceRequest;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract test base for SQL DDL operations on partitioned tables using LanceNamespaceSparkCatalog.
 */
public abstract class PartitionSparkLanceNamespaceTestBase {

  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";
  protected List<String> locations = new ArrayList<>();

  @BeforeEach
  void setup() throws Exception {
    spark =
        SparkSession.builder()
            .appName("lance-partition-namespace-test")
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
  }

  protected String genMemPartitionTableLocation() {
    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    MemoryLanceNamespace namespace = MemoryLanceNamespace.create(location, new HashMap<>());
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.id(Lists.asList("default", new String[0]));
    namespace.createNamespace(request);

    locations.add(location);
    return location;
  }

  @AfterEach
  void tearDown() throws Exception {
    if (spark != null) {
      spark.stop();
    }
    for (String location : locations) {
      MemoryLanceNamespace namespace = MemoryLanceNamespace.create(location, new HashMap<>());
      namespace.cleanup();
    }
  }

  /** Return the namespace implementation identifier (e.g. "dir", "rest"). */
  protected abstract String getNsImpl();

  /** Additional catalog-level configurations, such as DirectoryNamespace root. */
  protected Map<String, String> getAdditionalNsConfigs() {
    return new HashMap<>();
  }

  /**
   * Override this method to indicate whether the namespace implementation supports namespace
   * operations. Default is false for backward compatibility.
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

  private void createBasicPartitionedTable(String fullTableName, String location) {
    String ddl =
        "CREATE TABLE "
            + fullTableName
            + " (id INT NOT NULL, name STRING, value DOUBLE, country STRING, city STRING) "
            + "PARTITIONED BY (country, bucket(4, city)) "
            + "LOCATION '"
            + location
            + "'";
    spark.sql(ddl);
  }

  private void insertDefaultPartitionRows(String fullTableName) {
    spark.sql(
        "INSERT INTO "
            + fullTableName
            + " VALUES "
            + "(1, 'Alice', 100.0, 'CN', 'BJ'), "
            + "(2, 'Bob', 200.0, 'CN', 'SH'), "
            + "(3, 'Charlie', 300.0, 'US', 'NY'), "
            + "(4, 'David', 400.0, 'US', 'SF')");
  }

  @Test
  public void testCreateAndDescribeTablePartitioned() throws Exception {
    String tableName = generateTableName("pt_create_describe_sql");
    String fullTableName = catalogName + ".default." + tableName;
    String tableLocation = genMemPartitionTableLocation();

    // Create partitioned table using SQL DDL with PARTITIONED BY and explicit location.
    createBasicPartitionedTable(fullTableName, tableLocation);

    // Describe table and verify presence of data and partition columns.
    Dataset<Row> describeResult = spark.sql("DESCRIBE TABLE " + fullTableName);
    List<Row> columns = describeResult.collectAsList();

    boolean hasId = false;
    boolean hasName = false;
    boolean hasValue = false;
    boolean hasCountry = false;
    boolean hasCity = false;

    for (Row row : columns) {
      String colName = row.getString(0);
      if (colName == null || colName.isEmpty() || colName.startsWith("#")) {
        continue;
      }
      if ("id".equals(colName)) {
        hasId = true;
      }
      if ("name".equals(colName)) {
        hasName = true;
      }
      if ("value".equals(colName)) {
        hasValue = true;
      }
      if ("country".equals(colName)) {
        hasCountry = true;
      }
      if ("city".equals(colName)) {
        hasCity = true;
      }
    }

    assertTrue(hasId);
    assertTrue(hasName);
    assertTrue(hasValue);
    assertTrue(hasCountry);
    assertTrue(hasCity);

    // Insert sample rows and validate row count.
    insertDefaultPartitionRows(fullTableName);
    Dataset<Row> countResult = spark.sql("SELECT COUNT(*) FROM " + fullTableName);
    assertEquals(4L, countResult.collectAsList().get(0).getLong(0));
  }

  @Test
  public void testListTablesPartitioned() throws Exception {
    String tableName1 = generateTableName("pt_list_1_sql");
    String tableName2 = generateTableName("pt_list_2_sql");

    String fullTableName1 = catalogName + ".default." + tableName1;
    String fullTableName2 = catalogName + ".default." + tableName2;

    String location1 = genMemPartitionTableLocation();
    String location2 = genMemPartitionTableLocation();

    createBasicPartitionedTable(fullTableName1, location1);
    createBasicPartitionedTable(fullTableName2, location2);

    Dataset<Row> tablesResult = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tables = tablesResult.collectAsList();

    boolean foundTable1 = false;
    boolean foundTable2 = false;
    for (Row row : tables) {
      String name = row.getString(1);
      if (tableName1.equals(name)) {
        foundTable1 = true;
      }
      if (tableName2.equals(name)) {
        foundTable2 = true;
      }
    }

    assertTrue(foundTable1);
    assertTrue(foundTable2);
  }

  @Test
  public void testDropTablePartitioned() throws Exception {
    String tableName = generateTableName("pt_drop_sql");
    String fullTableName = catalogName + ".default." + tableName;
    String location = genMemPartitionTableLocation();

    createBasicPartitionedTable(fullTableName, location);
    insertDefaultPartitionRows(fullTableName);

    Dataset<Row> countResult = spark.sql("SELECT COUNT(*) FROM " + fullTableName);
    assertEquals(4L, countResult.collectAsList().get(0).getLong(0));

    spark.sql("DROP TABLE " + fullTableName);

    assertThrows(
        Exception.class, () -> spark.sql("SELECT COUNT(*) FROM " + fullTableName).collectAsList());
  }

  @Test
  public void testLoadSparkTablePartitioned() throws Exception {
    // Successful load of existing partitioned table.
    String existingTableName = generateTableName("pt_existing_table_sql");
    String fullExistingTable = catalogName + ".default." + existingTableName;
    String existingLocation = genMemPartitionTableLocation();

    createBasicPartitionedTable(fullExistingTable, existingLocation);
    spark.sql("INSERT INTO " + fullExistingTable + " VALUES (1, 'Alice', 100.0, 'CN', 'BJ')");

    Dataset<Row> table = spark.table(fullExistingTable);
    assertNotNull(table);
    assertEquals(1L, table.count());

    // Loading a non-existent partitioned table should fail.
    String nonExistent = generateTableName("pt_non_existent_sql");
    String fullNonExistent = catalogName + ".default." + nonExistent;

    assertThrows(Exception.class, () -> spark.table(fullNonExistent));
  }

  @Test
  public void testSparkSqlSelectPartitioned() throws Exception {
    String tableName = generateTableName("pt_sql_select_table");
    String fullTableName = catalogName + ".default." + tableName;
    String location = genMemPartitionTableLocation();

    createBasicPartitionedTable(fullTableName, location);
    insertDefaultPartitionRows(fullTableName);

    Dataset<Row> result = spark.sql("SELECT * FROM " + fullTableName);
    assertEquals(4L, result.count());

    Dataset<Row> filtered = spark.sql("SELECT * FROM " + fullTableName + " WHERE id > 1");
    assertEquals(3L, filtered.count());

    Dataset<Row> aggregated = spark.sql("SELECT COUNT(*) AS cnt FROM " + fullTableName);
    assertEquals(4L, aggregated.collectAsList().get(0).getLong(0));

    Dataset<Row> projected =
        spark.sql("SELECT name, value FROM " + fullTableName + " WHERE id = 2");
    Row row = projected.collectAsList().get(0);
    assertEquals("Bob", row.getString(0));
    assertEquals(200.0, row.getDouble(1), 0.001);
  }

  @Test
  public void testSparkSqlJoinPartitioned() throws Exception {
    String tableName1 = generateTableName("pt_join_table_1");
    String tableName2 = generateTableName("pt_join_table_2");

    String fullTable1 = catalogName + ".default." + tableName1;
    String fullTable2 = catalogName + ".default." + tableName2;

    String location1 = genMemPartitionTableLocation();
    String location2 = genMemPartitionTableLocation();

    // First partitioned table.
    String ddl1 =
        "CREATE TABLE "
            + fullTable1
            + " (id INT NOT NULL, name STRING, country STRING, city STRING) "
            + "PARTITIONED BY (country, bucket(4, city)) "
            + "LOCATION '"
            + location1
            + "'";
    spark.sql(ddl1);
    spark.sql(
        "INSERT INTO "
            + fullTable1
            + " VALUES "
            + "(1, 'Alice', 'CN', 'BJ'), "
            + "(2, 'Bob', 'CN', 'SH'), "
            + "(3, 'Charlie', 'US', 'NY')");

    // Second partitioned table.
    String ddl2 =
        "CREATE TABLE "
            + fullTable2
            + " (id INT NOT NULL, score INT, country STRING, city STRING) "
            + "PARTITIONED BY (country, bucket(4, city)) "
            + "LOCATION '"
            + location2
            + "'";
    spark.sql(ddl2);
    spark.sql(
        "INSERT INTO "
            + fullTable2
            + " VALUES "
            + "(1, 95, 'CN', 'BJ'), "
            + "(2, 87, 'CN', 'SH'), "
            + "(3, 92, 'US', 'NY')");

    Dataset<Row> joined =
        spark.sql(
            "SELECT t1.name, t2.score FROM "
                + fullTable1
                + " t1 JOIN "
                + fullTable2
                + " t2 ON t1.id = t2.id");
    assertEquals(3L, joined.count());

    List<Row> results = joined.orderBy("name").collectAsList();
    assertEquals("Alice", results.get(0).getString(0));
    assertEquals(95, results.get(0).getInt(1));
    assertEquals("Bob", results.get(1).getString(0));
    assertEquals(87, results.get(1).getInt(1));
    assertEquals("Charlie", results.get(2).getString(0));
    assertEquals(92, results.get(2).getInt(1));
  }

  @Test
  public void testCreateAndDropNamespacePartitioned() throws Exception {
    if (!supportsNamespace()) {
      return;
    }

    String namespaceName = "pt_ns_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespaceName);

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

    spark.sql("DROP NAMESPACE " + catalogName + "." + namespaceName);

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
  public void testListNamespacesPartitioned() throws Exception {
    if (!supportsNamespace()) {
      return;
    }

    String namespace1 = "pt_list_ns_1_" + UUID.randomUUID().toString().replace("-", "");
    String namespace2 = "pt_list_ns_2_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespace1);
    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespace2);

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
  public void testNamespaceMetadataPartitioned() throws Exception {
    if (!supportsNamespace()) {
      return;
    }

    String namespaceName = "pt_metadata_ns_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(
        "CREATE NAMESPACE "
            + catalogName
            + "."
            + namespaceName
            + " WITH DBPROPERTIES ('key1'='value1', 'key2'='value2')");

    Dataset<Row> properties =
        spark.sql("DESCRIBE NAMESPACE EXTENDED " + catalogName + "." + namespaceName);
    List<Row> propList = properties.collectAsList();

    assertNotNull(propList);
    assertTrue(propList.size() > 0);
  }

  @Test
  public void testNamespaceWithPartitionedTables() throws Exception {
    if (!supportsNamespace()) {
      return;
    }

    String namespaceName = "pt_tables_ns_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = generateTableName("pt_ns_table_sql");

    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespaceName);

    String fullTableName = catalogName + "." + namespaceName + "." + tableName;
    String location = genMemPartitionTableLocation();

    String ddl =
        "CREATE TABLE "
            + fullTableName
            + " (id BIGINT NOT NULL, name STRING, country STRING, city STRING) "
            + "PARTITIONED BY (country, bucket(4, city)) "
            + "LOCATION '"
            + location
            + "'";
    spark.sql(ddl);

    spark.sql("INSERT INTO " + fullTableName + " VALUES (1, 'test', 'CN', 'BJ')");

    Dataset<Row> result = spark.sql("SELECT * FROM " + fullTableName);
    assertEquals(1L, result.count());

    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + "." + namespaceName);
    List<Row> tableList = tables.collectAsList();

    boolean found = false;
    for (Row row : tableList) {
      if (tableName.equals(row.getString(1))) {
        found = true;
        break;
      }
    }
    assertTrue(found);
  }

  @Test
  public void testCascadeDropNamespacePartitioned() throws Exception {
    if (!supportsNamespace()) {
      return;
    }

    String namespaceName = "pt_cascade_ns_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = generateTableName("pt_cascade_table_sql");

    spark.sql("CREATE NAMESPACE " + catalogName + "." + namespaceName);

    String fullTableName = catalogName + "." + namespaceName + "." + tableName;
    String location = genMemPartitionTableLocation();

    String ddl =
        "CREATE TABLE "
            + fullTableName
            + " (id BIGINT NOT NULL, country STRING, city STRING) "
            + "PARTITIONED BY (country, bucket(4, city)) "
            + "LOCATION '"
            + location
            + "'";
    spark.sql(ddl);

    assertThrows(
        Exception.class, () -> spark.sql("DROP NAMESPACE " + catalogName + "." + namespaceName));

    spark.sql("DROP NAMESPACE " + catalogName + "." + namespaceName + " CASCADE");

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
  public void testTwoPartIdentifierPartitioned() throws Exception {
    String tableName = generateTableName("two_part_partitioned_sql");

    spark.sql("SET spark.sql.defaultCatalog=" + catalogName);

    String tableIdent = "default." + tableName;
    String location = genMemPartitionTableLocation();

    String ddl =
        "CREATE TABLE "
            + tableIdent
            + " (id BIGINT NOT NULL, name STRING, country STRING, city STRING) "
            + "PARTITIONED BY (country, bucket(4, city)) "
            + "LOCATION '"
            + location
            + "'";
    spark.sql(ddl);

    Dataset<Row> tables = spark.sql("SHOW TABLES IN default");
    boolean found =
        tables.collectAsList().stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found);

    Dataset<Row> description = spark.sql("DESCRIBE TABLE default." + tableName);
    List<Row> cols = description.collectAsList();

    boolean hasId = false;
    boolean hasName = false;
    boolean hasCountry = false;
    boolean hasCity = false;
    for (Row row : cols) {
      String colName = row.getString(0);
      if (colName == null || colName.isEmpty() || colName.startsWith("#")) {
        continue;
      }
      if ("id".equals(colName)) {
        hasId = true;
      }
      if ("name".equals(colName)) {
        hasName = true;
      }
      if ("country".equals(colName)) {
        hasCountry = true;
      }
      if ("city".equals(colName)) {
        hasCity = true;
      }
    }

    assertTrue(hasId);
    assertTrue(hasName);
    assertTrue(hasCountry);
    assertTrue(hasCity);

    spark.sql("INSERT INTO default." + tableName + " VALUES (1, 'test', 'CN', 'BJ')");

    Dataset<Row> result = spark.sql("SELECT * FROM default." + tableName);
    assertEquals(1L, result.count());

    Row row = result.collectAsList().get(0);
    assertEquals(1L, row.getLong(0));
    assertEquals("test", row.getString(1));
  }

  @Test
  public void testOnePartIdentifierPartitioned() throws Exception {
    String tableName = generateTableName("one_part_partitioned_sql");

    spark.sql("SET spark.sql.defaultCatalog=" + catalogName);
    spark.sql("USE default");

    String location = genMemPartitionTableLocation();

    String ddl =
        "CREATE TABLE "
            + tableName
            + " (id BIGINT NOT NULL, value DOUBLE, country STRING, city STRING) "
            + "PARTITIONED BY (country, bucket(4, city)) "
            + "LOCATION '"
            + location
            + "'";
    spark.sql(ddl);

    Dataset<Row> tables = spark.sql("SHOW TABLES");
    boolean found =
        tables.collectAsList().stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found);

    Dataset<Row> description = spark.sql("DESCRIBE TABLE " + tableName);
    List<Row> cols = description.collectAsList();

    boolean hasId = false;
    boolean hasValue = false;
    boolean hasCountry = false;
    boolean hasCity = false;
    for (Row row : cols) {
      String colName = row.getString(0);
      if (colName == null || colName.isEmpty() || colName.startsWith("#")) {
        continue;
      }
      if ("id".equals(colName)) {
        hasId = true;
      }
      if ("value".equals(colName)) {
        hasValue = true;
      }
      if ("country".equals(colName)) {
        hasCountry = true;
      }
      if ("city".equals(colName)) {
        hasCity = true;
      }
    }

    assertTrue(hasId);
    assertTrue(hasValue);
    assertTrue(hasCountry);
    assertTrue(hasCity);

    spark.sql("INSERT INTO " + tableName + " VALUES (42, 3.14, 'CN', 'BJ')");

    Dataset<Row> result = spark.sql("SELECT * FROM " + tableName);
    assertEquals(1L, result.count());

    Row row = result.collectAsList().get(0);
    assertEquals(42L, row.getLong(0));
    assertEquals(3.14, row.getDouble(1), 0.001);
  }
}
