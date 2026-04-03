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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for BaseLanceNamespaceSparkCatalog using DirectoryNamespace implementation. */
public abstract class BaseTestSparkDirectoryNamespace extends SparkLanceNamespaceTestBase {

  @Override
  protected String getNsImpl() {
    return "dir";
  }

  @Override
  protected Map<String, String> getAdditionalNsConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put("root", tempDir.toString());
    // Default is multi-level namespace mode (manifest mode)
    // No need to set single_level_ns since false is the default
    return configs;
  }

  @BeforeEach
  @Override
  void setup() throws IOException {
    super.setup();
    // Create the "default" namespace explicitly so that DirectoryNamespace uses manifest mode
    // instead of directory listing mode. This is required for deregisterTable to work correctly.
    spark.sql("CREATE NAMESPACE " + catalogName + ".default");
  }

  @Test
  public void testTableUsesHashPrefixedPathInNamespace() {
    String tableName = generateTableName("hash_path_test");
    String fullName = catalogName + ".default." + tableName;

    // Create table in default namespace
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT NOT NULL, name STRING)");

    // Verify table exists
    assertTrue(
        catalog.tableExists(
            org.apache.spark.sql.connector.catalog.Identifier.of(
                new String[] {"default"}, tableName)));

    // Verify the table is NOT stored with simple naming like {table_name}.lance
    File simpleNameDir = new File(tempDir.toFile(), tableName + ".lance");
    assertFalse(
        simpleNameDir.exists(),
        "Table should NOT be stored at "
            + simpleNameDir.getPath()
            + " - manifest mode should use hash-prefixed paths");

    // Verify there's a directory with hash-prefixed naming pattern: {hash}_{namespace}${table_name}
    File[] files = tempDir.toFile().listFiles();
    boolean foundHashPrefixedDir = false;
    String expectedSuffix = "_default$" + tableName;
    for (File file : files) {
      if (file.isDirectory() && file.getName().contains(expectedSuffix)) {
        foundHashPrefixedDir = true;
        // Verify it matches the pattern: 8 hex chars followed by underscore then object_id
        String name = file.getName();
        String prefix = name.substring(0, name.indexOf(expectedSuffix));
        assertTrue(
            prefix.matches("[0-9a-f]{8}"),
            "Directory prefix should be 8 hex chars, got: " + prefix);
        break;
      }
    }
    assertTrue(
        foundHashPrefixedDir,
        "Should find a hash-prefixed directory ending with " + expectedSuffix);
  }

  @Test
  public void testCreateExternalTableWithLocation() throws Exception {
    // existing data
    String tableName = generateTableName("external_table");
    String fullTableName = catalogName + ".default." + tableName;
    String externalDatasetPath =
        TestUtils.getDatasetUri(
            TestUtils.TestTable1Config.dbPath, TestUtils.TestTable1Config.datasetName);
    spark.sql(
        "CREATE TABLE " + fullTableName + " USING lance LOCATION '" + externalDatasetPath + "'");
    // schema are provided by the lance table in location, other than definition statement
    assertTrue(
        catalog.tableExists(
            org.apache.spark.sql.connector.catalog.Identifier.of(
                new String[] {"default"}, tableName)));

    Dataset<Row> result = spark.sql("SELECT * FROM " + fullTableName);
    assertEquals(4, result.count());
    // Verify the data is correct (from the original dataset)
    List<Row> rows = result.collectAsList();
    List<List<Long>> expected = TestUtils.TestTable1Config.expectedValues;
    assertEquals(expected.get(0).get(0), rows.get(0).getLong(0));
    assertEquals(expected.get(1).get(0), rows.get(1).getLong(0));
    assertEquals(expected.get(1).get(1), rows.get(1).getLong(1));

    // none existing
    String tableNameWithoutExisting = generateTableName("external_table");
    String fullTableNameWithoutExisting = catalogName + ".default." + tableNameWithoutExisting;
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "CREATE TABLE "
                    + fullTableNameWithoutExisting
                    + " USING lance  LOCATION '"
                    + TestUtils.TestTable1Config.dbPath
                    + "/"
                    + generateTableName("never_exist")
                    + "'"));
  }

  @Test
  public void testDuplicateRegistrationAtSameLocation() throws Exception {
    String externalDatasetPath =
        TestUtils.getDatasetUri(
            TestUtils.TestTable1Config.dbPath, TestUtils.TestTable1Config.datasetName);

    // Register first table at the location
    String tableName1 = generateTableName("dup_reg1");
    String fullTableName1 = catalogName + ".default." + tableName1;
    spark.sql(
        "CREATE TABLE " + fullTableName1 + " USING lance LOCATION '" + externalDatasetPath + "'");
    assertEquals(4, spark.sql("SELECT * FROM " + fullTableName1).count());

    // Register a second table pointing at the exact same location
    String tableName2 = generateTableName("dup_reg2");
    String fullTableName2 = catalogName + ".default." + tableName2;
    spark.sql(
        "CREATE TABLE " + fullTableName2 + " USING lance LOCATION '" + externalDatasetPath + "'");
    assertEquals(4, spark.sql("SELECT * FROM " + fullTableName2).count());

    // Both tables should be visible
    assertTrue(
        catalog.tableExists(
            org.apache.spark.sql.connector.catalog.Identifier.of(
                new String[] {"default"}, tableName1)));
    assertTrue(
        catalog.tableExists(
            org.apache.spark.sql.connector.catalog.Identifier.of(
                new String[] {"default"}, tableName2)));

    // Dropping one should not affect the other
    spark.sql("DROP TABLE " + fullTableName1);
    assertFalse(
        catalog.tableExists(
            org.apache.spark.sql.connector.catalog.Identifier.of(
                new String[] {"default"}, tableName1)));
    assertEquals(4, spark.sql("SELECT * FROM " + fullTableName2).count());

    // duplicate name should fail
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "CREATE TABLE "
                    + fullTableName2
                    + " USING lance LOCATION '"
                    + externalDatasetPath
                    + "'"));
  }
}
