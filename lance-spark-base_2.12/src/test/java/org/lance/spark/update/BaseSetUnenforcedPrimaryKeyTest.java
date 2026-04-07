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

import org.lance.schema.LanceField;
import org.lance.spark.LanceRuntime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class BaseSetUnenforcedPrimaryKeyTest {
  protected String catalogName = "lance_test";
  protected String tableName = "set_pk_test";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;
  protected String tableDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-set-pk-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "set_pk_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
    this.tableDir =
        FileSystems.getDefault().getPath(testRoot, this.tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  private void createTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, name STRING NOT NULL, value DOUBLE) USING lance",
            fullTable));
    spark.sql(
        String.format(
            "INSERT INTO %s (id, name, value) VALUES (1, 'a', 1.0), (2, 'b', 2.0)", fullTable));
  }

  @Test
  public void testSetSingleColumnPrimaryKey() {
    createTable();

    Dataset<Row> result =
        spark.sql(String.format("ALTER TABLE %s SET UNENFORCED PRIMARY KEY (id)", fullTable));

    Row row = result.collectAsList().get(0);
    Assertions.assertEquals("OK", row.getString(0));
    Assertions.assertEquals("id", row.getString(1));
  }

  @Test
  public void testSetCompositeColumnPrimaryKey() {
    createTable();

    Dataset<Row> result =
        spark.sql(String.format("ALTER TABLE %s SET UNENFORCED PRIMARY KEY (id, name)", fullTable));

    Row row = result.collectAsList().get(0);
    Assertions.assertEquals("OK", row.getString(0));
    Assertions.assertEquals("id, name", row.getString(1));
  }

  @Test
  public void testPrimaryKeyPersistedAfterReopen() {
    createTable();

    spark.sql(String.format("ALTER TABLE %s SET UNENFORCED PRIMARY KEY (id, name)", fullTable));

    // Reopen the native dataset and verify PK metadata
    try (org.lance.Dataset dataset =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(tableDir).build()) {
      List<LanceField> fields = dataset.getLanceSchema().fields();
      LanceField idField =
          fields.stream().filter(f -> f.getName().equals("id")).findFirst().orElseThrow();
      LanceField nameField =
          fields.stream().filter(f -> f.getName().equals("name")).findFirst().orElseThrow();
      LanceField valueField =
          fields.stream().filter(f -> f.getName().equals("value")).findFirst().orElseThrow();

      assertPrimaryKey(idField, "1");
      assertPrimaryKey(nameField, "2");
      assertNotPrimaryKey(valueField);
    }
  }

  @Test
  public void testSetPrimaryKeyOnNonExistentColumn() {
    createTable();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s SET UNENFORCED PRIMARY KEY (nonexistent)", fullTable))
                    .collect());

    Assertions.assertTrue(
        exception.getMessage().contains("not found"),
        "Error should mention column not found: " + exception.getMessage());
  }

  @Test
  public void testSetPrimaryKeyOnNullableColumn() {
    createTable();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s SET UNENFORCED PRIMARY KEY (value)", fullTable))
                    .collect());

    Assertions.assertTrue(
        exception.getMessage().contains("nullable"),
        "Error should mention nullable: " + exception.getMessage());
  }

  @Test
  public void testSetPrimaryKeyOnStructColumn() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, info STRUCT<first: STRING, last: STRING> NOT NULL)"
                + " USING lance",
            fullTable));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES (1, named_struct('first', 'John', 'last', 'Doe'))", fullTable));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s SET UNENFORCED PRIMARY KEY (info)", fullTable))
                    .collect());

    Assertions.assertTrue(
        exception.getMessage().contains("not a leaf field"),
        "Error should mention not a leaf field: " + exception.getMessage());
  }

  @Test
  public void testSetPrimaryKeyWhenAlreadySet() {
    createTable();

    spark.sql(String.format("ALTER TABLE %s SET UNENFORCED PRIMARY KEY (id)", fullTable));

    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s SET UNENFORCED PRIMARY KEY (name)", fullTable))
                    .collect());

    Assertions.assertTrue(
        exception.getMessage().contains("already has unenforced primary key"),
        "Error should mention already set: " + exception.getMessage());
  }

  private static boolean isPrimaryKey(LanceField field) {
    if (field.isUnenforcedPrimaryKey()) {
      return true;
    }
    Map<String, String> metadata = field.getMetadata();
    if (metadata == null) {
      return false;
    }
    return "true".equalsIgnoreCase(metadata.get("lance-schema:unenforced-primary-key"));
  }

  private static void assertPrimaryKey(LanceField field, String expectedPosition) {
    Assertions.assertTrue(isPrimaryKey(field), field.getName() + " should be a primary key");
    Map<String, String> metadata = field.getMetadata();
    Assertions.assertEquals(
        expectedPosition,
        metadata.get("lance-schema:unenforced-primary-key:position"),
        field.getName() + " PK position should be " + expectedPosition);
  }

  private static void assertNotPrimaryKey(LanceField field) {
    Assertions.assertFalse(isPrimaryKey(field), field.getName() + " should not be a primary key");
  }
}
