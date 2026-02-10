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
package org.lance.spark.cdf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for enable_stable_row_ids configuration at catalog level vs TBLPROPERTIES
 * level.
 *
 * <p>Each test manages its own SparkSession to control catalog-level configuration independently.
 */
public abstract class BaseCdfConfigTest {
  protected String catalogName = "lance_cfg";

  @TempDir protected Path tempDir;

  @Test
  public void testCatalogLevelConfigWithoutTblProperties() {
    // Catalog-level enable_stable_row_ids=true, no TBLPROPERTIES override
    SparkSession spark =
        SparkSession.builder()
            .appName("lance-cdf-config-test-catalog")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .config("spark.sql.catalog." + catalogName + ".enable_stable_row_ids", "true")
            .getOrCreate();

    try {
      String tableName = "cdf_cfg_" + UUID.randomUUID().toString().replace("-", "");
      String fqn = catalogName + ".default." + tableName;

      // CREATE TABLE without TBLPROPERTIES — relies on catalog default
      spark.sql(String.format("CREATE TABLE %s (id INT NOT NULL, name STRING, value INT)", fqn));

      // v2: Insert
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice', 100), (2, 'Bob', 200)", fqn));

      // v3: Update
      spark.sql(String.format("UPDATE %s SET value = value + 10 WHERE id = 1", fqn));

      // Verify version columns are populated
      List<Row> rows =
          spark
              .sql(
                  String.format(
                      "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                          + "FROM %s ORDER BY id",
                      fqn))
              .collectAsList();

      assertEquals(2, rows.size());

      // Alice: updated in v3
      Row alice = rows.get(0);
      assertEquals(1, alice.getInt(0));
      assertEquals(110, alice.getInt(2));
      assertEquals(1L, alice.getLong(3)); // created
      assertEquals(3L, alice.getLong(4)); // updated

      // Bob: untouched after fragment rewrite
      Row bob = rows.get(1);
      assertEquals(2, bob.getInt(0));
      assertEquals(200, bob.getInt(2));
      assertEquals(1L, bob.getLong(3)); // created
      assertEquals(1L, bob.getLong(4)); // updated
    } finally {
      spark.stop();
    }
  }

  @Test
  public void testTblPropertiesOverridesCatalogDefault() {
    // Catalog-level enable_stable_row_ids NOT SET (defaults to false),
    // TBLPROPERTIES overrides to true
    SparkSession spark =
        SparkSession.builder()
            .appName("lance-cdf-config-test-override")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "cdf_cfg_" + UUID.randomUUID().toString().replace("-", "");
      String fqn = catalogName + ".default." + tableName;

      // CREATE TABLE with TBLPROPERTIES overriding catalog default (false -> true)
      spark.sql(
          String.format(
              "CREATE TABLE %s (id INT NOT NULL, name STRING, value INT) "
                  + "TBLPROPERTIES ('enable_stable_row_ids' = 'true')",
              fqn));

      // v2: Insert
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice', 100), (2, 'Bob', 200)", fqn));

      // v3: Update
      spark.sql(String.format("UPDATE %s SET value = value + 10 WHERE id = 1", fqn));

      // Verify version columns are populated
      List<Row> rows =
          spark
              .sql(
                  String.format(
                      "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                          + "FROM %s ORDER BY id",
                      fqn))
              .collectAsList();

      assertEquals(2, rows.size());

      // Alice: updated in v3
      Row alice = rows.get(0);
      assertEquals(1, alice.getInt(0));
      assertEquals(110, alice.getInt(2));
      assertEquals(1L, alice.getLong(3)); // created
      assertEquals(3L, alice.getLong(4)); // updated

      // Bob: untouched after fragment rewrite
      Row bob = rows.get(1);
      assertEquals(2, bob.getInt(0));
      assertEquals(200, bob.getInt(2));
      assertEquals(1L, bob.getLong(3)); // created
      assertEquals(1L, bob.getLong(4)); // updated
    } finally {
      spark.stop();
    }
  }
}
