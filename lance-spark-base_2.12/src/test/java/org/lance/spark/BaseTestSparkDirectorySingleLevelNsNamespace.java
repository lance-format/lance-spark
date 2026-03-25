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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for DirectoryNamespace in single-level namespace mode (no manifest). This exercises the
 * dir.rs "Table does not exist" error path, which is different from the manifest.rs "Table '...'
 * not found" path tested in {@link BaseTestSparkDirectoryNamespace}.
 */
public abstract class BaseTestSparkDirectorySingleLevelNsNamespace {
  private SparkSession spark;
  private TableCatalog catalog;
  private final String catalogName = "lance_single_level";

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-single-level-ns-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testTableExistsReturnsFalseForNonExistentTable() {
    // In single-level mode, this goes through dir.rs check_table_status
    // which produces "Table does not exist: {table_name}"
    assertFalse(catalog.tableExists(Identifier.of(new String[] {"default"}, "non_existent_table")));
  }

  @Test
  public void testLoadNonExistentTableThrowsNoSuchTableException() {
    // Verifies that the "Table does not exist" message from dir.rs
    // is correctly translated to NoSuchTableException
    assertThrows(
        NoSuchTableException.class,
        () -> catalog.loadTable(Identifier.of(new String[] {"default"}, "non_existent_table")));
  }
}
