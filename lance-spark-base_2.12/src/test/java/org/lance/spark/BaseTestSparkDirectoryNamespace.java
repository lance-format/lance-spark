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

import org.lance.namespace.errors.UnsupportedOperationException;

import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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
  @Override
  public void testRenameTable() {
    String oldName = generateTableName("rename_old");
    String fullOld = catalogName + ".default." + oldName;
    String fullNew = catalogName + ".default." + generateTableName("rename_new");

    spark.sql("CREATE TABLE " + fullOld + " (id BIGINT NOT NULL, name STRING)");

    // DirectoryNamespace does not support rename. The Lance namespace layer throws
    // o.l.n.e.UnsupportedOperationException directly — Spark does not intercept or
    // wrap this exception type, so it propagates as-is.
    UnsupportedOperationException ex1 =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              spark.sql("ALTER TABLE " + fullOld + " RENAME TO " + fullNew);
            });
    assertTrue(
        ex1.getMessage().contains("Not supported: renameTable"),
        "Expected 'Not supported: renameTable' but got: " + ex1.getMessage());
  }

  @Test
  @Override
  public void testRenameNonExistentTableFails() {
    String fullOld = catalogName + ".default." + generateTableName("nonexistent");
    String fullNew = catalogName + ".default." + generateTableName("new_target");

    // Spark's analyzer resolves the source table before the rename execution plan runs.
    // When the table doesn't exist, the analyzer throws ExtendedAnalysisException with
    // TABLE_OR_VIEW_NOT_FOUND — the catalog's renameTable() is never reached,
    // so the "unsupported" error is never triggered.
    AnalysisException ex =
        assertThrows(
            AnalysisException.class,
            () -> {
              spark.sql("ALTER TABLE " + fullOld + " RENAME TO " + fullNew);
            });
    assertEquals("TABLE_OR_VIEW_NOT_FOUND", ex.getErrorClass());
  }

  @Test
  @Override
  public void testRenameTableToExistingNameFails() {
    String full1 = catalogName + ".default." + generateTableName("rename_src");
    String full2 = catalogName + ".default." + generateTableName("rename_dst");

    spark.sql("CREATE TABLE " + full1 + " (id BIGINT NOT NULL)");
    spark.sql("CREATE TABLE " + full2 + " (id BIGINT NOT NULL)");

    // DirectoryNamespace does not support rename — same error regardless of whether
    // the target exists. The Lance namespace layer rejects the operation before
    // checking for conflicts.
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              spark.sql("ALTER TABLE " + full1 + " RENAME TO " + full2);
            });
    assertTrue(
        ex.getMessage().contains("Not supported: renameTable"),
        "Expected 'Not supported: renameTable' but got: " + ex.getMessage());
  }
}
