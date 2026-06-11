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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code CREATE TABLE ... LOCATION} support in {@link BaseLanceNamespaceSparkCatalog}.
 *
 * <p>Custom LOCATION (both creating a new dataset at a path and registering an existing one) is
 * only meaningful for namespace backends that accept caller-supplied locations, such as the
 * REST/Gravitino backend. The {@code DirectoryNamespace} backend used by these unit tests owns its
 * storage layout and requires every table to live at a server-assigned, hash-prefixed path under
 * the catalog root: it rejects a custom location for a new table outright, and only round-trips
 * locations relative to the catalog root, so an absolute external location registers but is not
 * readable. The positive create-new and register-existing paths are therefore exercised by
 * integration tests against a live REST namespace server; the tests below cover the
 * directory-backend regression (no LOCATION) and the directory-backend boundaries.
 */
public abstract class BaseTestLocationSupport extends BaseTestSparkDirectoryNamespace {

  @Test
  public void testCreateTableWithoutLocationUnchanged() {
    String tableName = generateTableName("loc_managed");
    String fullName = catalogName + ".default." + tableName;

    spark.sql("CREATE TABLE " + fullName + " (id INT, name STRING)");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'alice'), (2, 'bob')");

    Dataset<Row> result = spark.sql("SELECT * FROM " + fullName + " ORDER BY id");
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("alice", rows.get(0).getString(1));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("bob", rows.get(1).getString(1));
  }

  @Test
  public void testCreateTableWithLocationOnNewPathRejectedByDirectoryNamespace() {
    String fullName = catalogName + ".default." + generateTableName("loc_new");
    String customLocation = tempDir.resolve("custom_new.lance").toString();

    // DirectoryNamespace assigns its own storage path, so declaring a table at a custom location
    // is rejected. The catalog wraps this in a runtime exception.
    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    "CREATE TABLE "
                        + fullName
                        + " (id INT) USING lance LOCATION '"
                        + customLocation
                        + "'"));
    assertTrue(
        rootCauseMessage(ex).contains("must be at location"),
        "Expected DirectoryNamespace to reject a custom location, but got: " + ex);
  }

  @Test
  public void testCreateTableRegisteringExistingDatasetNotReadableOnDirectoryNamespace() {
    // Produce a real Lance dataset by creating a managed table, then read back its absolute path.
    String src = generateTableName("loc_src");
    String srcFull = catalogName + ".default." + src;
    spark.sql("CREATE TABLE " + srcFull + " (id INT, name STRING)");
    spark.sql("INSERT INTO " + srcFull + " VALUES (1, 'alice')");
    String existingLocation = locationOf(srcFull);

    // Pointing LOCATION at the existing dataset takes the register-existing branch. The
    // DirectoryNamespace records the location as a path relative to the catalog root, so an
    // absolute external location does not round-trip: registration is accepted but the table is
    // not readable. (REST backends accept absolute external locations; see class doc.)
    String regFull = catalogName + ".default." + generateTableName("loc_reg");
    spark.sql(
        "CREATE TABLE "
            + regFull
            + " (id INT, name STRING) USING lance LOCATION '"
            + existingLocation
            + "'");
    assertThrows(Exception.class, () -> spark.sql("SELECT * FROM " + regFull).collectAsList());
  }

  /** Reads the on-disk location of an already-created table via DESCRIBE EXTENDED. */
  private String locationOf(String fullTableName) {
    List<Row> rows =
        spark
            .sql("DESCRIBE EXTENDED " + fullTableName)
            .filter("col_name = 'Location'")
            .collectAsList();
    return rows.get(0).getString(1);
  }

  private static String rootCauseMessage(Throwable t) {
    Throwable cur = t;
    StringBuilder sb = new StringBuilder();
    while (cur != null) {
      if (cur.getMessage() != null) {
        sb.append(cur.getMessage()).append(' ');
      }
      cur = cur.getCause();
    }
    return sb.toString();
  }
}
