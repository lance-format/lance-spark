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

/**
 * Regression tests for LOCATION support in BaseLanceNamespaceSparkCatalog.
 *
 * <p>Custom LOCATION for new tables requires a REST namespace backend (e.g., Gravitino) that
 * supports DeclareTableRequest.setLocation(). The DirectoryNamespace backend enforces server-
 * assigned paths and rejects custom locations. Full LOCATION tests run as integration tests against
 * a live REST namespace server.
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
}
