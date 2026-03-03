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

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public abstract class BaseMergeIntoTest {
  private static final int SHUFFLE_PARTITIONS = 4;

  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-merge-into-distribution-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .config("spark.sql.shuffle.partitions", String.valueOf(SHUFFLE_PARTITIONS))
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.default.parallelism", String.valueOf(SHUFFLE_PARTITIONS))
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testMergeIntoInsertDistributionOnNullSegmentId() {
    String tableName = "merge_dist_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName
            + " (id INT NOT NULL, value INT, tag STRING)");

    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 10, 'base'), "
            + "(2, 20, 'base'), "
            + "(3, 30, 'base'), "
            + "(4, 40, 'base'), "
            + "(5, 50, 'base'), "
            + "(6, 60, 'base')");

    // Build merge source with update/delete rows plus insert-only rows (null _fragid path).
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, 110),
                RowFactory.create(2, 120),
                RowFactory.create(3, 130),
                RowFactory.create(4, 140),
                RowFactory.create(5, null),
                RowFactory.create(6, null)),
            new org.apache.spark.sql.types.StructType().add("id", "int").add("value", "int"))
        .union(
            spark
                .range(0, 200)
                .repartition(SHUFFLE_PARTITIONS)
                .selectExpr("cast(id + 1000 as int) as id", "cast(id as int) as value"))
        .createOrReplaceTempView("merge_source");

    // MERGE triggers delete/update/insert branches in a single run.
    spark.sql(
        "MERGE INTO "
            + catalogName
            + ".default."
            + tableName
            + " t USING merge_source s ON t.id = s.id "
            + "WHEN MATCHED AND s.value IS NULL THEN DELETE "
            + "WHEN MATCHED THEN UPDATE SET value = s.value, tag = 'updated' "
            + "WHEN NOT MATCHED THEN INSERT (id, value, tag) VALUES (s.id, s.value, 'inserted')");

    long insertedRowCount =
        spark
            .sql(
                "SELECT COUNT(*) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE tag = 'inserted'")
            .first()
            .getLong(0);
    Assertions.assertEquals(
        200L, insertedRowCount, "Expected merge to insert 200 rows into new fragments");
    long updatedCount =
        spark
            .sql(
                "SELECT COUNT(*) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE tag = 'updated'")
            .first()
            .getLong(0);
    Assertions.assertEquals(4L, updatedCount, "Expected 4 updated rows");

    long deletedCount =
        spark
            .sql(
                "SELECT COUNT(*) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE id IN (5, 6)")
            .first()
            .getLong(0);
    Assertions.assertEquals(0L, deletedCount, "Expected rows 5 and 6 to be deleted");

    // Inserted rows should span multiple fragments to avoid skew.
    long insertFragmentCount =
        spark
            .sql(
                "SELECT COUNT(DISTINCT _fragid) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE tag = 'inserted'")
            .first()
            .getLong(0);
    Assertions.assertTrue(
        insertFragmentCount >= 2, "Expected inserted rows to span multiple fragments");
  }

  @Test
  public void testMergeInto() {
    String tableName = "merge_result_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(
        "CREATE TABLE " + catalogName + ".default." + tableName + " (id INT NOT NULL, value INT)");

    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 10), "
            + "(2, 20), "
            + "(3, 30), "
            + "(4, 40), "
            + "(5, 50)");

    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, 110),
                RowFactory.create(2, 120),
                RowFactory.create(3, null),
                RowFactory.create(100, 1000),
                RowFactory.create(101, 1010)),
            new org.apache.spark.sql.types.StructType().add("id", "int").add("value", "int"))
        .createOrReplaceTempView("merge_result_source");

    spark.sql(
        "MERGE INTO "
            + catalogName
            + ".default."
            + tableName
            + " t USING merge_result_source s ON t.id = s.id "
            + "WHEN MATCHED AND s.value IS NULL THEN DELETE "
            + "WHEN MATCHED THEN UPDATE SET value = s.value "
            + "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)");

    List<org.apache.spark.sql.Row> actual =
        spark
            .sql("SELECT id, value FROM " + catalogName + ".default." + tableName + " ORDER BY id")
            .collectAsList();
    List<org.apache.spark.sql.Row> expected =
        Arrays.asList(
            RowFactory.create(1, 110),
            RowFactory.create(2, 120),
            RowFactory.create(4, 40),
            RowFactory.create(5, 50),
            RowFactory.create(100, 1000),
            RowFactory.create(101, 1010));
    Assertions.assertEquals(expected, actual, "Expected merged rows to match result set");
  }
}
