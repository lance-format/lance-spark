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

import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
                .range(0, 2000)
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
        2000L, insertedRowCount, "Expected merge to insert 2000 rows into new fragments");
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
    List<org.apache.spark.sql.Row> fragStats =
        spark
            .sql(
                "SELECT _fragid, COUNT(*) as cnt FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE tag = 'inserted' GROUP BY _fragid ORDER BY _fragid")
            .collectAsList();
    long insertFragmentCount = fragStats.size();
    Assertions.assertTrue(
        insertFragmentCount >= 2,
        "Expected inserted rows to span multiple fragments, but got "
            + insertFragmentCount
            + " fragment(s). Distribution: "
            + fragStats
            + ". master="
            + spark.sparkContext().master()
            + ", shuffle.partitions="
            + spark.conf().get("spark.sql.shuffle.partitions"));
  }

  /**
   * Pins down per-branch version-column behavior of MERGE INTO on a stable-row-id table:
   *
   * <ul>
   *   <li>UPDATE branch: advances {@code _row_last_updated_at_version}. MERGE rewrites the matched
   *       row into the merge commit, so {@code _row_created_at_version} matches {@code
   *       _row_last_updated_at_version} for the replacement row.
   *   <li>DELETE branch: row disappears.
   *   <li>Untouched rows: both versions preserved.
   *   <li>INSERT branch: inserted rows are created in the merge commit, so {@code
   *       _row_created_at_version} matches {@code _row_last_updated_at_version}.
   * </ul>
   */
  @Test
  public void testMergeIntoTracksVersionColumnsPerBranch() {
    String tableName = "merge_versions_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, value INT) "
                + "TBLPROPERTIES ('enable_stable_row_ids' = 'true')",
            fullTable));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30), (4, 40)", fullTable));

    Map<Integer, Long> beforeLastUpdated = new HashMap<>();
    Long initialInsertVersion = null;
    for (org.apache.spark.sql.Row row :
        spark
            .sql(
                String.format(
                    "SELECT id, _row_created_at_version, _row_last_updated_at_version "
                        + "FROM %s ORDER BY id",
                    fullTable))
            .collectAsList()) {
      beforeLastUpdated.put(row.getInt(0), row.getLong(2));
      if (initialInsertVersion == null) {
        initialInsertVersion = row.getLong(1);
      }
    }
    Assertions.assertNotNull(initialInsertVersion, "initial insert version must be observable");

    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, 110), // UPDATE branch
                RowFactory.create(2, null), // DELETE branch
                RowFactory.create(5, 50), // INSERT branch
                RowFactory.create(6, 60)), // INSERT branch
            new org.apache.spark.sql.types.StructType().add("id", "int").add("value", "int"))
        .createOrReplaceTempView("merge_version_source");

    spark.sql(
        String.format(
            "MERGE INTO %s t USING merge_version_source s ON t.id = s.id "
                + "WHEN MATCHED AND s.value IS NULL THEN DELETE "
                + "WHEN MATCHED THEN UPDATE SET value = s.value "
                + "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
            fullTable));

    List<org.apache.spark.sql.Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, value, _row_created_at_version, _row_last_updated_at_version "
                        + "FROM %s ORDER BY id",
                    fullTable))
            .collectAsList();

    // id=2 deleted; surviving ids: 1, 3, 4, 5, 6.
    Assertions.assertEquals(5, rows.size(), "expected 5 surviving rows after merge");

    Long mergeCommitLastUpdated = null;
    for (org.apache.spark.sql.Row row : rows) {
      int id = row.getInt(0);
      long createdAt = row.getLong(2);
      long lastUpdated = row.getLong(3);
      switch (id) {
        case 1:
          // UPDATE branch: last_updated advances; replacement row is created by the merge commit.
          Assertions.assertTrue(
              lastUpdated > beforeLastUpdated.get(1),
              "id=1 last_updated must advance across UPDATE branch (before="
                  + beforeLastUpdated.get(1)
                  + ", after="
                  + lastUpdated
                  + ")");
          Assertions.assertEquals(
              lastUpdated, createdAt, "id=1 created_at must match the merge commit");
          mergeCommitLastUpdated = lastUpdated;
          break;
        case 3:
        case 4:
          // Untouched rows: not hit by any branch — both versions preserved.
          Assertions.assertEquals(
              initialInsertVersion.longValue(),
              createdAt,
              "id=" + id + " created_at must be preserved (no branch matched)");
          Assertions.assertEquals(
              beforeLastUpdated.get(id).longValue(),
              lastUpdated,
              "id=" + id + " last_updated must be preserved (no branch matched)");
          break;
        case 5:
        case 6:
          // INSERT branch sharing the merge commit: created_at and last_updated both equal the
          // UPDATE branch's last_updated (single commit).
          Assertions.assertEquals(
              mergeCommitLastUpdated == null ? lastUpdated : mergeCommitLastUpdated.longValue(),
              lastUpdated,
              "id=" + id + " inserted row last_updated must match the merge commit");
          Assertions.assertEquals(
              lastUpdated, createdAt, "id=" + id + " inserted row created_at must match merge");
          break;
        default:
          Assertions.fail("unexpected surviving id=" + id);
      }
    }
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

  /**
   * End-to-end smoke test for DELETE on a Lance table with a UDT column. Verifies the catalog path
   * ({@code saveAsTable} → {@code createTable} → {@code toArrowSchema}) accepts a UDT-rooted schema
   * and that position-delta DELETE handles UDT-derived struct metadata.
   *
   * <p>Intentionally narrow — what this does NOT cover:
   *
   * <ul>
   *   <li>PR #471's UDT case in {@code LanceArrowWriter.createFieldWriter} — the catalog unwraps
   *       UDT to sqlType before the writer sees it. Direct coverage: {@code testVectorUDTRoundtrip}
   *       in {@code BaseSparkDataTypeRoundtripTest}.
   *   <li>PR #549's nullable-relaxation — DELETE only records position bitmaps, never calls {@code
   *       writer.write()}. UPDATE / MERGE would, but Spark V2's analyzer ({@code
   *       TableOutputResolver.canWrite}) rejects UDT assignments before lance-spark code runs.
   * </ul>
   */
  @Test
  public void testDeleteOnVectorUDTColumn() {
    String tableName = "delete_udt_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;
    VectorUDT vectorUDT = new VectorUDT();
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("vec", vectorUDT, true);

    Vector v1 = new DenseVector(new double[] {1.0, 2.0, 3.0});
    Vector v2 = new DenseVector(new double[] {4.0, 5.0, 6.0});
    Vector v3 = new DenseVector(new double[] {7.0, 8.0, 9.0});
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, v1), RowFactory.create(2, v2), RowFactory.create(3, v3)),
            schema)
        .write()
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable(fullTable);

    spark.sql("DELETE FROM " + fullTable + " WHERE id = 2");

    List<Integer> ids =
        spark.sql("SELECT id FROM " + fullTable + " ORDER BY id").collectAsList().stream()
            .map(r -> r.getInt(0))
            .collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList(1, 3), ids, "expected id=2 to be deleted");
  }
}
