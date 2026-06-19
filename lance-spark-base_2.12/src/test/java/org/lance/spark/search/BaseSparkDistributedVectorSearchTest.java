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
package org.lance.spark.search;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for distributed VECTOR_SEARCH (the path through {@link LanceSearchTable} with
 * {@code distributed=true}).
 *
 * <p>These tests cover the fallback-only path: with no vector index built on the table, {@link
 * LanceSearchScan} emits one fallback partition per fragment and each Spark task runs flat KNN.
 * Indexed-unit behavior (IVF_PQ etc.) is intentionally not exercised here yet — see the
 * {@code @Disabled} placeholder at the bottom of this class.
 */
public abstract class BaseSparkDistributedVectorSearchTest {
  private static final String CATALOG_NAME = "lance_dist_search";
  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-distributed-vector-search-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  /**
   * Build a 5-fragment table with no vector index. The planner emits five fallback units, one per
   * fragment. Each fragment has a single row; vectors are spaced so the closest to (0,0,0,0) is id
   * 0, then 1, then 2, etc.
   */
  private String createFiveFragmentTable() {
    String fullName = CATALOG_NAME + ".default.dist_vec";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.sql("INSERT INTO " + fullName + " VALUES (0, array(0.0, 0.0, 0.0, 0.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, array(1.0, 1.0, 1.0, 1.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (2, array(2.0, 2.0, 2.0, 2.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (3, array(3.0, 3.0, 3.0, 3.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (4, array(4.0, 4.0, 4.0, 4.0))");
    return fullName;
  }

  @Test
  void distributedAndSinglePartitionAgreeOnTopK() {
    String fullName = createFiveFragmentTable();
    String sql =
        "SELECT id, _distance FROM VECTOR_SEARCH('"
            + fullName
            + "', array(0.0, 0.0, 0.0, 0.0), 5) ORDER BY _distance, id";

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "false");
    List<Row> single = new ArrayList<>(spark.sql(sql).collectAsList());

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> distributed = new ArrayList<>(spark.sql(sql).collectAsList());

    Comparator<Row> byIdThenDist =
        Comparator.<Row, Integer>comparing(r -> r.getInt(0))
            .thenComparingDouble(r -> (double) r.getFloat(1));
    single.sort(byIdThenDist);
    distributed.sort(byIdThenDist);

    assertEquals(single.size(), distributed.size(), "result row count must match");
    for (int i = 0; i < single.size(); i++) {
      assertEquals(
          single.get(i).getInt(0), distributed.get(i).getInt(0), "row " + i + " id mismatch");
      assertEquals(
          single.get(i).getFloat(1),
          distributed.get(i).getFloat(1),
          1e-3f,
          "row " + i + " _distance mismatch");
    }
  }

  @Test
  void distributedReturnsExactlyKRows() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 3)")
            .collectAsList();
    assertEquals(3, rows.size());
  }

  @Test
  void distributedKLargerThanRowsReturnsAllRows() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 100)")
            .collectAsList();
    assertEquals(5, rows.size());
  }

  @Test
  void distributedEmptyTableReturnsZeroRows() {
    String fullName = CATALOG_NAME + ".default.empty_vec";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 5)")
            .collectAsList();
    assertEquals(0, rows.size());
  }

  @Test
  void distributedNearestRowIsClosest() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM VECTOR_SEARCH('"
                    + fullName
                    + "', array(0.0, 0.0, 0.0, 0.0), 1)")
            .collectAsList();
    assertEquals(1, rows.size());
    assertEquals(0, rows.get(0).getInt(0), "closest id to origin should be 0");
    assertEquals(0.0f, rows.get(0).getFloat(1), 1e-4f);
  }

  /**
   * TODO: end-to-end coverage for indexed-unit path with IVF_PQ.
   *
   * <p>lance-spark does not yet support {@code CREATE INDEX ... USING IVF_PQ} via SQL; once that
   * DDL is wired through, this test should:
   *
   * <ol>
   *   <li>Build a table large enough for IVF_PQ training (>= 256 rows).
   *   <li>{@code CREATE INDEX ... USING IVF_PQ} on the vector column.
   *   <li>Enable {@code spark.sql.lance.search.distributed.enabled=true}.
   *   <li>Run VECTOR_SEARCH and assert {@link LanceSearchScan} planned indexed units (one per
   *       segment) and the merged top-k matches the non-distributed reference run.
   * </ol>
   */
  @Test
  @Disabled("TODO: pending CREATE INDEX ... USING IVF_PQ support in lance-spark SQL extensions")
  void distributedIndexedUnitWithIvfPqIndex() {
    // Intentionally empty until IVF_PQ DDL is available.
  }
}
