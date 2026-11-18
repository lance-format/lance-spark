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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for distributed VECTOR_SEARCH (the path through {@link
 * LanceDistributedSearchTable}).
 *
 * <p>This class covers the fallback-only path: with no vector index on the table, {@link
 * LanceDistributedSearchScan} emits one fallback partition per fragment and each Spark task runs
 * flat KNN. Indexed units are covered by {@code BaseSparkDistributedVectorSearchIndexTest}, which
 * builds real index segments through the Java API.
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

  @Test
  void distanceBoundsAreRejectedByDistributedSearch() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
    String fullName = createFiveFragmentTable();
    String sql =
        "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
            + fullName
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 5, "
            + "lower_bound => 0.5, upper_bound => 5.0)";

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "false");
    List<Row> rows = spark.sql(sql).collectAsList();
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(4.0f, rows.get(0).getFloat(1), 1e-4f);

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    Exception error = assertThrows(Exception.class, () -> spark.sql(sql).collectAsList());
    assertTrue(
        rootMessage(error)
            .contains("Distributed VECTOR_SEARCH does not support lower_bound or upper_bound"),
        rootMessage(error));
  }

  @Test
  void aFilterRequiresAnExplicitPrefilterChoice() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
    String fullName = createFiveFragmentTable();
    String filtered =
        "SELECT id FROM VECTOR_SEARCH(table => '"
            + fullName
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 1, filter => 'id = 4'";

    // Namespace execution defaults to prefilter=false: it takes the top 1 (id 0), then applies
    // the filter, so nothing comes back. This is the semantics a distributed plan cannot offer,
    // because a fragment-restricted scan has to prefilter.
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "false");
    assertEquals(
        0,
        spark.sql(filtered + ")").collectAsList().size(),
        "namespace execution post-filters the top k");

    // Silently prefiltering instead would return id 4 here, so the query is rejected until the
    // user says which semantics they want.
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    Exception error =
        assertThrows(Exception.class, () -> spark.sql(filtered + ")").collectAsList());
    assertTrue(rootMessage(error).contains("requires prefilter=true"), rootMessage(error));

    List<Row> prefiltered = spark.sql(filtered + ", prefilter => true)").collectAsList();
    assertEquals(1, prefiltered.size(), "prefilter=true yields the true filtered top k");
    assertEquals(4, prefiltered.get(0).getInt(0));
  }

  @Test
  void tiedDistancesPaginateTheSameWayEveryRun() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
    // Identical vectors spread over several fragments: every row ties on `_distance`, so sorting
    // on distance alone leaves the k and offset boundaries up to task arrival order and the two
    // pages overlap or skip rows between runs. lance-core sorts by (distance, row id).
    String fullName = CATALOG_NAME + ".default.dist_ties";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    for (int fragment = 0; fragment < 4; fragment++) {
      int start = fragment * 3;
      spark.sql(
          "INSERT INTO "
              + fullName
              + " SELECT CAST(id AS INT), array(1.0, 1.0, 1.0, 1.0) FROM range("
              + start
              + ", "
              + (start + 3)
              + ", 1, 1)");
    }

    String page =
        "SELECT id FROM VECTOR_SEARCH(table => '"
            + fullName
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 5";
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");

    List<Integer> firstPage = idsOf(page + ")");
    List<Integer> secondPage = idsOf(page + ", offset => 5)");
    assertEquals(5, firstPage.size());
    assertEquals(5, secondPage.size());
    for (int run = 0; run < 4; run++) {
      assertEquals(firstPage, idsOf(page + ")"), "page 1 must not move between runs");
      assertEquals(secondPage, idsOf(page + ", offset => 5)"), "page 2 must not move");
    }
    List<Integer> combined = new ArrayList<>(firstPage);
    combined.addAll(secondPage);
    assertEquals(
        combined.size(),
        new java.util.HashSet<>(combined).size(),
        "the two pages must not repeat a row: " + combined);
  }

  private List<Integer> idsOf(String sql) {
    List<Integer> ids = new ArrayList<>();
    for (Row row : spark.sql(sql).collectAsList()) {
      ids.add(row.getInt(0));
    }
    return ids;
  }

  private static String rootMessage(Throwable throwable) {
    Throwable root = throwable;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return String.valueOf(root.getMessage());
  }
}
