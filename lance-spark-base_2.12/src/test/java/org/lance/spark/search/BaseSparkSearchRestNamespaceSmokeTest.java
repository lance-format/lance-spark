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

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseSparkSearchRestNamespaceSmokeTest {
  private static final String CATALOG_NAME = "lance_rest_search";
  private SparkSession spark;

  @BeforeEach
  void setup() {
    String uri = System.getenv("LANCE_SPARK_REST_URI");
    String apiKey = System.getenv("LANCE_SPARK_REST_API_KEY");
    String database = System.getenv("LANCE_SPARK_REST_DATABASE");
    Assumptions.assumeTrue(uri != null && apiKey != null && database != null);

    spark =
        SparkSession.builder()
            .appName("lance-search-rest-namespace-smoke-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "rest")
            .config("spark.sql.catalog." + CATALOG_NAME + ".uri", uri)
            .config("spark.sql.catalog." + CATALOG_NAME + ".headers.x-api-key", apiKey)
            .config("spark.sql.catalog." + CATALOG_NAME + ".headers.x-lancedb-database", database)
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testVectorAndFullTextSearchViaRestNamespace() {
    String vectorTable = fullTableName("vector_search");
    spark.sql(
        "CREATE TABLE "
            + vectorTable
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.sql(
        "INSERT INTO "
            + vectorTable
            + " VALUES "
            + "(0, array(0.0, 0.0, 0.0, 0.0)), "
            + "(1, array(1.0, 1.0, 1.0, 1.0)), "
            + "(2, array(10.0, 10.0, 10.0, 10.0))");

    List<Row> vectorRows =
        spark
            .sql(
                "SELECT id, _distance FROM VECTOR_SEARCH('"
                    + vectorTable
                    + "', array(0.0, 0.0, 0.0, 0.0), 2) ORDER BY _distance, id")
            .collectAsList();
    assertEquals(2, vectorRows.size());
    assertEquals(0, vectorRows.get(0).getInt(0));
    assertEquals(1, vectorRows.get(1).getInt(0));

    String ftsTable = fullTableName("fts_search");
    spark.sql("CREATE TABLE " + ftsTable + " (id INT NOT NULL, body STRING) USING lance");
    spark.sql(
        "INSERT INTO "
            + ftsTable
            + " VALUES "
            + "(1, 'lance vector search'), "
            + "(2, 'spark connector table function'), "
            + "(3, 'lance full text search')");
    spark.sql(
        "ALTER TABLE "
            + ftsTable
            + " CREATE INDEX body_fts USING fts (body) WITH ("
            + "base_tokenizer='simple', "
            + "language='English', "
            + "max_token_length=40, "
            + "lower_case=true, "
            + "stem=false, "
            + "remove_stop_words=false, "
            + "ascii_folding=false, "
            + "with_position=true)");

    List<Row> ftsRows =
        spark
            .sql("SELECT id, _score FROM SEARCH('" + ftsTable + "', 'lance', 10) ORDER BY id")
            .collectAsList();
    List<Integer> ids = ftsRows.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
    assertEquals(java.util.Arrays.asList(1, 3), ids);
    assertTrue(ftsRows.get(0).getFloat(1) > 0.0f);

    String hybridTable = fullTableName("hybrid_search");
    spark.sql(
        "CREATE TABLE "
            + hybridTable
            + " (id INT NOT NULL, body STRING, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.sql(
        "INSERT INTO "
            + hybridTable
            + " VALUES "
            + "(1, 'lance vector search', array(0.0, 0.0, 0.0, 0.0)), "
            + "(2, 'spark connector table function', array(1.0, 1.0, 1.0, 1.0)), "
            + "(3, 'lance full text search', array(10.0, 10.0, 10.0, 10.0))");
    spark.sql(
        "ALTER TABLE "
            + hybridTable
            + " CREATE INDEX body_fts USING fts (body) WITH ("
            + "base_tokenizer='simple', "
            + "language='English', "
            + "max_token_length=40, "
            + "lower_case=true, "
            + "stem=false, "
            + "remove_stop_words=false, "
            + "ascii_folding=false, "
            + "with_position=true)");

    List<Row> hybridRows =
        spark
            .sql(
                "SELECT id, _distance, _score, _relevance_score FROM HYBRID_SEARCH("
                    + "table => '"
                    + hybridTable
                    + "', "
                    + "query_vector => array(0.0, 0.0, 0.0, 0.0), "
                    + "query => 'lance', "
                    + "columns => array('id'), "
                    + "num_results => 3, "
                    + "candidates => 3, "
                    + "rrf_k => 1.0) "
                    + "ORDER BY _relevance_score DESC, id")
            .collectAsList();
    List<Integer> hybridIds =
        hybridRows.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
    assertEquals(java.util.Arrays.asList(1, 3, 2), hybridIds);
    assertEquals(0.0f, hybridRows.get(0).getFloat(1), 0.001f);
    assertTrue(hybridRows.get(0).getFloat(2) > 0.0f);
    assertTrue(hybridRows.get(2).isNullAt(2));
  }

  private String fullTableName(String prefix) {
    return CATALOG_NAME
        + ".default."
        + prefix
        + "_"
        + UUID.randomUUID().toString().replace("-", "");
  }
}
