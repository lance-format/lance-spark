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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseSparkSearchTableFunctionTest {
  private static final String CATALOG_NAME = "lance_search";
  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-search-table-function-test")
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
  void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testVectorSearchTableFunction() {
    String fullName = createVectorTable();

    Dataset<Row> result =
        spark.sql(
            "SELECT id, _distance FROM VECTOR_SEARCH('"
                + fullName
                + "', array(0.0, 0.0, 0.0, 0.0), 2) ORDER BY _distance, id");

    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(0, rows.get(0).getInt(0));
    assertEquals(1, rows.get(1).getInt(0));
    assertEquals(0.0f, rows.get(0).getFloat(1), 0.001f);
    assertTrue(rows.get(1).getFloat(1) > rows.get(0).getFloat(1));
  }

  @Test
  public void testSearchTableFunction() {
    String fullName = createFtsTable();

    Dataset<Row> result =
        spark.sql(
            "SELECT id, body, _score FROM SEARCH('" + fullName + "', 'lance', 10) ORDER BY id");

    List<Row> rows = result.collectAsList();
    List<Integer> ids = rows.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
    assertEquals(java.util.Arrays.asList(1, 3), ids);
    assertTrue(rows.get(0).getString(1).contains("lance"));
    assertTrue(rows.get(0).getFloat(2) > 0.0f);
  }

  @Test
  public void testVectorSearchRequiresQueryVector() {
    Assumptions.assumeTrue(supportsNamedArguments());
    String fullName = createVectorTable();

    Exception exception =
        assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        "SELECT * FROM VECTOR_SEARCH("
                            + "table => '"
                            + fullName
                            + "', "
                            + "vector_column => 'vec')")
                    .collectAsList());
    assertTrue(getDeepMessage(exception).contains("query_vector is required"));
  }

  @Test
  public void testNamedArguments() {
    Assumptions.assumeTrue(supportsNamedArguments());
    String vectorTable = createVectorTable();
    String ftsTable = createFtsTable();

    List<Row> vectorRows =
        spark
            .sql(
                "SELECT id, _distance FROM VECTOR_SEARCH("
                    + "table => '"
                    + vectorTable
                    + "', "
                    + "query_vector => array(0.0, 0.0, 0.0, 0.0), "
                    + "vector_column => 'vector', "
                    + "num_results => 2, "
                    + "distance_type => 'l2', "
                    + "bypass_vector_index => true, "
                    + "columns => array('id')) ORDER BY _distance, id")
            .collectAsList();
    assertEquals(2, vectorRows.size());
    assertEquals(0, vectorRows.get(0).getInt(0));

    List<Row> metricOnlyRows =
        spark
            .sql(
                "SELECT * FROM VECTOR_SEARCH("
                    + "table => '"
                    + vectorTable
                    + "', "
                    + "query_vector => array(0.0, 0.0, 0.0, 0.0), "
                    + "columns => array('_distance'), "
                    + "filter => 'id >= 0', "
                    + "num_results => 1)")
            .collectAsList();
    assertEquals(1, metricOnlyRows.size());
    assertEquals(1, metricOnlyRows.get(0).size());
    assertEquals(0.0f, metricOnlyRows.get(0).getFloat(0), 0.001f);

    Dataset<Row> vectorRowIdOnly =
        spark.sql(
            "SELECT * FROM VECTOR_SEARCH("
                + "table => '"
                + vectorTable
                + "', "
                + "query_vector => array(0.0, 0.0, 0.0, 0.0), "
                + "columns => array('_rowid'), "
                + "num_results => 1)");
    assertEquals(
        java.util.Arrays.asList("_rowid", "_distance"),
        java.util.Arrays.asList(vectorRowIdOnly.columns()));
    Row vectorRowIdOnlyRow = vectorRowIdOnly.collectAsList().get(0);
    assertTrue(vectorRowIdOnlyRow.getLong(0) >= 0);
    assertEquals(0.0f, vectorRowIdOnlyRow.getFloat(1), 0.001f);

    Dataset<Row> vectorWithRowId =
        spark.sql(
            "SELECT * FROM VECTOR_SEARCH("
                + "table => '"
                + vectorTable
                + "', "
                + "query_vector => array(0.0, 0.0, 0.0, 0.0), "
                + "columns => array('ID'), "
                + "with_row_id => true, "
                + "num_results => 1)");
    assertEquals(
        java.util.Arrays.asList("id", "_distance", "_rowid"),
        java.util.Arrays.asList(vectorWithRowId.columns()));
    Row vectorRowId = vectorWithRowId.collectAsList().get(0);
    assertEquals(0, vectorRowId.getInt(0));
    assertEquals(0.0f, vectorRowId.getFloat(1), 0.001f);
    assertTrue(vectorRowId.getLong(2) >= 0);

    List<Row> searchRows =
        spark
            .sql(
                "SELECT id, body, _score FROM SEARCH("
                    + "table => '"
                    + ftsTable
                    + "', "
                    + "query => 'lance', "
                    + "search_columns => array('body'), "
                    + "columns => array('id', 'body'), "
                    + "limit => 10) ORDER BY id")
            .collectAsList();
    assertEquals(2, searchRows.size());
    assertEquals(1, searchRows.get(0).getInt(0));

    List<Row> scoreOnlyRows =
        spark
            .sql(
                "SELECT * FROM SEARCH("
                    + "table => '"
                    + ftsTable
                    + "', "
                    + "query => 'lance', "
                    + "columns => array('_score'), "
                    + "filter => 'id >= 0', "
                    + "limit => 1)")
            .collectAsList();
    assertEquals(1, scoreOnlyRows.size());
    assertEquals(1, scoreOnlyRows.get(0).size());
    assertTrue(scoreOnlyRows.get(0).getFloat(0) > 0.0f);

    Dataset<Row> searchRowIdOnly =
        spark.sql(
            "SELECT * FROM SEARCH("
                + "table => '"
                + ftsTable
                + "', "
                + "query => 'lance', "
                + "columns => array('_rowid'), "
                + "limit => 1)");
    assertEquals(
        java.util.Arrays.asList("_rowid", "_score"),
        java.util.Arrays.asList(searchRowIdOnly.columns()));
    Row searchRowIdOnlyRow = searchRowIdOnly.collectAsList().get(0);
    assertTrue(searchRowIdOnlyRow.getLong(0) >= 0);
    assertTrue(searchRowIdOnlyRow.getFloat(1) > 0.0f);

    Dataset<Row> searchWithRowId =
        spark.sql(
            "SELECT * FROM SEARCH("
                + "table => '"
                + ftsTable
                + "', "
                + "query => 'lance', "
                + "columns => array('ID'), "
                + "with_row_id => true, "
                + "limit => 1)");
    assertEquals(
        java.util.Arrays.asList("id", "_score", "_rowid"),
        java.util.Arrays.asList(searchWithRowId.columns()));
    Row searchRowId = searchWithRowId.collectAsList().get(0);
    assertTrue(searchRowId.getInt(0) == 1 || searchRowId.getInt(0) == 3);
    assertTrue(searchRowId.getFloat(1) > 0.0f);
    assertTrue(searchRowId.getLong(2) >= 0);
  }

  private String createVectorTable() {
    String fullName = fullTableName("vector_search");
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.sql(
        "INSERT INTO "
            + fullName
            + " VALUES "
            + "(0, array(0.0, 0.0, 0.0, 0.0)), "
            + "(1, array(1.0, 1.0, 1.0, 1.0)), "
            + "(2, array(10.0, 10.0, 10.0, 10.0))");
    return fullName;
  }

  private String createFtsTable() {
    String fullName = fullTableName("fts_search");
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, body STRING) USING lance");
    spark.sql(
        "INSERT INTO "
            + fullName
            + " VALUES "
            + "(1, 'lance vector search'), "
            + "(2, 'spark connector table function'), "
            + "(3, 'lance full text search')");
    spark.sql(
        "ALTER TABLE "
            + fullName
            + " CREATE INDEX body_fts USING fts (body) WITH ("
            + "base_tokenizer='simple', "
            + "language='English', "
            + "max_token_length=40, "
            + "lower_case=true, "
            + "stem=false, "
            + "remove_stop_words=false, "
            + "ascii_folding=false, "
            + "with_position=true)");
    return fullName;
  }

  private String fullTableName(String prefix) {
    return CATALOG_NAME
        + ".default."
        + prefix
        + "_"
        + UUID.randomUUID().toString().replace("-", "");
  }

  private String getDeepMessage(Throwable throwable) {
    StringBuilder builder = new StringBuilder();
    Throwable current = throwable;
    while (current != null) {
      if (current.getMessage() != null) {
        builder.append(current.getMessage()).append('\n');
      }
      current = current.getCause();
    }
    return builder.toString();
  }

  private boolean supportsNamedArguments() {
    return !spark.version().startsWith("3.4.");
  }
}
