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

import org.lance.namespace.DirectoryNamespace;
import org.lance.spark.LanceSparkSQLProperties;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for distributed full-text search: {@link
 * LanceSparkSQLProperties#SEARCH_DISTRIBUTED_ENABLED} routes the full-text search SQL functions
 * through distributed Spark tasks instead of the namespace {@code queryTable} endpoint.
 *
 * <p>The namespace ({@link QueryUnavailableNamespace}) is a real {@link DirectoryNamespace} whose
 * {@code queryTable} throws; the server-side path has no working reference, so the toggle test
 * asserts the flag-off dispatch failure and the flag-on rows rather than equality.
 */
public abstract class BaseSparkDistributedFullTextSearchTest {
  private static final String CATALOG_NAME = "lance_dist_fts";
  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-distributed-full-text-search-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config(
                "spark.sql.catalog." + CATALOG_NAME + ".impl",
                QueryUnavailableNamespace.class.getName())
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .config("spark.sql.defaultCatalog", CATALOG_NAME)
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
   * Twenty rows in two fragments: ids 0-9 "hello world doc_N", 10-14 "foo bar doc_N", 15-19 "hello
   * spark doc_N" in {@code body}; {@code title} never contains "hello". Each column gets its own
   * FTS index, as multiMatch needs per-column indexes.
   */
  private String createAndIndexTable() {
    String fullName = CATALOG_NAME + ".default.dist_fts";
    spark.sql(
        String.format("CREATE TABLE %s (id INT, body STRING, title STRING) USING lance", fullName));
    String frag1 =
        IntStream.range(0, 10)
            .mapToObj(i -> String.format("(%d, 'hello world doc_%d', 'note doc_%d')", i, i, i))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s", fullName, frag1));
    String frag2a =
        IntStream.range(10, 15)
            .mapToObj(i -> String.format("(%d, 'foo bar doc_%d', 'note doc_%d')", i, i, i))
            .collect(Collectors.joining(", "));
    String frag2b =
        IntStream.range(15, 20)
            .mapToObj(i -> String.format("(%d, 'hello spark doc_%d', 'note doc_%d')", i, i, i))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s, %s", fullName, frag2a, frag2b));
    String indexOptions =
        "base_tokenizer='simple', language='English', max_token_length=40, "
            + "lower_case=true, stem=false, remove_stop_words=false, "
            + "ascii_folding=false, with_position=true";
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX fts_body USING fts (body) WITH (%s)",
            fullName, indexOptions));
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX fts_title USING fts (title) WITH (%s)",
            fullName, indexOptions));
    return fullName;
  }

  @Test
  void distributedFlagRoutesMatchPredicate() {
    String table = createAndIndexTable();
    String sql = String.format("SELECT id FROM %s WHERE lance_match(body, 'hello')", table);

    spark.conf().set(LanceSparkSQLProperties.SEARCH_DISTRIBUTED_ENABLED, "false");
    Exception failure = assertThrows(Exception.class, () -> spark.sql(sql).collectAsList());
    assertTrue(
        rootMessage(failure).contains(QueryUnavailableNamespace.ERROR_MESSAGE),
        "default dispatch must reach the namespace queryTable: " + rootMessage(failure));

    spark.conf().set(LanceSparkSQLProperties.SEARCH_DISTRIBUTED_ENABLED, "true");
    assertEquals(
        15,
        spark.sql(sql).collectAsList().size(),
        "distributed scan must return every row whose body contains 'hello'");
  }

  @Test
  void distributedMatchPhraseReturnsMatches() {
    String table = createAndIndexTable();
    spark.conf().set(LanceSparkSQLProperties.SEARCH_DISTRIBUTED_ENABLED, "true");
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world')", table))
            .collectAsList();
    assertEquals(10, rows.size(), "expected the 10 rows whose body contains the phrase");
  }

  @Test
  void distributedMultiMatchWithScalarFilterReturnsMatches() {
    String table = createAndIndexTable();
    spark.conf().set(LanceSparkSQLProperties.SEARCH_DISTRIBUTED_ENABLED, "true");
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', body, title)"
                        + " AND id >= 10",
                    table))
            .collectAsList();
    assertEquals(5, rows.size(), "ids 15-19 match 'hello' in body and id >= 10");
  }

  @Test
  void distributedSearchFunctionReturnsScoredRows() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
    String table = createAndIndexTable();
    spark.conf().set(LanceSparkSQLProperties.SEARCH_DISTRIBUTED_ENABLED, "true");
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _score FROM SEARCH(table => '%s', query => 'hello', "
                        + "search_columns => array('body'), num_results => 5)",
                    table))
            .collectAsList();
    assertEquals(5, rows.size());
    float previous = Float.MAX_VALUE;
    for (Row row : rows) {
      int id = row.getInt(0);
      assertTrue(id < 10 || id >= 15, "unexpected non-matching id " + id);
      float score = row.getFloat(1);
      assertTrue(score > 0.0f && score <= previous, "scores must be positive and non-increasing");
      previous = score;
    }
  }

  private static String rootMessage(Throwable throwable) {
    Throwable root = throwable;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return String.valueOf(root.getMessage());
  }
}
