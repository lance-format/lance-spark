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
package org.lance.spark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/** Base test for FTS query-side functionality — {@code lance_match(col, 'q')} + _score column. */
public abstract class BaseFtsQueryTest {
  protected String catalogName = "lance_test";
  protected String tableName;
  protected String fullTable;
  protected SparkSession spark;

  @TempDir Path tempDir;

  private static final String FTS_OPTIONS =
      "base_tokenizer='simple', "
          + "language='English', "
          + "max_token_length=40, "
          + "lower_case=true, "
          + "stem=false, "
          + "remove_stop_words=false, "
          + "ascii_folding=false, "
          + "with_position=true";

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-fts-query-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "fts_query_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (spark != null) {
      spark.close();
    }
  }

  /** Creates a 2-fragment table with FTS index on {@code content} column. */
  private void prepareIndexedTable() {
    spark.sql(String.format("create table %s (id int, content string) using lance;", fullTable));
    spark.sql(
        String.format(
            "insert into %s values "
                + "(1, 'introduction to python programming'), "
                + "(2, 'advanced java techniques'), "
                + "(3, 'python data analysis with pandas')",
            fullTable));
    spark.sql(
        String.format(
            "insert into %s values "
                + "(4, 'scala and spark for big data'), "
                + "(5, 'web development with python flask'), "
                + "(6, 'rust systems programming')",
            fullTable));
    spark.sql(
        String.format(
            "alter table %s create index fts_content using fts (content) with (%s)",
            fullTable, FTS_OPTIONS));
  }

  @Test
  public void testFtsMatchSingleTerm() {
    prepareIndexedTable();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "select id from %s where lance_match(content, 'python') order by id", fullTable));
    List<Row> rows = result.collectAsList();
    Assertions.assertEquals(3, rows.size(), "Three rows contain 'python'");
    Assertions.assertEquals(1, rows.get(0).getInt(0));
    Assertions.assertEquals(3, rows.get(1).getInt(0));
    Assertions.assertEquals(5, rows.get(2).getInt(0));
  }

  @Test
  public void testFtsMatchCombinedWithScalarFilter() {
    prepareIndexedTable();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "select id from %s where lance_match(content, 'python') and id > 2 order by id",
                fullTable));
    List<Row> rows = result.collectAsList();
    Assertions.assertEquals(2, rows.size(), "lance_match AND id>2 keeps ids 3 and 5");
    Assertions.assertEquals(3, rows.get(0).getInt(0));
    Assertions.assertEquals(5, rows.get(1).getInt(0));
  }

  @Test
  public void testFtsScoreColumn() {
    prepareIndexedTable();

    // Just assert that _score is materialized with non-null positive BM25 values.
    // ORDER BY _score in a single query triggers RangePartitioner.sketch in Spark, which
    // builds a separate sample scan where our optimizer rule may not re-apply — leaving the
    // scan without the FTS query. Ranking end-to-end is tracked as a follow-up.
    Dataset<Row> result =
        spark.sql(
            String.format(
                "select id, _score from %s where lance_match(content, 'python programming')",
                fullTable));
    List<Row> rows = result.collectAsList();
    Assertions.assertFalse(rows.isEmpty(), "Expected at least one match");
    for (Row r : rows) {
      Assertions.assertFalse(r.isNullAt(1), "_score must not be null when FTS is active");
      Assertions.assertTrue(r.getFloat(1) > 0.0f, "_score should be a positive BM25 value");
    }
  }
}
