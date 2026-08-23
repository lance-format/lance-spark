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

import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.RegisterTableResponse;
import org.lance.namespace.model.RenameTableRequest;
import org.lance.namespace.model.RenameTableResponse;
import org.lance.namespace.model.TableExistsRequest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the {@code _score} metadata column behavior on the FTS predicate path:
 *
 * <ul>
 *   <li>Returns BM25 relevance scores when a full-text search predicate is active.
 *   <li>Hidden from {@code SELECT *} (MetadataColumn contract).
 *   <li>Rejected at build time when projected without an FTS predicate.
 * </ul>
 *
 * <p>Uses a catalog-only namespace (local per-fragment scan path) so the tests exercise the
 * explicit {@code _score} projection in {@code LanceFragmentScanner.create()} rather than the
 * namespace {@code queryTable} delivery.
 */
public abstract class BaseFtsScoreColumnTest {

  protected SparkSession spark;
  protected String catalogName = "lance_fts_score_test";
  protected String fullTable;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws Exception {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);

    spark =
        SparkSession.builder()
            .appName("lance-fts-score-column-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config(
                "spark.sql.catalog." + catalogName + ".impl", CatalogOnlyNamespace.class.getName())
            .config("spark.sql.catalog." + catalogName + ".root", rootPath.toString())
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .config("spark.sql.defaultCatalog", catalogName)
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();

    fullTable = catalogName + ".default.fts_score_" + UUID.randomUUID().toString().replace("-", "");
    createAndIndexTable();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Twenty rows in two INSERTs: ids 0-9 with body "hello world doc_N", then ids 10-14 with "foo bar
   * doc_N" and ids 15-19 with "hello spark doc_N". Fifteen rows contain "hello".
   */
  private void createAndIndexTable() {
    spark.sql(String.format("CREATE TABLE %s (id INT, body STRING) USING lance", fullTable));
    String frag1 =
        IntStream.range(0, 10)
            .mapToObj(i -> String.format("(%d, 'hello world doc_%d')", i, i))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s", fullTable, frag1));
    String frag2a =
        IntStream.range(10, 15)
            .mapToObj(i -> String.format("(%d, 'foo bar doc_%d')", i, i))
            .collect(Collectors.joining(", "));
    String frag2b =
        IntStream.range(15, 20)
            .mapToObj(i -> String.format("(%d, 'hello spark doc_%d')", i, i))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s, %s", fullTable, frag2a, frag2b));
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX fts_body USING fts (body) WITH ("
                + "base_tokenizer='simple', language='English', max_token_length=40, "
                + "lower_case=true, stem=false, remove_stop_words=false, "
                + "ascii_folding=false, with_position=true)",
            fullTable));
  }

  @Test
  public void testScoreReturnedWithLanceMatch() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _score FROM %s WHERE lance_match(body, 'hello')", fullTable))
            .collectAsList();
    assertEquals(15, rows.size(), "Expected 15 rows matching 'hello'");
    for (Row row : rows) {
      assertFalse(row.isNullAt(1), "Every matching row must have a non-null _score");
      assertTrue(row.getFloat(1) > 0.0f, "_score must be positive for matching rows");
    }
  }

  @Test
  public void testScoreReturnedWithLanceMatchPhrase() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _score FROM %s WHERE lance_match_phrase(body, 'hello world')",
                    fullTable))
            .collectAsList();
    assertEquals(10, rows.size(), "Expected 10 rows matching phrase 'hello world'");
    for (Row row : rows) {
      assertFalse(row.isNullAt(1), "Every matching row must have a non-null _score");
      assertTrue(row.getFloat(1) > 0.0f, "_score must be positive for matching rows");
    }
  }

  @Test
  public void testScoreWithLimitReturnsSubset() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _score FROM %s WHERE lance_match(body, 'hello') LIMIT 5",
                    fullTable))
            .collectAsList();
    assertEquals(5, rows.size(), "LIMIT 5 must return exactly 5 rows");
    for (Row row : rows) {
      assertFalse(row.isNullAt(1), "Every matching row must have a non-null _score");
      assertTrue(row.getFloat(1) > 0.0f, "_score must be positive for matching rows");
    }
  }

  @Test
  public void testScoreExcludedFromSelectStar() {
    Dataset<Row> df =
        spark.sql(String.format("SELECT * FROM %s WHERE lance_match(body, 'hello')", fullTable));
    List<String> columns = Arrays.asList(df.columns());
    assertFalse(
        columns.contains("_score"), "SELECT * must not include _score (MetadataColumn contract)");
    assertEquals(2, columns.size(), "Only data columns (id, body) expected in SELECT *");
  }

  @Test
  public void testScoreWithoutFtsRejected() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> spark.sql(String.format("SELECT _score FROM %s", fullTable)).collectAsList());
    assertTrue(
        ex.getMessage().contains("full-text search predicate"),
        "Error message should mention full-text search predicate requirement");
  }

  @Test
  public void testScoreWithScalarFilterOnlyRejected() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(String.format("SELECT _score FROM %s WHERE id > 5", fullTable))
                    .collectAsList());
    assertTrue(
        ex.getMessage().contains("full-text search predicate"),
        "Scalar filter alone must not satisfy the _score FTS requirement");
  }

  @Test
  public void testScoreWithCombinedFilter() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _score FROM %s WHERE lance_match(body, 'hello') AND id >= 15",
                    fullTable))
            .collectAsList();
    assertEquals(5, rows.size(), "Expected 5 rows (ids 15-19) matching 'hello' AND id >= 15");
    for (Row row : rows) {
      assertFalse(row.isNullAt(1), "Every matching row must have a non-null _score");
      assertTrue(row.getFloat(1) > 0.0f, "_score must be positive for matching rows");
    }
  }

  @Test
  public void testScoreOrderDescending() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _score FROM %s WHERE lance_match(body, 'hello') "
                        + "ORDER BY _score DESC",
                    fullTable))
            .collectAsList();
    assertEquals(15, rows.size());
    for (int i = 1; i < rows.size(); i++) {
      assertTrue(
          rows.get(i - 1).getFloat(1) >= rows.get(i).getFloat(1),
          "Rows must be sorted by _score descending");
    }
  }

  @Test
  public void testScoreOrderDescendingWithLimit() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _score FROM %s WHERE lance_match(body, 'hello') "
                        + "ORDER BY _score DESC LIMIT 5",
                    fullTable))
            .collectAsList();
    assertEquals(5, rows.size(), "ORDER BY _score DESC LIMIT 5 must return exactly 5 rows");
    for (int i = 1; i < rows.size(); i++) {
      assertTrue(
          rows.get(i - 1).getFloat(1) >= rows.get(i).getFloat(1),
          "Rows must be sorted by _score descending");
    }
  }

  @Test
  public void testScoreIsNullableInSchema() {
    Dataset<Row> df =
        spark.sql(
            String.format("SELECT _score FROM %s WHERE lance_match(body, 'hello')", fullTable));
    assertTrue(
        df.schema().apply("_score").nullable(), "_score metadata column must be declared nullable");
  }

  /**
   * Catalog-only namespace: delegates to {@link DirectoryNamespace} but leaves {@code queryTable}
   * unimplemented, forcing the local per-fragment scan path.
   */
  public static class CatalogOnlyNamespace implements LanceNamespace {
    private final DirectoryNamespace delegate = new DirectoryNamespace();

    public CatalogOnlyNamespace() {}

    @Override
    public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
      delegate.initialize(configProperties, allocator);
    }

    @Override
    public String namespaceId() {
      return delegate.namespaceId();
    }

    @Override
    public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
      return delegate.createNamespace(request);
    }

    @Override
    public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
      return delegate.dropNamespace(request);
    }

    @Override
    public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
      return delegate.listNamespaces(request);
    }

    @Override
    public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
      return delegate.describeNamespace(request);
    }

    @Override
    public void namespaceExists(NamespaceExistsRequest request) {
      delegate.namespaceExists(request);
    }

    @Override
    public DeclareTableResponse declareTable(DeclareTableRequest request) {
      return delegate.declareTable(request);
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      return delegate.describeTable(request);
    }

    @Override
    public DropTableResponse dropTable(DropTableRequest request) {
      return delegate.dropTable(request);
    }

    @Override
    public ListTablesResponse listTables(ListTablesRequest request) {
      return delegate.listTables(request);
    }

    @Override
    public void tableExists(TableExistsRequest request) {
      delegate.tableExists(request);
    }

    @Override
    public RegisterTableResponse registerTable(RegisterTableRequest request) {
      return delegate.registerTable(request);
    }

    @Override
    public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
      return delegate.deregisterTable(request);
    }

    @Override
    public RenameTableResponse renameTable(RenameTableRequest request) {
      return delegate.renameTable(request);
    }
  }
}
