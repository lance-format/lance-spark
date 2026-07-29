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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that FTS predicates work against a catalog-only namespace — one that manages table
 * metadata but does not implement {@code queryTable}, as AWS Glue, Hive, and Iceberg namespaces do.
 * Such a namespace cannot run the query server-side, so the scan must fall back to the local
 * per-fragment path instead of failing with {@code Not supported: queryTable}.
 */
public abstract class BaseFtsCatalogOnlyNamespaceTest {

  protected SparkSession spark;
  protected String catalogName = "lance_catalog_only_fts";
  protected String fullTable;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws Exception {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);

    spark =
        SparkSession.builder()
            .appName("lance-fts-catalog-only-namespace-test")
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

    fullTable = catalogName + ".default.fts_" + UUID.randomUUID().toString().replace("-", "");
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
   * doc_N" and ids 15-19 with "hello spark doc_N".
   *
   * <p>Each INSERT writes one fragment per write task, and {@code local[4]} gives four tasks, so
   * the table has EIGHT fragments of 2-3 rows, not two. Slices land at 0-2/2-5/5-7/7-10 within each
   * INSERT, so ids 17-19 share a fragment and no fragment holds more than three FTS hits. Any
   * analysis of per-fragment pushdown (limit/top-N truncation in particular) must use eight, not
   * two.
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
  public void lanceMatchFallsBackToLocalScan() {
    List<Row> rows =
        spark
            .sql(String.format("SELECT id FROM %s WHERE lance_match(body, 'hello')", fullTable))
            .collectAsList();
    assertEquals(15, rows.size(), "Expected the 15 rows whose body contains 'hello'");
  }

  @Test
  public void lanceMatchPhraseFallsBackToLocalScan() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world')", fullTable))
            .collectAsList();
    assertEquals(10, rows.size(), "Expected the 10 rows whose body contains 'hello world'");
  }

  /**
   * COUNT(*) must respect an FTS predicate. The FTS rule moves the predicate out of the Filter and
   * into the relation options, so no predicate is pushed; the metadata-only COUNT(*) shortcut must
   * therefore also check for an active full-text query before answering from the manifest.
   *
   * <p>Also asserts the count was answered by the scan path rather than the metadata shortcut. The
   * value alone does not pin that down: the row-scan path applies FTS correctly too, so an
   * implementation that stopped pushing COUNT(*) altogether would still return 15. The scan's
   * readSchema is the discriminating signal — {@code LanceScan.readSchema()} returns the
   * single-column count schema only when an aggregation was pushed.
   */
  @Test
  public void countStarRespectsFtsPredicate() {
    Dataset<Row> df =
        spark.sql(
            String.format("SELECT count(*) FROM %s WHERE lance_match(body, 'hello')", fullTable));
    assertCountPushedToScan(df);
    List<Row> rows = df.collectAsList();
    assertEquals(1, rows.size());
    assertEquals(15L, rows.get(0).getLong(0), "count(*) must count only rows matching 'hello'");
  }

  /**
   * Asserts the count was computed by the scan (i.e. by {@code LanceCountStarPartitionReader}) and
   * not by re-aggregating scanned rows. The discriminating signal is the scan's output column list:
   * when the aggregate is pushed, {@code LanceScan.readSchema()} returns the single-column count
   * schema and the plan shows {@code BatchScan <table>[count#N]}; when it is not pushed, the scan
   * projects nothing and the plan shows {@code BatchScan <table>[]}. Matching on the word "count"
   * alone is not enough — the un-pushed plan also contains {@code count(1)} in its aggregate.
   */
  private void assertCountPushedToScan(Dataset<Row> df) {
    String plan = df.queryExecution().executedPlan().toString();
    assertTrue(
        plan.matches("(?s).*BatchScan\\s+\\S*\\[count#\\d+L?\\].*"),
        "COUNT(*) with an FTS predicate must be answered by the scan (BatchScan ...[count#N]), "
            + "not by re-aggregating rows. Plan: "
            + plan);
  }

  @Test
  public void countStarRespectsNonMatchingFtsPredicate() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT count(*) FROM %s WHERE lance_match(body, 'zzz_no_such_term')",
                    fullTable))
            .collectAsList();
    assertEquals(1, rows.size());
    assertEquals(
        0L, rows.get(0).getLong(0), "count(*) must be 0 when no row matches the FTS query");
  }

  /**
   * The scan-based COUNT(*) path: a pushable scalar predicate defeats the metadata shortcut, so the
   * count is computed by {@code LanceCountStarPartitionReader}, which must also apply the FTS
   * query. {@code id >= 10} spans ids 10-14 ("foo bar", no match) and 15-19 ("hello spark", match),
   * so the FTS-respecting answer (5) differs from the scalar-only answer (10).
   */
  @Test
  public void countStarRespectsFtsPredicateCombinedWithScalarFilter() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT count(*) FROM %s WHERE lance_match(body, 'hello') AND id >= 10",
                    fullTable))
            .collectAsList();
    assertEquals(1, rows.size());
    assertEquals(
        5L,
        rows.get(0).getLong(0),
        "count(*) must count only ids 15-19, which match both 'hello' and id >= 10");
  }

  /**
   * Delegates every operation the connector uses to a {@link DirectoryNamespace}, but deliberately
   * leaves {@code queryTable} unimplemented so it keeps the interface default that throws.
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
    public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
      return delegate.listNamespaces(request);
    }

    @Override
    public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
      return delegate.describeNamespace(request);
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
    public void namespaceExists(NamespaceExistsRequest request) {
      delegate.namespaceExists(request);
    }

    @Override
    public ListTablesResponse listTables(ListTablesRequest request) {
      return delegate.listTables(request);
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      return delegate.describeTable(request);
    }

    @Override
    public DeclareTableResponse declareTable(DeclareTableRequest request) {
      return delegate.declareTable(request);
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
    public void tableExists(TableExistsRequest request) {
      delegate.tableExists(request);
    }

    @Override
    public DropTableResponse dropTable(DropTableRequest request) {
      return delegate.dropTable(request);
    }

    @Override
    public RenameTableResponse renameTable(RenameTableRequest request) {
      return delegate.renameTable(request);
    }
  }
}
