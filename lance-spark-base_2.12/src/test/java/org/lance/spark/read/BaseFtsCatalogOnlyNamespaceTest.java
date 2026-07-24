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
   * Two fragments: ids 0-9 with body "hello world doc_N", then ids 10-14 with "foo bar doc_N" and
   * ids 15-19 with "hello spark doc_N".
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
