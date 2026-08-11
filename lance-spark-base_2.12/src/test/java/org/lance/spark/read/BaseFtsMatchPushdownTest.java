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

import org.lance.index.DistanceType;
import org.lance.ipc.Query;
import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.QueryUtils;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract integration test for FTS predicate pushdown via {@code lance_match}.
 *
 * <p>Sets up a Lance table with an FTS index and verifies: correct rows returned, pushdown
 * confirmed via scan metadata, LIMIT behaviour across fragments, combined FTS + scalar, and all
 * planning-time rejection scenarios.
 */
public abstract class BaseFtsMatchPushdownTest {

  @org.junit.jupiter.api.BeforeEach
  void skipUntilNamespaceStructuredQuerySupported() {
    // Namespace-configured FTS routes to queryTable, which ignores structured_query until
    // lance-core dir.rs adds structured support. Skip until the lance-core bump, or migrate to a
    // path-based table to exercise the local scan path.
    org.junit.jupiter.api.Assumptions.assumeTrue(false);
  }

  protected SparkSession spark;
  protected String catalogName = "lance_fts_test";
  protected String tableName;
  protected String fullTable;
  private String testRoot;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws Exception {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    testRoot = rootPath.toString();

    spark =
        SparkSession.builder()
            .appName("lance-fts-pushdown-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            // Set Lance catalog as default so lance_match resolves without qualification
            .config("spark.sql.defaultCatalog", catalogName)
            // Disable AQE to keep plan inspection simple
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();

    tableName = "fts_test_" + UUID.randomUUID().toString().replace("-", "");
    fullTable = catalogName + ".default." + tableName;

    createAndIndexTable();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Creates a two-fragment table with an FTS index on the {@code body} column. Fragment 1: ids 0-9,
   * body "hello world doc_N" Fragment 2: ids 10-19, body "foo bar doc_N" (ids 10-14) and "hello
   * spark doc_N" (ids 15-19)
   */
  private void createAndIndexTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, body STRING, price DOUBLE) USING lance", fullTable));

    // First insert — fragment 1
    String frag1Values =
        IntStream.range(0, 10)
            .mapToObj(i -> String.format("(%d, 'hello world doc_%d', %s)", i, i, (i * 10.0)))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s", fullTable, frag1Values));

    // Second insert — fragment 2
    String frag2aValues =
        IntStream.range(10, 15)
            .mapToObj(i -> String.format("(%d, 'foo bar doc_%d', %s)", i, i, (i * 10.0)))
            .collect(Collectors.joining(", "));
    String frag2bValues =
        IntStream.range(15, 20)
            .mapToObj(i -> String.format("(%d, 'hello spark doc_%d', %s)", i, i, (i * 10.0)))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s, %s", fullTable, frag2aValues, frag2bValues));

    // Build FTS index on body
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX fts_body USING fts (body) WITH ("
                + "base_tokenizer='simple', "
                + "language='English', "
                + "max_token_length=40, "
                + "lower_case=true, "
                + "stem=false, "
                + "remove_stop_words=false, "
                + "ascii_folding=false, "
                + "with_position=true"
                + ")",
            fullTable));
  }

  // ── Scenario: correct rows returned ─────────────────────────────────────

  @Test
  public void testLanceMatchReturnsCorrectRows() {
    // "hello" appears in fragment 1 (ids 0-9) and fragment 2 (ids 15-19)
    List<Row> rows =
        spark
            .sql(String.format("SELECT id FROM %s WHERE lance_match(body, 'hello')", fullTable))
            .collectAsList();
    assertEquals(15, rows.size(), "Expected 15 rows matching 'hello'");
  }

  @Test
  public void testLanceMatchMultiTokenReturnsCorrectRows() {
    // "hello world" with simple tokenizer uses OR semantics: matches docs with "hello" OR "world".
    // Fragment 1 (ids 0-9): "hello world doc_N" — 10 rows
    // Fragment 2 (ids 15-19): "hello spark doc_N" — 5 rows (contain "hello")
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hello world')", fullTable))
            .collectAsList();
    assertEquals(15, rows.size(), "Expected 15 rows matching 'hello world' (OR semantics)");
  }

  // ── Scenario: pushdown confirmed via scan metadata ───────────────────────

  @Test
  public void testLanceMatchPushdownVisibleInMetadata() {
    Dataset<Row> ds =
        spark.sql(String.format("SELECT id FROM %s WHERE lance_match(body, 'hello')", fullTable));
    ds.collect(); // trigger plan execution

    Map<String, String> meta = extractLanceScanMetadata(ds);
    assertNotNull(meta, "Could not find LanceScan in executed plan");
    assertTrue(
        meta.containsKey("fullTextQuery"),
        "Expected 'fullTextQuery' key in scan metadata after pushdown");
    assertNotNull(meta.get("fullTextQuery"), "fullTextQuery metadata value must not be null");
  }

  // ── Scenario: pruneByLimit is suppressed for FTS ────────────────────────

  @Test
  public void testLanceMatchReturnsAllMatchingRowsAcrossFragments() {
    // "hello" matches 15 rows across both fragments. Without a LIMIT, pruneByLimit is not
    // engaged, but this test verifies that the FTS guard does not itself suppress fragment
    // scheduling. Both fragment ranges (ids 0-9 and ids 15-19) must be present in the result.
    List<Row> rows =
        spark
            .sql(String.format("SELECT id FROM %s WHERE lance_match(body, 'hello')", fullTable))
            .collectAsList();

    assertEquals(15, rows.size(), "Expected 15 rows — both fragment ranges must be scheduled");
    List<Integer> ids = rows.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertTrue(ids.stream().anyMatch(id -> id < 10), "Expected rows from fragment 1 (ids 0-9)");
    assertTrue(ids.stream().anyMatch(id -> id >= 15), "Expected rows from fragment 2 (ids 15-19)");
  }

  @Test
  public void testLanceMatchLimitDoesNotPruneFragments() {
    // "spark" appears only in fragment 2 (ids 15-19). Fragment 1 has 10 rows but none match
    // "spark". Without the pruneByLimit FTS guard, Spark accumulates fragment-1's 10 row-count
    // toward LIMIT 3 (10 >= 3), stops scheduling, and fragment 2 is never queried — returning
    // 0 rows. With the guard active, both fragments are scheduled, and 3 rows are returned from
    // fragment 2 (ids 15-19).
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'spark') LIMIT 3", fullTable))
            .collectAsList();
    assertEquals(3, rows.size(), "Expected 3 rows — fragment 2 must be scheduled despite LIMIT");
    List<Integer> ids = rows.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertTrue(
        ids.stream().allMatch(id -> id >= 15 && id <= 19),
        "All returned ids must be from fragment 2 (ids 15-19), got: " + ids);
  }

  // ── Scenario: combined FTS + scalar predicate ────────────────────────────

  @Test
  public void testLanceMatchCombinedWithScalarFilter() {
    // "hello" rows: ids 0-9 (price 0-90) and ids 15-19 (price 150-190).
    // price > 100 keeps ids 11+ only — intersecting with "hello" gives ids 15-19 (5 rows).
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hello') AND price > 100.0",
                    fullTable))
            .collectAsList();
    assertEquals(5, rows.size(), "Expected 5 rows matching 'hello' AND price > 100");
    List<Integer> ids = rows.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertEquals(List.of(15, 16, 17, 18, 19), ids);
  }

  // ── Scenario: planning-time rejection: duplicate FTS predicates ──────────

  @Test
  public void testDuplicateFtsPredicateRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_match(body, 'hello') AND lance_match(body, 'world')",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for duplicate lance_match in WHERE clause");
  }

  // ── Scenario: planning-time rejection: FTS as OR operand with scalar ─────

  @Test
  public void testFtsOrScalarRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_match(body, 'hello') OR price > 100.0",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for lance_match OR scalar");
  }

  // ── Scenario: planning-time rejection: FTS nested inside OR sub-expression

  @Test
  public void testNestedOrWithFtsRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE (lance_match(body, 'hello') OR price > 5.0) AND id > 3",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for lance_match nested inside OR sub-expression");
  }

  // ── Scenario: planning-time rejection: runtime-valued query argument ─────

  @Test
  public void testRuntimeQueryArgumentRaisesAnalysisException() {
    // The query text must be a string literal — a column reference cannot be pushed to the index
    // at planning time.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(String.format("SELECT id FROM %s t WHERE lance_match(body, body)", fullTable))
                .collect(),
        "Expected IllegalArgumentException when lance_match query argument is a column reference");
  }

  // ── Scenario: planning-time rejection: OR(FTS, FTS) ─────────────────────

  @Test
  public void testOrOfTwoFtsPredicatesRaisesAnalysisExceptionNamingMultiMatch() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_match(body, 'hello') OR lance_match(body, 'world')",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for OR of two lance_match predicates");
    assertTrue(
        ex.getMessage().contains("lance_multi_match"),
        "IllegalArgumentException message must name lance_multi_match as the alternative, got: "
            + ex.getMessage());
  }

  // ── Scenario: planning-time rejection: NOT(FTS) ──────────────────────────

  @Test
  public void testNotWrappedFtsRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE NOT lance_match(body, 'hello')", fullTable))
                .collect(),
        "Expected IllegalArgumentException for NOT(lance_match(...))");
  }

  // ── Scenario: planning-time rejection: FTS + nearest ────────────────────

  @Test
  public void testFtsWithNearestRaisesAnalysisException() {
    // Retrieve the actual dataset location from the catalog — the dir namespace stores tables
    // at hash-prefixed paths, not at root/default/tableName.
    org.apache.spark.sql.connector.catalog.CatalogPlugin catalogPlugin =
        spark.sessionState().catalogManager().catalog(catalogName);
    org.apache.spark.sql.connector.catalog.TableCatalog tableCatalog =
        (org.apache.spark.sql.connector.catalog.TableCatalog) catalogPlugin;
    String datasetUri;
    try {
      org.apache.spark.sql.connector.catalog.Table table =
          tableCatalog.loadTable(
              org.apache.spark.sql.connector.catalog.Identifier.of(
                  new String[] {"default"}, tableName));
      datasetUri = table.properties().get("location");
    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve table location from catalog", e);
    }

    Query.Builder qb = new Query.Builder();
    qb.setK(5);
    qb.setColumn("body");
    qb.setKey(new float[] {1.0f, 2.0f});
    qb.setUseIndex(false);
    qb.setDistanceType(DistanceType.L2);

    String nearestJson = QueryUtils.queryToString(qb.build());

    // The nearest read option is rejected at schema-inference time (parseTypedFlags fail-fast)
    // when Spark calls inferSchema/getTable during .load().
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .read()
                    .format(LanceDataSource.name)
                    .option(LanceSparkReadOptions.CONFIG_NEAREST, nearestJson)
                    .option(LanceSparkReadOptions.CONFIG_DATASET_URI, datasetUri)
                    .load(),
            "Expected IllegalArgumentException when nearest option is present");
    assertTrue(
        exception.getMessage().contains("nearest"), "Error message should mention 'nearest'");
    assertTrue(
        exception.getMessage().contains("VECTOR_SEARCH"),
        "Error message should mention VECTOR_SEARCH as the replacement");
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  protected Map<String, String> extractLanceScanMetadata(Dataset<Row> ds) {
    return LanceScanTestHelper.extractLanceScanMetadata(ds);
  }
}
