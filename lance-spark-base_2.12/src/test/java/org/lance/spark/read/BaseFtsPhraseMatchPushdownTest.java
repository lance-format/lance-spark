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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract integration test for FTS phrase predicate pushdown via {@code lance_match_phrase}.
 *
 * <p>Sets up a Lance table with an FTS index built with {@code with_position=true} and verifies:
 * correct rows returned for exact phrase (slop=0), slop=1 producing a strict superset of slop=0,
 * pushdown confirmed via scan metadata, negative slop rejection, and all planning-time rejection
 * scenarios (OR operand, duplicate phrase, mixed match+phrase).
 *
 * <p>Dataset design: the {@code body} column contains rows with the exact phrase "hello world"
 * (contiguous) and rows with "hello dear world" (one intervening token). This ensures the slop=0
 * and slop=1 result sets are strictly different so the superset assertion is not vacuously
 * satisfied.
 */
public abstract class BaseFtsPhraseMatchPushdownTest {

  @org.junit.jupiter.api.BeforeEach
  void skipUntilNamespaceStructuredQuerySupported() {
    // Namespace-configured FTS routes to queryTable, which ignores structured_query until
    // lance-core dir.rs adds structured support. Skip until the lance-core bump, or migrate to a
    // path-based table to exercise the local scan path.
    org.junit.jupiter.api.Assumptions.assumeTrue(false);
  }

  protected SparkSession spark;
  protected String catalogName = "lance_match_phrase_test";
  protected String tableName;
  protected String fullTable;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws Exception {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();

    spark =
        SparkSession.builder()
            .appName("lance-phrase-pushdown-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .config("spark.sql.defaultCatalog", catalogName)
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();

    tableName = "phrase_test_" + UUID.randomUUID().toString().replace("-", "");
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
   * Creates a table where:
   *
   * <ul>
   *   <li>ids 0-9: "hello world doc_N" — contiguous phrase, matches slop=0 and slop=1
   *   <li>ids 10-14: "hello dear world doc_N" — one intervening token, matches slop=1 only
   *   <li>ids 15-19: "foo bar baz doc_N" — no phrase match at any slop
   * </ul>
   *
   * FTS index is built with {@code with_position=true}.
   */
  private void createAndIndexTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, body STRING, price DOUBLE) USING lance", fullTable));

    // Fragment 1: exact phrase rows
    String frag1Values =
        java.util.stream.IntStream.range(0, 10)
            .mapToObj(i -> String.format("(%d, 'hello world doc_%d', %s)", i, i, (i * 10.0)))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s", fullTable, frag1Values));

    // Fragment 2: near-phrase rows (slop=1 only) + non-matching rows
    String frag2aValues =
        java.util.stream.IntStream.range(10, 15)
            .mapToObj(i -> String.format("(%d, 'hello dear world doc_%d', %s)", i, i, (i * 10.0)))
            .collect(Collectors.joining(", "));
    String frag2bValues =
        java.util.stream.IntStream.range(15, 20)
            .mapToObj(i -> String.format("(%d, 'foo bar baz doc_%d', %s)", i, i, (i * 10.0)))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s, %s", fullTable, frag2aValues, frag2bValues));

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

  // ── Scenario: exact phrase (slop=0 default) ──────────────────────────────

  @Test
  public void testLanceMatchPhraseReturnsCorrectRowsExactPhrase() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world')", fullTable))
            .collectAsList();
    // Only ids 0-9 have the exact contiguous phrase "hello world"
    assertEquals(10, rows.size(), "Expected 10 rows for exact phrase 'hello world' (slop=0)");
    List<Integer> ids = rows.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertEquals(java.util.stream.IntStream.range(0, 10).boxed().collect(Collectors.toList()), ids);
  }

  // ── Scenario: slop=1 strict superset of slop=0 ───────────────────────────

  @Test
  public void testLanceMatchPhraseSlop1ProducesStrictSupersetOfSlop0() {
    List<Row> slop0 =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world', 0)",
                    fullTable))
            .collectAsList();
    List<Row> slop1 =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world', 1)",
                    fullTable))
            .collectAsList();

    assertTrue(
        slop1.size() > slop0.size(),
        "slop=1 result set must be strictly larger than slop=0: slop0="
            + slop0.size()
            + " slop1="
            + slop1.size());

    List<Integer> ids0 = slop0.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    List<Integer> ids1 = slop1.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertTrue(
        ids1.containsAll(ids0),
        "slop=1 result must contain all slop=0 results (superset). slop0="
            + ids0
            + " slop1="
            + ids1);
  }

  // ── Scenario: pushdown confirmed via scan metadata ───────────────────────

  @Test
  public void testLanceMatchPhrasePushdownVisibleInMetadata() {
    Dataset<Row> ds =
        spark.sql(
            String.format(
                "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world')", fullTable));
    ds.collect();

    Map<String, String> meta = extractLanceScanMetadata(ds);
    assertNotNull(meta, "Could not find LanceScan in executed plan");
    assertTrue(
        meta.containsKey("fullTextQuery"),
        "Expected 'fullTextQuery' key in scan metadata after phrase pushdown");
    assertNotNull(meta.get("fullTextQuery"), "fullTextQuery metadata value must not be null");
  }

  // ── Scenario: negative slop raises IllegalArgumentException ─────────────────────

  @Test
  public void testNegativeSlopRaisesAnalysisException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello', -1)",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for negative slop");
    assertTrue(
        ex.getMessage().contains("slop"),
        "IllegalArgumentException message must mention 'slop', got: " + ex.getMessage());
  }

  // ── Scenario: planning-time rejection: lance_match_phrase as OR operand ─────────

  @Test
  public void testLanceMatchPhraseOrScalarRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world') OR price > 100.0",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for lance_match_phrase OR scalar");
  }

  // ── Scenario: planning-time rejection: duplicate lance_match_phrase ─────────────

  @Test
  public void testDuplicatePhrasePredRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world') AND lance_match_phrase(body, 'foo bar')",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for duplicate lance_match_phrase in WHERE clause");
  }

  // ── Scenario: planning-time rejection: mixed lance_match + lance_match_phrase ───

  @Test
  public void testMixedMatchAndPhrasePredicateRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_match(body, 'hello') AND lance_match_phrase(body, 'hello world')",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for mixed lance_match + lance_match_phrase in WHERE clause");
  }

  // ── Scenario: combined phrase + scalar predicate — scalar is not dropped ──

  @Test
  public void testLanceMatchPhraseWithScalarResidualsRetained() {
    // Exact phrase "hello world" matches ids 0-9 (prices 0.0-90.0).
    // AND price > 50.0 keeps ids 6-9 (prices 60.0, 70.0, 80.0, 90.0) — 4 rows.
    // If the scalar residual were silently dropped, all 10 phrase-matching rows
    // would be returned instead of 4.
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world') AND price > 50.0",
                    fullTable))
            .collectAsList();
    assertEquals(4, rows.size(), "Expected 4 rows matching phrase AND price > 50.0");
    List<Integer> ids = rows.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    assertEquals(java.util.Arrays.asList(6, 7, 8, 9), ids);
  }

  // ── Scenario: FTS phrase nested inside OR sub-expression (deep nesting) ──

  @Test
  public void testNestedOrWithPhraseRaisesAnalysisException() {
    // WHERE (lance_match_phrase(...) OR price > 5) AND id > 3 — the FTS predicate is
    // inside an OR that is itself an AND operand. OR detection must traverse the
    // condition tree at any nesting depth, not only inspect top-level operands.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE (lance_match_phrase(body, 'hello world') OR price > 5.0) AND id > 3",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for lance_match_phrase nested inside OR sub-expression");
  }

  // ── Scenario: OR of two lance_match_phrase predicates — names lance_multi_match ─

  @Test
  public void testOrOfTwoPhrasePredRaisesAnalysisExceptionNamingMultiMatch() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_match_phrase(body, 'hello world') OR lance_match_phrase(body, 'foo bar')",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for OR of two lance_match_phrase predicates");
    assertTrue(
        ex.getMessage().contains("lance_multi_match"),
        "IllegalArgumentException message must name lance_multi_match as the alternative, got: "
            + ex.getMessage());
  }

  // ── Scenario: runtime-valued query argument rejected at planning time ──────

  @Test
  public void testRuntimeQueryArgumentRaisesAnalysisException() {
    // The query text must be a string literal — a column reference cannot be pushed
    // to the FTS index at planning time and has no representation there.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s t WHERE lance_match_phrase(body, body)", fullTable))
                .collect(),
        "Expected IllegalArgumentException when lance_match_phrase query argument is a column reference");
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  protected Map<String, String> extractLanceScanMetadata(Dataset<Row> ds) {
    return LanceScanTestHelper.extractLanceScanMetadata(ds);
  }
}
