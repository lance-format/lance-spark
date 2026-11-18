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
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract integration test for advanced FTS options: extended {@code lance_match} options and
 * {@code lance_multi_match}.
 *
 * <p>Fixture: one table with two FTS-indexed columns ({@code body} and {@code title}). Each column
 * gets a separate FTS index (required — multiMatch needs per-column indexes at the Rust layer).
 *
 * <p>Row layout:
 *
 * <ul>
 *   <li>ids 0-9: body="hello world doc_N", title="greeting doc_N" — body-only matches
 *   <li>ids 10-14: body="foo bar doc_N", title="hello welcome doc_N" — title-only matches for
 *       "hello"
 *   <li>ids 15-19: body="jell test doc_N", title="other doc_N" — near-miss rows for fuzziness test
 *       (body contains "jell", one edit from "hell" which is 4 chars — auto-fuzziness allows edit
 *       distance 1 for tokens of length 3-5)
 * </ul>
 *
 * <p>The fuzziness test queries "hell" (a 4-char token NOT present in the fixture): fuzziness=1
 * (edit distance 1) matches "hello" (ids 0-9) and "jell" (ids 15-19), both within edit distance 1.
 * With fuzziness=0 (exact match), "hell" has no exact match in the index so the result is empty —
 * strictly a subset of fuzziness=1.
 */
public abstract class BaseFtsAdvancedOptionsTest {

  @org.junit.jupiter.api.BeforeEach
  void skipUntilNamespaceStructuredQuerySupported() {
    // Namespace-configured FTS routes to queryTable, which ignores structured_query until
    // lance-core dir.rs adds structured support. Skip until the lance-core bump, or migrate to a
    // path-based table to exercise the local scan path.
    org.junit.jupiter.api.Assumptions.assumeTrue(false);
  }

  protected SparkSession spark;
  protected String catalogName = "lance_adv_test";
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
            .appName("lance-adv-fts-test")
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

    tableName = "adv_fts_" + UUID.randomUUID().toString().replace("-", "");
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
   * Creates the shared fixture table with two FTS-indexed columns.
   *
   * <ul>
   *   <li>ids 0-9: body="hello world doc_N", title="greeting doc_N"
   *   <li>ids 10-14: body="foo bar doc_N", title="hello welcome doc_N"
   *   <li>ids 15-19: body="jell test doc_N", title="other doc_N"
   * </ul>
   */
  private void createAndIndexTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, body STRING, title STRING) USING lance", fullTable));

    String frag1Values =
        IntStream.range(0, 10)
            .mapToObj(i -> String.format("(%d, 'hello world doc_%d', 'greeting doc_%d')", i, i, i))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s", fullTable, frag1Values));

    String frag2aValues =
        IntStream.range(10, 15)
            .mapToObj(i -> String.format("(%d, 'foo bar doc_%d', 'hello welcome doc_%d')", i, i, i))
            .collect(Collectors.joining(", "));
    // Near-miss rows: body contains "jell" (1 edit from "hell"), title contains "other"
    String frag2bValues =
        IntStream.range(15, 20)
            .mapToObj(i -> String.format("(%d, 'jell test doc_%d', 'other doc_%d')", i, i, i))
            .collect(Collectors.joining(", "));
    spark.sql(String.format("INSERT INTO %s VALUES %s, %s", fullTable, frag2aValues, frag2bValues));

    // Separate FTS index per column (required for multiMatch Rust layer)
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

    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX fts_title USING fts (title) WITH ("
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

  // ── Extended lance_match options: execute without error ──────────────────

  @Test
  public void testLanceMatchWithOperatorAndExecutesWithoutError() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hello world', 'operator=AND')",
                    fullTable))
            .collectAsList();
    // operator=AND: row must contain both "hello" AND "world"; ids 0-9 qualify
    assertEquals(10, rows.size(), "Expected 10 rows for operator=AND on 'hello world'");
  }

  @Test
  public void testLanceMatchWithBoostExecutesWithoutError() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hello', 'boost=2.0')", fullTable))
            .collectAsList();
    // boost affects scoring only, not row selection — same rows as plain lance_match
    assertEquals(10, rows.size(), "Expected 10 rows for lance_match with boost=2.0");
  }

  @Test
  public void testLanceMatchWithPrefixLengthExecutesWithoutError() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hello', 'prefix_length=2')",
                    fullTable))
            .collectAsList();
    assertEquals(10, rows.size(), "Expected 10 rows for lance_match with prefix_length=2");
  }

  @Test
  public void testLanceMatchWithMaxExpansionsExecutesWithoutError() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hello', 'max_expansions=20')",
                    fullTable))
            .collectAsList();
    assertEquals(10, rows.size(), "Expected 10 rows for lance_match with max_expansions=20");
  }

  @Test
  public void testLanceMatchWithFuzzinessOneExecutesWithoutError() {
    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hello', 'fuzziness=1')", fullTable))
            .collectAsList();
    // fuzziness=1 allows edit distance 1; "hello" is an exact token so all ids 0-9 qualify at min
    assertTrue(rows.size() >= 10, "Expected at least 10 rows for lance_match with fuzziness=1");
  }

  // ── fuzziness=0: exact match, subset of auto-fuzziness ───────────────────

  @Test
  public void testFuzzinessZeroIsSubsetOfFuzzinessOne() {
    // "hell" is 4 chars and is NOT a token in the fixture. With fuzziness=1, "hello"
    // (ids 0-9, edit distance 1: hell→hello by insertion) and "jell" (ids 15-19, edit
    // distance 1: hell→jell by substitution) both match. With fuzziness=0 (exact match
    // only), neither "hello" nor "jell" is an exact match for "hell", so the result is
    // empty — strictly a subset of fuzziness=1.
    List<Row> fuzzOne =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hell', 'fuzziness=1')", fullTable))
            .collectAsList();
    List<Row> fuzzZero =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_match(body, 'hell', 'fuzziness=0')", fullTable))
            .collectAsList();

    List<Integer> idsFuzzOne =
        fuzzOne.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    List<Integer> idsFuzzZero =
        fuzzZero.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());

    // fuzziness=0 result must be a subset of fuzziness=1 result
    assertTrue(
        idsFuzzOne.containsAll(idsFuzzZero),
        "fuzziness=0 result must be a subset of fuzziness=1 result. "
            + "fuzziness=1="
            + idsFuzzOne
            + " fuzziness=0="
            + idsFuzzZero);

    // fuzziness=1 on "hell" must return more rows than fuzziness=0 (exact only). "hell" is
    // not a fixture token, so fuzziness=0 returns empty; fuzziness=1 returns at least ids
    // 0-9 (body has "hello", edit distance 1) and ids 15-19 (body has "jell", edit distance 1).
    assertTrue(
        idsFuzzOne.size() > idsFuzzZero.size(),
        "fuzziness=1 result must be strictly larger than fuzziness=0 result. "
            + "fuzziness=1="
            + idsFuzzOne
            + " fuzziness=0="
            + idsFuzzZero);
  }

  // ── lance_multi_match OR semantics ───────────────────────────────────────

  @Test
  public void testLanceMultiMatchOrSemanticsReturnsSupersetOfSingleColumnMatch() {
    // lance_match(body, 'hello') matches ids 0-9 (body column only)
    // lance_multi_match('hello', body, title) uses OR: a row matches if body OR title contains
    // "hello". ids 10-14 have title="hello welcome doc_N" — so multi_match returns ids 0-14.
    List<Row> singleColumn =
        spark
            .sql(String.format("SELECT id FROM %s WHERE lance_match(body, 'hello')", fullTable))
            .collectAsList();
    List<Row> multiColumn =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', body, title)", fullTable))
            .collectAsList();

    List<Integer> idsSingle =
        singleColumn.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    List<Integer> idsMulti =
        multiColumn.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());

    // multi-match result must be a strict superset of single-column result (ids 10-14 added)
    assertTrue(
        idsMulti.size() > idsSingle.size(),
        "lance_multi_match result must be strictly larger than single-column match. "
            + "single="
            + idsSingle
            + " multi="
            + idsMulti);
    assertTrue(
        idsMulti.containsAll(idsSingle),
        "lance_multi_match result must contain all single-column match results. "
            + "single="
            + idsSingle
            + " multi="
            + idsMulti);
  }

  // ── Planning-time validation errors ──────────────────────────────────────

  @Test
  public void testUnknownOptionKeyRaisesAnalysisException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_match(body, 'hello', 'fuzzines=1')",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for unknown option key 'fuzzines'");
    assertTrue(
        ex.getMessage().contains("fuzzines"),
        "Error message must identify the unknown key, got: " + ex.getMessage());
  }

  @Test
  public void testMaxExpansionsZeroRaisesAnalysisException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_match(body, 'hello', 'max_expansions=0')",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for max_expansions=0");
    assertTrue(
        ex.getMessage().contains("max_expansions"),
        "Error message must mention max_expansions, got: " + ex.getMessage());
  }

  @Test
  public void testMalformedBoostValueRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_match(body, 'hello', 'boost=abc')",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for malformed boost value");
  }

  @Test
  public void testLanceMultiMatchAsOrOperandWithScalarRaisesAnalysisException() {
    // Acceptance criterion: lance_multi_match as a direct OR operand with a non-FTS scalar
    // must raise IllegalArgumentException at planning time (OR semantics cannot be preserved after
    // pushdown).
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_multi_match('hello', body, title) OR id > 5",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for lance_multi_match OR scalar");
    assertTrue(
        ex.getMessage().contains("OR"),
        "Error message must explain the OR restriction, got: " + ex.getMessage());
  }

  @Test
  public void testTwoFtsPredicatesIncludingMultiMatchRaisesAnalysisException() {
    // Two FTS predicates in the same WHERE clause must raise IllegalArgumentException regardless of
    // which FTS functions are combined.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_multi_match('hello', body, title) AND lance_match(body, 'world')",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for two FTS predicates in WHERE");
  }

  @Test
  public void testTwoLanceMultiMatchInWhereRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_multi_match('hello', body, title) AND lance_multi_match('world', body, title)",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for two lance_multi_match in WHERE");
  }

  // ── lance_multi_match operator option ────────────────────────────────────

  @Test
  public void testLanceMultiMatchExplicitOrMatchesDefault() {
    // Explicit 'operator=OR' must produce the same result as no options (default OR semantics).
    List<Row> defaultOr =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', body, title)", fullTable))
            .collectAsList();
    List<Row> explicitOr =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', 'operator=OR', body, title)",
                    fullTable))
            .collectAsList();

    List<Integer> idsDefault =
        defaultOr.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    List<Integer> idsExplicit =
        explicitOr.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());

    assertEquals(
        idsDefault,
        idsExplicit,
        "lance_multi_match with operator=OR must return the same rows as no options. "
            + "default="
            + idsDefault
            + " explicit="
            + idsExplicit);
  }

  @Test
  public void testLanceMultiMatchAndReturnsSubsetOfOr() {
    // The operator parameter is passed through to lance-core's MultiMatchQuery. In multi-match,
    // the operator currently does not produce a different result set (lance-core applies OR
    // semantics for term matching within each column regardless of the operator, then unions
    // column results). This test verifies that AND is a (non-strict) subset of OR — the key
    // invariant — and that both queries execute without error.
    List<Row> orResult =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', body, title)", fullTable))
            .collectAsList();
    List<Row> andResult =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', 'operator=AND', body, title)",
                    fullTable))
            .collectAsList();

    List<Integer> idsOr =
        orResult.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    List<Integer> idsAnd =
        andResult.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());

    // AND result must be a subset of OR result (or equal)
    assertTrue(
        idsOr.containsAll(idsAnd),
        "operator=AND result must be a subset of operator=OR result. OR="
            + idsOr
            + " AND="
            + idsAnd);
    // Both must return rows — the fixture has "hello" in body (ids 0-9) and title (ids 10-14)
    assertTrue(idsOr.size() > 0, "operator=OR must return at least one row. OR=" + idsOr);
    assertTrue(idsAnd.size() > 0, "operator=AND must return at least one row. AND=" + idsAnd);
  }

  @Test
  public void testLanceMultiMatchOperatorCaseInsensitive() {
    // 'operator=and' (lowercase) must be accepted and produce the same result as 'operator=AND'.
    List<Row> upper =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', 'operator=AND', body, title)",
                    fullTable))
            .collectAsList();
    List<Row> lower =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE lance_multi_match('hello', 'operator=and', body, title)",
                    fullTable))
            .collectAsList();

    List<Integer> idsUpper =
        upper.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
    List<Integer> idsLower =
        lower.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());

    assertEquals(idsUpper, idsLower, "operator= value must be case-insensitive");
  }

  @Test
  public void testLanceMultiMatchUnknownOptionKeyRaisesAnalysisException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_multi_match('hello', 'boost=2.0', body, title)",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for unknown option key 'boost' in lance_multi_match");
    assertTrue(
        ex.getMessage().contains("boost"),
        "Error message must identify the unknown key, got: " + ex.getMessage());
  }

  @Test
  public void testLanceMultiMatchInvalidOperatorValueRaisesAnalysisException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql(
                    String.format(
                        "SELECT id FROM %s WHERE lance_multi_match('hello', 'operator=MAYBE', body, title)",
                        fullTable))
                .collect(),
        "Expected IllegalArgumentException for invalid operator value");
  }

  @Test
  public void testLanceMultiMatchOptionsWithSingleColumnRaisesAnalysisException() {
    // lance_multi_match('query', 'operator=AND', col1) has arity 3 with an options string
    // at position 1, so colStartIdx=2 and args.size - colStartIdx = 1 < 2 → must reject.
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE lance_multi_match('hello', 'operator=AND', body)",
                            fullTable))
                    .collect(),
            "Expected IllegalArgumentException for lance_multi_match with options and only 1 column");
    assertTrue(
        ex.getMessage().contains("at least 2 column"),
        "Error message must mention minimum column requirement, got: " + ex.getMessage());
  }

  // ── Scenario: pushdown confirmed via scan metadata ───────────────────────

  @Test
  public void testLanceMultiMatchPushdownVisibleInMetadata() {
    Dataset<Row> ds =
        spark.sql(
            String.format(
                "SELECT id FROM %s WHERE lance_multi_match('hello', body, title)", fullTable));
    ds.collect(); // trigger plan execution

    Map<String, String> meta = extractLanceScanMetadata(ds);
    assertNotNull(meta, "Could not find LanceScan in executed plan");
    assertTrue(
        meta.containsKey("fullTextQuery"),
        "Expected 'fullTextQuery' key in scan metadata after multi_match pushdown");
    assertNotNull(meta.get("fullTextQuery"), "fullTextQuery metadata value must not be null");
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  protected Map<String, String> extractLanceScanMetadata(Dataset<Row> ds) {
    return LanceScanTestHelper.extractLanceScanMetadata(ds);
  }
}
