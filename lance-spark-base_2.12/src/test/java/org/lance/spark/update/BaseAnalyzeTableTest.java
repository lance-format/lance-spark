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
package org.lance.spark.update;

import org.lance.spark.read.LanceStatsKeys;
import org.lance.spark.read.LanceStatsTestKeys;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for {@code ANALYZE TABLE … COMPUTE STATISTICS}. Exercises the full stack:
 * grammar → AST → logical plan → {@code LanceAnalyzeTableExec} → {@code catalog.alterTable} →
 * manifest write → {@code LanceScanBuilder.loadPersistedColumnStats}. Asserts (a) the TBLPROPERTIES
 * write happens with the expected wire-format keys, (b) the persisted payload round-trips through
 * the read path back into Spark's {@code Statistics.columnStats()}, and (c) staleness invalidates
 * the cached stats so a subsequent INSERT correctly forces fallback.
 *
 * <p>Sub-classed per Spark major version so each module's parser-strategy wiring is exercised.
 */
public abstract class BaseAnalyzeTableTest {

  protected final String catalogName = "lance_test";

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-analyze-table-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      // Clear active+default session refs so that a follow-up test class calling
      // SparkSession.builder().getOrCreate() builds a fresh session with its own catalog config.
      // Without this, the next test inherits this class's catalog wiring and silently
      // mis-routes ANALYZE TABLE / SELECT to the wrong namespace.
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      spark.close();
      spark = null;
    }
  }

  private TableCatalog catalog() {
    return (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  private java.util.Map<String, String> properties(String tableName) throws Exception {
    Table table = catalog().loadTable(Identifier.of(new String[] {"default"}, tableName));
    return table.properties();
  }

  /**
   * Build a fresh scan for the table and return the per-column statistics it reports to Spark's
   * CBO. A fresh scan re-reads the current manifest version, so this reflects the read path's
   * staleness/gate decisions at call time.
   */
  private java.util.Map<
          org.apache.spark.sql.connector.expressions.NamedReference,
          org.apache.spark.sql.connector.read.colstats.ColumnStatistics>
      scanColumnStats(String tableName) throws Exception {
    Table table = catalog().loadTable(Identifier.of(new String[] {"default"}, tableName));
    org.apache.spark.sql.connector.read.Scan scan =
        ((org.apache.spark.sql.connector.catalog.SupportsRead) table)
            .newScanBuilder(org.apache.spark.sql.util.CaseInsensitiveStringMap.empty())
            .build();
    return ((org.apache.spark.sql.connector.read.SupportsReportStatistics) scan)
        .estimateStatistics()
        .columnStats();
  }

  /**
   * Build a scan with a pushed {@code id >= lowerBound} predicate (so zonemap fragment pruning can
   * engage) and return its reported statistics.
   */
  private org.apache.spark.sql.connector.read.Statistics prunedScanStats(
      String tableName, long lowerBound) throws Exception {
    Table table = catalog().loadTable(Identifier.of(new String[] {"default"}, tableName));
    org.apache.spark.sql.connector.read.ScanBuilder sb =
        ((org.apache.spark.sql.connector.catalog.SupportsRead) table)
            .newScanBuilder(org.apache.spark.sql.util.CaseInsensitiveStringMap.empty());
    org.apache.spark.sql.connector.expressions.filter.Predicate ge =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            ">=",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              org.apache.spark.sql.connector.expressions.FieldReference.column("id"),
              new org.apache.spark.sql.connector.expressions.LiteralValue<>(
                  lowerBound, org.apache.spark.sql.types.DataTypes.LongType)
            });
    ((org.apache.spark.sql.connector.read.SupportsPushDownV2Filters) sb)
        .pushPredicates(new org.apache.spark.sql.connector.expressions.filter.Predicate[] {ge});
    return ((org.apache.spark.sql.connector.read.SupportsReportStatistics) sb.build())
        .estimateStatistics();
  }

  @Test
  public void analyzeForAllColumnsPersistsExpectedKeys() throws Exception {
    String tableName = "t_for_all_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    // Top-level wire-format tags must be present.
    assertEquals(LanceStatsKeys.SUPPORTED_VERSION, props.get(LanceStatsKeys.VERSION));
    assertEquals("3", props.get(LanceStatsKeys.NUM_ROWS));
    assertNotNull(props.get(LanceStatsKeys.COMPUTED_AT_VERSION));
    assertNotNull(props.get(LanceStatsKeys.SCHEMA_HASH));
    assertNotNull(props.get(LanceStatsKeys.COMPUTED_AT));

    // Per-column stats use Spark's CatalogColumnStat string form. For a BIGINT column, min/max are
    // plain decimal external strings, and a per-column .version key is always present.
    String idMinKey = LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN;
    String idMaxKey = LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MAX;
    assertEquals("1", props.get(idMinKey));
    assertEquals("3", props.get(idMaxKey));
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_VERSION),
        "per-column .version key (required by CatalogColumnStat.fromMap) must be present");
    assertNotNull(
        props.get(
            LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT),
        "distinctCount must be persisted");

    // String columns: Spark's computeColumnStats does NOT compute min/max for StringType (matching
    // native ANALYZE), but DOES compute nullCount / distinctCount / avgLen / maxLen.
    assertNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN),
        "Spark does not compute min/max for string columns");
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsTestKeys.COLUMN_SUFFIX_MAX_LEN),
        "string column maxLen must be persisted");
    assertNotNull(
        props.get(
            LanceStatsKeys.COLUMN_PREFIX
                + "name"
                + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT),
        "string column distinctCount must be persisted");
  }

  @Test
  public void analyzeForColumnsSubsetOnlyPersistsRequestedColumns() throws Exception {
    String tableName = "t_subset_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id INT, payload STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'x'), (2, 'y')");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR COLUMNS id");

    java.util.Map<String, String> props = properties(tableName);
    // id stats present.
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
    // payload stats absent (subset).
    assertNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "payload" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
  }

  @Test
  public void analyzeComputesApproximateDistinctCount() throws Exception {
    String tableName = "t_ndv_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2), (3), (1), (2)");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    // NDV is always HLL-approximate (Spark's native computeColumnStats behavior); the APPROX
    // keyword was removed with the custom grammar. distinctCount must be persisted and positive
    // (3 distinct values → HLL returns 3 for this tiny cardinality).
    java.util.Map<String, String> props = properties(tableName);
    String ndv =
        props.get(
            LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT);
    assertNotNull(ndv, "distinctCount must be persisted");
    assertTrue(Long.parseLong(ndv) > 0, "distinctCount must be positive, got: " + ndv);
  }

  @Test
  public void subsequentInsertInvalidatesPersistedStats() throws Exception {
    String tableName = "t_invalidate_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2)");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    long taggedVersion = Long.parseLong(props.get(LanceStatsKeys.COMPUTED_AT_VERSION));

    // A subsequent INSERT bumps the manifest version; the persisted-stats version-equality
    // check on the read path should now reject these stats and fall back to live aggregation.
    spark.sql("INSERT INTO " + fullName + " VALUES (3)");

    // Stats keys are still on disk (we don't proactively delete); the reader's exact-equality
    // check is what protects correctness. Sanity-check the key still exists with the OLD value
    // — the read path's protection is logical, not physical.
    java.util.Map<String, String> after = properties(tableName);
    assertEquals(
        Long.toString(taggedVersion),
        after.get(LanceStatsKeys.COMPUTED_AT_VERSION),
        "INSERT should not modify persisted-stats keys; the read path detects staleness");
  }

  @Test
  public void analyzeReturnsResultRowWithAnalyzedCount() throws Exception {
    String tableName = "t_result_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (a INT, b STRING, c DOUBLE) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'x', 1.5)");

    List<Row> rows =
        spark
            .sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS")
            .collectAsList();
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    // Per LanceAnalyzeTableOutputType.SCHEMA: (columns_analyzed, rows_scanned, manifest_version).
    assertEquals(3, r.getInt(0)); // 3 columns analyzed
    assertEquals(1L, r.getLong(1)); // 1 row
    assertTrue(r.getLong(2) > 0); // manifest_version > 0
  }

  @Test
  public void reAnalyzeOnUnchangedSchemaIsIdempotent() throws Exception {
    // Idempotency check: running ANALYZE twice on an unchanged table produces the same per-
    // column stats keys AND the same schemaHash. The orphan-cleanup pass during the second run
    // sees no orphans (every key belongs to a still-present column) and the per-column writes
    // overwrite with identical values. Only computedAt (ISO timestamp) is allowed to differ.
    String tableName = "t_idempotent_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'a'), (2, 'b')");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");
    java.util.Map<String, String> first = new java.util.HashMap<>(properties(tableName));

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");
    java.util.Map<String, String> second = properties(tableName);

    // Per-column key stability.
    for (String key : first.keySet()) {
      if (!key.startsWith(LanceStatsKeys.COLUMN_PREFIX)) {
        continue;
      }
      assertEquals(first.get(key), second.get(key), "Stat key '" + key + "' should be stable");
    }
    // Schema hash must be deterministic — a regression here would invalidate persisted stats
    // on every read.
    assertEquals(
        first.get(LanceStatsKeys.SCHEMA_HASH),
        second.get(LanceStatsKeys.SCHEMA_HASH),
        "schemaHash must be stable across re-ANALYZE on unchanged schema");
    // Numeric counts must also be identical.
    assertEquals(first.get(LanceStatsKeys.NUM_ROWS), second.get(LanceStatsKeys.NUM_ROWS));
    assertEquals(first.get(LanceStatsKeys.VERSION), second.get(LanceStatsKeys.VERSION));
  }

  @Test
  public void analyzeTableMalformedSyntaxSurfacesParseError() throws Exception {
    // A malformed ANALYZE TABLE must surface a parse error, NOT Spark's confusing
    // NOT_SUPPORTED_COMMAND_FOR_V2_TABLE. `FOR COLUMNS` with no column list is a syntax error in
    // Spark's native grammar, raised at parse time — before resolution and the resolution-rule
    // interception — so the user sees a positioned parse error, never the V2-table rejection.
    String tableName = "t_malformed_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");

    Exception ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            Exception.class,
            () -> spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR COLUMNS"));
    String msg = ex.getMessage() == null ? "" : ex.getMessage();
    assertFalse(
        msg.contains("NOT_SUPPORTED_COMMAND_FOR_V2_TABLE"),
        "Lance parse error should surface, not Spark's V2 rejection. Got: " + msg);
  }

  @Test
  public void analyzeTableWithLeadingCommentIsHandled() throws Exception {
    // ANALYZE TABLE with leading SQL comments must still reach the resolution-rule interception.
    // Spark's native parser skips leading comments natively, parses AnalyzeColumn, and
    // LanceAnalyzeTableResolution rewrites it for the Lance table — so stats are persisted rather
    // than the analyzer rejecting it with NOT_SUPPORTED_COMMAND_FOR_V2_TABLE.
    String tableName = "t_comment_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1)");

    // Block comment + line comment + extra whitespace before the keyword.
    spark.sql(
        "/* hint */\n-- another\n  ANALYZE TABLE "
            + fullName
            + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    // ANALYZE persisted a stats payload — the version tag is the read path's "stats present" gate,
    // and the id column carries a min/max — so interception happened past the leading comments.
    assertEquals(LanceStatsKeys.SUPPORTED_VERSION, props.get(LanceStatsKeys.VERSION));
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
  }

  @Test
  public void killSwitchSuppressesPersistedStatsConsumption() throws Exception {
    // R19: spark.lance.cbo.column.stats.enabled=false is the production rollback path. Must
    // suppress persisted-stats consumption end-to-end. Hard to assert "no stats consumed"
    // directly (no driver-side metric); assert that scans complete successfully and produce
    // correct rows even with the kill-switch on, AND that the persisted TBLPROPERTIES are
    // unchanged (the writer path is unaffected by the read-side kill-switch).
    String tableName = "t_killswitch_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2), (3)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> propsBefore = properties(tableName);
    String hashBefore = propsBefore.get(LanceStatsKeys.SCHEMA_HASH);
    assertNotNull(hashBefore, "ANALYZE should have written stats");

    try {
      spark.conf().set(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ENABLED, "false");
      // Scan must succeed and return correct rows. With the kill-switch on, the read path's
      // CBO column-stats fast path is skipped entirely (no persisted-stats consumption, no
      // zonemap aggregation). Underlying scan correctness is unaffected.
      long count = spark.sql("SELECT COUNT(*) FROM " + fullName).collectAsList().get(0).getLong(0);
      assertEquals(3L, count);
      long selected = spark.sql("SELECT * FROM " + fullName).count();
      assertEquals(3L, selected);
    } finally {
      spark.conf().unset(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ENABLED);
    }

    // Persisted-stats keys are untouched by the kill-switch — re-flipping enabled=true should
    // bring back the fast path without re-running ANALYZE.
    java.util.Map<String, String> propsAfter = properties(tableName);
    assertEquals(hashBefore, propsAfter.get(LanceStatsKeys.SCHEMA_HASH));
  }

  @Test
  public void readPathRejectsStaleStatsAfterInsert() throws Exception {
    // Prove the read-path staleness check fires at runtime by inspecting the stats the scan
    // actually reports to the CBO: fresh ANALYZE stats reach columnStats(), but after an INSERT
    // bumps the manifest version the exact-version check (default allow.stale=false) rejects them,
    // so the scan surfaces NO column stats for the column. (Asserting on row count would be
    // vacuous — the scan's rowCount is always live from the manifest, never from the persisted
    // payload, so it can't distinguish a working staleness guard from a broken one.)
    String tableName = "t_runtime_stale_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    org.apache.spark.sql.connector.expressions.NamedReference idRef =
        org.apache.spark.sql.connector.expressions.FieldReference.column("id");
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    // Fresh: the persisted stats reach the scan's columnStats().
    assertTrue(
        scanColumnStats(tableName).containsKey(idRef),
        "fresh ANALYZE stats must reach Statistics.columnStats()");

    // A subsequent INSERT bumps the manifest version → computedAtVersion < currentVersion → the
    // read path rejects the now-stale stats and surfaces no column stats for id.
    spark.sql("INSERT INTO " + fullName + " VALUES (3)");
    assertFalse(
        scanColumnStats(tableName).containsKey(idRef),
        "stale ANALYZE stats must be rejected after an INSERT bumps the manifest version");

    // Sanity: SELECTs still return correct (live) data.
    assertEquals(
        3L, spark.sql("SELECT COUNT(*) FROM " + fullName).collectAsList().get(0).getLong(0));
  }

  @Test
  public void reAnalyzeClearsStaleKeysWhenColumnBecomesAllNull() throws Exception {
    String tableName = "t_reanalyze_clear_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, v BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 10), (2, 20), (3, 30)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    String vMin = LanceStatsKeys.COLUMN_PREFIX + "v" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN;
    String vMax = LanceStatsKeys.COLUMN_PREFIX + "v" + LanceStatsTestKeys.COLUMN_SUFFIX_MAX;
    String vNdv =
        LanceStatsKeys.COLUMN_PREFIX + "v" + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT;
    String vNull = LanceStatsKeys.COLUMN_PREFIX + "v" + LanceStatsTestKeys.COLUMN_SUFFIX_NULL_COUNT;
    java.util.Map<String, String> run1 = properties(tableName);
    assertNotNull(run1.get(vMin), "run1 should persist v.min");
    assertNotNull(run1.get(vNdv), "run1 should persist v.distinctCount");

    // v becomes all-NULL; re-ANALYZE must CLEAR the now-invalid min/max keys (Spark computes no
    // min/max for an all-null column) so the reader never surfaces a prior run's bounds tagged with
    // the fresh manifest version. distinctCount is overwritten to "0" (Spark's computeColumnStats
    // emits NDV=0 for all-null); the reader treats a 0 NDV as "no distinct-count info".
    spark.sql(
        "INSERT OVERWRITE "
            + fullName
            + " VALUES (1, CAST(NULL AS BIGINT)), (2, CAST(NULL AS BIGINT))");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> run2 = properties(tableName);
    assertNull(run2.get(vMin), "stale v.min must be cleared after re-ANALYZE");
    assertNull(run2.get(vMax), "stale v.max must be cleared after re-ANALYZE");
    assertEquals("0", run2.get(vNdv), "v.distinctCount must be overwritten to 0 (all-null column)");
    assertEquals("2", run2.get(vNull), "v.nullCount must reflect the 2 nulls");
    // id is still non-null, so its stats survive the re-ANALYZE.
    assertNotNull(
        run2.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
  }

  @Test
  public void analyzeDecimalColumnPersistsDecStat() throws Exception {
    String tableName = "t_decimal_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, amount DECIMAL(10,2)) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 1.50), (2, 9.99)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    // DECIMAL min/max round-trip through Spark's CatalogColumnStat external-string form (the plain
    // decimal text, e.g. "1.50"), not a tagged codec form. Compare numerically to be robust to
    // scale formatting.
    java.util.Map<String, String> props = properties(tableName);
    String amountMin =
        props.get(LanceStatsKeys.COLUMN_PREFIX + "amount" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN);
    assertNotNull(amountMin, "DECIMAL min must be persisted, not dropped");
    assertEquals(
        0,
        new java.math.BigDecimal(amountMin).compareTo(new java.math.BigDecimal("1.50")),
        "expected DECIMAL min == 1.50, got: " + amountMin);
  }

  @Test
  public void analyzePersistedStatsReachScanColumnStats() throws Exception {
    String tableName = "t_colstats_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    // End-to-end: the persisted payload must round-trip through the scan back into Spark's
    // DSv2 Statistics.columnStats() — proving the full write→read wiring, not just each half.
    java.util.Map<
            org.apache.spark.sql.connector.expressions.NamedReference,
            org.apache.spark.sql.connector.read.colstats.ColumnStatistics>
        cs = scanColumnStats(tableName);
    assertNotNull(cs);
    org.apache.spark.sql.connector.expressions.NamedReference idRef =
        org.apache.spark.sql.connector.expressions.FieldReference.column("id");
    assertTrue(
        cs.containsKey(idRef), "ANALYZE-persisted stats should reach Statistics.columnStats()");
    assertEquals(1L, cs.get(idRef).min().get());
    assertEquals(3L, cs.get(idRef).max().get());
  }

  @Test
  public void prunedScanDoesNotSurfaceFullTableColumnStats() throws Exception {
    // When zonemap fragment pruning shrinks the scanned row count, the persisted FULL-table stats
    // no longer describe the scanned subset (e.g. distinctCount could exceed the pruned numRows),
    // so the scan must NOT surface them — otherwise the CBO gets internally-inconsistent stats.
    String tableName = "t_prune_stats_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, region STRING) USING lance");
    // 3 separate INSERTs → 3 fragments with disjoint id ranges, so a zonemap on id can prune.
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'a')");
    spark.sql("INSERT INTO " + fullName + " VALUES (2, 'b')");
    spark.sql("INSERT INTO " + fullName + " VALUES (3, 'c')");
    spark.sql("ALTER TABLE " + fullName + " CREATE INDEX id_idx USING zonemap (id)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    org.apache.spark.sql.connector.expressions.NamedReference idRef =
        org.apache.spark.sql.connector.expressions.FieldReference.column("id");
    // Baseline: no predicate → no pruning → persisted stats ARE surfaced.
    assertTrue(
        scanColumnStats(tableName).containsKey(idRef),
        "without pruning, persisted stats must be surfaced");

    // id >= 3 prunes the first two fragments; the reported row count drops below the table total,
    // so the full-table persisted stats must NOT be surfaced.
    org.apache.spark.sql.connector.read.Statistics pruned = prunedScanStats(tableName, 3L);
    assertTrue(
        pruned.numRows().getAsLong() < 3L,
        "precondition: id>=3 must prune fragments (numRows=" + pruned.numRows().getAsLong() + ")");
    assertTrue(
        pruned.columnStats().isEmpty(),
        "fragment-pruned scan must not surface full-table column stats");
  }

  @Test
  public void partialReAnalyzeClearsStatsForColumnsNotAnalyzed() throws Exception {
    String tableName = "t_partial_reanalyze_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, v BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 100), (2, 200)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    String vMin = LanceStatsKeys.COLUMN_PREFIX + "v" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN;
    String vMax = LanceStatsKeys.COLUMN_PREFIX + "v" + LanceStatsTestKeys.COLUMN_SUFFIX_MAX;
    assertNotNull(properties(tableName).get(vMin), "FOR ALL COLUMNS run should persist v.min");

    // The data changes, then ONLY `id` is re-analyzed. v's prior stats are now stale; the partial
    // run must CLEAR them — otherwise the reader would serve them as fresh under the re-advanced
    // table-level computedAtVersion (false-accept staleness).
    spark.sql("INSERT INTO " + fullName + " VALUES (3, 999)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR COLUMNS id");

    java.util.Map<String, String> props = properties(tableName);
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN),
        "re-analyzed id stats must be present");
    assertNull(props.get(vMin), "stale v.min (not analyzed this run) must be cleared");
    assertNull(props.get(vMax), "stale v.max (not analyzed this run) must be cleared");
  }

  @Test
  public void analyzeNoscanFallsThroughToSpark() throws Exception {
    String tableName = "t_noscan_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2)");

    // NOSCAN parses natively as AnalyzeTable(noScan=true). LanceAnalyzeTableResolution deliberately
    // does NOT intercept noScan (column stats require a scan), so it falls through to Spark's V2
    // ANALYZE rejection rather than silently computing all-column stats.
    Exception ex =
        assertThrows(
            Exception.class,
            () -> spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS NOSCAN"));
    String msg = String.valueOf(ex.getMessage()).toLowerCase(java.util.Locale.ROOT);
    assertTrue(
        msg.contains("v2") || msg.contains("not supported"),
        "NOSCAN should reach Spark's V2 ANALYZE rejection, got: " + ex.getMessage());
  }

  @Test
  public void analyzeEmptyTableSucceeds() throws Exception {
    String tableName = "t_empty_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    // No rows inserted — ANALYZE must still complete and persist a 0-row payload, not crash.
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    assertEquals(LanceStatsKeys.SUPPORTED_VERSION, props.get(LanceStatsKeys.VERSION));
    assertEquals("0", props.get(LanceStatsKeys.NUM_ROWS));
    // An all-empty column has no min/max; only nullCount (= 0) is persisted.
    assertNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
    assertEquals(
        "0",
        props.get(
            LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_NULL_COUNT));
  }

  @Test
  public void analyzeWithoutForClauseDefaultsToAllColumns() throws Exception {
    String tableName = "t_no_for_clause_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'a'), (2, 'b')");
    // Bare COMPUTE STATISTICS (no FOR clause) is treated as FOR ALL COLUMNS by this connector.
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS");

    java.util.Map<String, String> props = properties(tableName);
    assertEquals(LanceStatsKeys.SUPPORTED_VERSION, props.get(LanceStatsKeys.VERSION));
    // id (BIGINT) gets min/max; name (STRING) gets no min/max but is still analyzed — assert via
    // its distinctCount, proving the bare COMPUTE STATISTICS covered every column.
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
    assertNotNull(
        props.get(
            LanceStatsKeys.COLUMN_PREFIX
                + "name"
                + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT));
  }

  @Test
  public void analyzeSkipsTimestampNtzColumn() throws Exception {
    String tableName = "t_tsntz_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, ts TIMESTAMP_NTZ) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, TIMESTAMP_NTZ '2020-01-01 00:00:00')");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    // id is analyzed; the TimestampNTZ column is skipped entirely — its persisted min/max would
    // crash Spark's CBO FilterEstimation (no toDouble case → MatchError) on any filtered query.
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
    assertNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "ts" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
    assertNull(
        props.get(
            LanceStatsKeys.COLUMN_PREFIX + "ts" + LanceStatsTestKeys.COLUMN_SUFFIX_NULL_COUNT));
  }

  @Test
  public void analyzeForColumnsIsCaseInsensitiveByDefault() throws Exception {
    String tableName = "t_ci_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'a'), (2, 'b')");
    // spark.sql.caseSensitive defaults to false, so FOR COLUMNS with a different case must resolve
    // (matching Spark's native column resolution) instead of failing with "column not found".
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR COLUMNS ID");

    java.util.Map<String, String> props = properties(tableName);
    // Persisted under the table's stored column case ("id"), and present.
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
    // name was not requested → absent.
    assertNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
  }

  @Test
  public void analyzeOnNonLanceTableIsNotHijacked() throws Exception {
    // The headline correctness win: with the Lance extension enabled, ANALYZE on a NON-Lance
    // (session-catalog) table must run Spark's native ANALYZE — the resolution rule keys on
    // ResolvedTable(LanceDataset), so a non-Lance table is left untouched and NOT rewritten to
    // LanceAnalyzeTable (which would throw "only Lance tables supported").
    String t = "nonlance_" + System.nanoTime();
    spark.sql("CREATE TABLE " + t + " (id INT, name STRING) USING parquet");
    spark.sql("INSERT INTO " + t + " VALUES (1, 'a'), (2, 'b')");

    // Must not throw the Lance rejection — runs natively.
    spark.sql("ANALYZE TABLE " + t + " COMPUTE STATISTICS FOR ALL COLUMNS");

    // Proof it ran natively: Spark's session catalog now holds V1 statistics for the table, which
    // the Lance ANALYZE path never writes.
    org.apache.spark.sql.catalyst.catalog.CatalogTable meta =
        spark
            .sessionState()
            .catalog()
            .getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier.apply(t));
    assertTrue(meta.stats().isDefined(), "native ANALYZE must populate session-catalog statistics");
    spark.sql("DROP TABLE " + t);
  }

  @Test
  public void analyzeOnTempViewIsNotHijacked() {
    // A temp view resolves to a View node, not ResolvedTable, so the Lance rule must not intercept
    // it. Spark may accept or reject ANALYZE on a temp view, but it must never be the Lance
    // rejection (which would mean the rule hijacked a non-table relation).
    String view = "tmpview_" + System.nanoTime();
    spark.range(5).createOrReplaceTempView(view);
    try {
      spark.sql("ANALYZE TABLE " + view + " COMPUTE STATISTICS FOR ALL COLUMNS");
    } catch (Exception e) {
      String msg = String.valueOf(e.getMessage());
      assertFalse(
          msg.contains("only Lance tables") || msg.contains("BaseLanceNamespaceSparkCatalog"),
          "ANALYZE on a temp view must not be hijacked by the Lance rule. Got: " + msg);
    } finally {
      // Positive anchor (runs whether or not ANALYZE threw): the view is intact and still
      // queryable, proving the rule neither rewrote nor broke a non-table relation.
      assertEquals(5L, spark.table(view).count());
      spark.catalog().dropTempView(view);
    }
  }

  @Test
  public void histogramEnabledDoesNotPersistHistogramButRoundTripsStats() throws Exception {
    // Even with spark.sql.statistics.histogram.enabled=true in the session, LanceNativeColumnStats
    // forces the flag off for the aggregation, so Spark's computeColumnStats never builds a
    // histogram and toMap emits no `<col>.histogram[.partN]` payload. min/max/etc. still round-trip
    // but no histogram key is persisted — avoiding the wasted percentile-sketch pass, TBLPROPERTIES
    // bloat, and an unbounded-deserialize DoS on read.
    String tableName = "t_hist_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2), (3), (4), (5)");
    try {
      spark.conf().set("spark.sql.statistics.histogram.enabled", "true");
      spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");
    } finally {
      spark.conf().unset("spark.sql.statistics.histogram.enabled");
    }

    java.util.Map<String, String> props = properties(tableName);
    assertEquals(
        "1", props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN));
    String histogramPrefix =
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_HISTOGRAM;
    for (String k : props.keySet()) {
      assertFalse(
          k.startsWith(histogramPrefix), "histogram must not be persisted, found key: " + k);
    }
  }
}
