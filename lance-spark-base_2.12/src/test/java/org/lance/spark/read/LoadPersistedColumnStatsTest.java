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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LanceScanBuilder#loadPersistedColumnStats} fast-path branches. The fast
 * path is the entire premise of ANALYZE TABLE persistent stats; a regression in any branch silently
 * degrades CBO without a failing test, so we test each guard independently rather than relying on
 * end-to-end coverage alone.
 *
 * <p>The per-column payload is Spark's own {@code CatalogColumnStat} map form (written by {@code
 * CatalogColumnStat.toMap}, read by {@code CatalogColumnStat.fromMap}) under the {@code
 * lance.stats.column.} prefix: each column needs a {@code .version} key (Spark's serialization
 * version, required by {@code fromMap}) plus any of {@code .min/.max/.nullCount/.distinctCount/
 * .avgLen/.maxLen}. min/max are plain external strings (decimal for Long, the raw text for String).
 *
 * <p>Tests pass {@code session = null} since the only session use is the {@code allowStale}
 * SparkConf lookup, which gracefully falls through to {@code allowStale=false} when no session is
 * bound.
 */
class LoadPersistedColumnStatsTest {

  /**
   * Spark's {@code CatalogColumnStat} serialization version (the value of {@code
   * CatalogColumnStat.VERSION}). Stable at 2 since histogram support landed; {@code fromMap}
   * requires the per-column {@code .version} key to be present and integer-parseable.
   */
  private static final String CATALOG_STAT_VERSION = "2";

  /**
   * Local SparkSession used by the {@code allowStale} branch tests. Built once for the class so the
   * per-test cost stays low; the rest of the tests pass {@code session = null} since they don't
   * exercise SparkConf.
   */
  private static SparkSession spark;

  @BeforeAll
  static void setUpSession() {
    spark =
        SparkSession.builder()
            .appName("LoadPersistedColumnStatsTest")
            .master("local")
            .getOrCreate();
  }

  @AfterAll
  static void tearDownSession() {
    if (spark != null) {
      // Clear active+default refs: SparkSession.builder().getOrCreate() in a sibling test class
      // returns this thread's active session if one is still bound, silently inheriting our
      // (non-catalog-configured) builder.
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      spark.stop();
      spark = null;
    }
  }

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.LongType, true, null),
            new StructField("name", DataTypes.StringType, true, null)
          });

  /** Put the per-column {@code .version} key Spark's {@code fromMap} requires. */
  private static void putVersion(Map<String, String> p, String col) {
    p.put(
        LanceStatsKeys.COLUMN_PREFIX + col + LanceStatsTestKeys.COLUMN_SUFFIX_VERSION,
        CATALOG_STAT_VERSION);
  }

  /** Build a properties map representing a fully-valid stats payload at version=42. */
  private static Map<String, String> validProps() {
    Map<String, String> p = new HashMap<>();
    p.put(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION);
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "42");
    p.put(LanceStatsKeys.SCHEMA_HASH, LanceStatsKeys.computeSchemaHash(SCHEMA));
    putVersion(p, "id");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN, "1");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MAX, "100");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_NULL_COUNT, "5");
    p.put(
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT,
        "95");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_AVG_LEN, "8");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MAX_LEN, "8");
    return p;
  }

  @Test
  @DisplayName("Valid payload returns ColumnStatistics for analyzed column")
  void validPayloadReturnsStats() {
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, SCHEMA, 42L);
    assertNotNull(result);
    NamedReference idRef = FieldReference.column("id");
    assertTrue(result.containsKey(idRef));
    ColumnStatistics idStats = result.get(idRef);
    // LongType internal rep == external long, so min/max surface as java.lang.Long.
    assertEquals(1L, idStats.min().get());
    assertEquals(100L, idStats.max().get());
    assertEquals(5L, idStats.nullCount().getAsLong());
    assertEquals(95L, idStats.distinctCount().getAsLong());
    // avgLen/maxLen must round-trip too (they feed Spark's join-size estimation).
    assertEquals(8L, idStats.avgLen().getAsLong());
    assertEquals(8L, idStats.maxLen().getAsLong());
  }

  @Test
  @DisplayName("formatVersion absent → fall back (fail-safe gate)")
  void formatVersionAbsentRejected() {
    Map<String, String> p = validProps();
    p.remove(LanceStatsKeys.VERSION);
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("formatVersion=2 (unrecognized) → fall back")
  void formatVersionUnrecognizedRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.VERSION, "2");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("computedAtVersion mismatch + allowStale unset → fall back")
  void staleVersionRejected() {
    // currentManifestVersion=43 but stats are tagged at version=42 — must reject without an
    // active session bound (so allowStale defaults to false).
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, SCHEMA, 43L));
  }

  @Test
  @DisplayName("computedAtVersion malformed → fall back")
  void computedAtVersionMalformedRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "not-a-number");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("computedAtVersion negative → fall back")
  void computedAtVersionNegativeRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "-1");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("schemaHash mismatch → fall back (drift guard)")
  void schemaHashMismatchRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.SCHEMA_HASH, "0".repeat(64)); // not the SCHEMA's hash
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("schemaHash absent → fall back (fail-safe)")
  void schemaHashAbsentRejected() {
    Map<String, String> p = validProps();
    p.remove(LanceStatsKeys.SCHEMA_HASH);
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Empty properties → null (no stats to consume)")
  void emptyPropsReturnsNull() {
    assertNull(
        LanceScanBuilder.loadPersistedColumnStats(null, new HashMap<>(), SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Null properties → null")
  void nullPropsReturnsNull() {
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, null, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Poisoned min that does not parse for the type → whole column dropped (fail-safe)")
  void poisonedMinDropsColumn() {
    // With Spark's CatalogColumnStat codec, a min that won't parse for the column's type
    // (here a non-numeric string for a Long column) makes toPlanStat → fromExternalString throw.
    // The reader degrades the WHOLE column to "no stats" rather than surfacing a partial stat.
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN, "not-a-long");
    // id is the only analyzed column, so dropping it yields a null result map.
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Missing per-column .version → column dropped (fromMap requires it)")
  void missingVersionDropsColumn() {
    Map<String, String> p = validProps();
    p.remove(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_VERSION);
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Projected schema is a subset → only projected columns appear")
  void projectionFiltersStats() {
    // Add full stats for "name" too, then project only "id".
    Map<String, String> p = validProps();
    putVersion(p, "name");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN, "alice");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsTestKeys.COLUMN_SUFFIX_MAX, "zach");
    StructType projected = new StructType(new StructField[] {SCHEMA.fields()[0]});
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, projected, 42L);
    assertNotNull(result);
    assertTrue(result.containsKey(FieldReference.column("id")));
    assertEquals(1, result.size(), "name should not appear in projected result");
  }

  @Test
  @DisplayName("Projected schema with no analyzed columns → null (caller falls back)")
  void projectedSchemaWithNoStatsReturnsNull() {
    // ANALYZE wrote stats for "id" only; project only "name". All sentinels pass but no
    // per-column entries match the projection, so the method returns null and the caller
    // falls back to live aggregation.
    StructType nameOnly = new StructType(new StructField[] {SCHEMA.fields()[1]});
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, nameOnly, 42L);
    assertNull(result);
  }

  @Test
  @DisplayName("Column with no stats keys at all is skipped (not a malformed-decode case)")
  void columnWithoutAnalyzeIsSkipped() {
    // "name" never had ANALYZE stats written; only "id" did. result should contain only "id".
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, SCHEMA, 42L);
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.containsKey(FieldReference.column("id")));
  }

  @Test
  @DisplayName("allowStale=true → accept stats whose computedAtVersion lags currentManifestVersion")
  void allowStaleAcceptsStaleVersion() {
    try {
      spark.conf().set(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE, "true");
      // computedAtVersion=42, currentManifestVersion=43 — stale, but allowStale=true accepts.
      Map<NamedReference, ColumnStatistics> result =
          LanceScanBuilder.loadPersistedColumnStats(spark, validProps(), SCHEMA, SCHEMA, 43L);
      assertNotNull(result, "allowStale=true should accept stale stats");
      assertTrue(result.containsKey(FieldReference.column("id")));
    } finally {
      spark.conf().unset(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE);
    }
  }

  @Test
  @DisplayName("allowStale=false (explicit) → still reject stale version")
  void allowStaleFalseStillRejectsStale() {
    try {
      spark.conf().set(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE, "false");
      Map<NamedReference, ColumnStatistics> result =
          LanceScanBuilder.loadPersistedColumnStats(spark, validProps(), SCHEMA, SCHEMA, 43L);
      assertNull(result, "allowStale=false must reject stale stats");
    } finally {
      spark.conf().unset(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE);
    }
  }

  @Test
  @DisplayName("allowStale unset + session present → defaults to false (rejects stale)")
  void allowStaleUnsetDefaultsToFalse() {
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(spark, validProps(), SCHEMA, SCHEMA, 43L);
    assertNull(result);
  }

  @Test
  @DisplayName("Negative nullCount is treated as corrupt → whole column dropped (range guard)")
  void negativeNullCountRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_NULL_COUNT, "-1");
    // id was the only analyzed column; negative nullCount drops it → result map is empty → null.
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("distinctCount=0 keeps the column but omits NDV; negative drops the column")
  void distinctCountZeroKeepsColumnNegativeDrops() {
    // distinctCount == 0 is legitimate (e.g. an all-null column): keep min/max/nullCount, drop NDV.
    Map<String, String> zero = validProps();
    zero.put(
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT, "0");
    Map<NamedReference, ColumnStatistics> zeroResult =
        LanceScanBuilder.loadPersistedColumnStats(null, zero, SCHEMA, SCHEMA, 42L);
    assertNotNull(zeroResult);
    ColumnStatistics idStats = zeroResult.get(FieldReference.column("id"));
    assertNotNull(idStats);
    assertTrue(idStats.distinctCount().isEmpty(), "distinctCount=0 → empty NDV");
    assertEquals(1L, idStats.min().get(), "min still surfaced");
    assertEquals(5L, idStats.nullCount().getAsLong(), "nullCount still surfaced");

    // A negative distinctCount is corrupt → whole column dropped (fail-safe).
    Map<String, String> negative = validProps();
    negative.put(
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT,
        "-5");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, negative, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("All-null column: nullCount present, no min/max/distinctCount → kept with empty NDV")
  void allNullColumnKeepsNullCountWithoutDistinctCount() {
    // Mirrors what the writer persists for an all-NULL column: only version + nullCount, no
    // min/max/distinctCount. The reader must KEEP the column (surfacing nullCount), not drop it.
    Map<String, String> p = new HashMap<>();
    p.put(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION);
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "42");
    p.put(LanceStatsKeys.SCHEMA_HASH, LanceStatsKeys.computeSchemaHash(SCHEMA));
    putVersion(p, "id");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_NULL_COUNT, "7");

    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L);
    assertNotNull(result);
    ColumnStatistics idStats = result.get(FieldReference.column("id"));
    assertNotNull(idStats, "all-null column must be reported, not dropped");
    assertEquals(7L, idStats.nullCount().getAsLong());
    assertTrue(idStats.distinctCount().isEmpty(), "no distinctCount persisted → empty");
    assertTrue(idStats.min().isEmpty(), "no min persisted → empty");
    assertTrue(idStats.max().isEmpty(), "no max persisted → empty");
  }

  @Test
  @DisplayName("Column name needing SQL quoting is excluded (Spark CBO can't match describe())")
  void quotingNeededColumnNameExcluded() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("ok_col", DataTypes.LongType, true, null),
              new StructField("weird name", DataTypes.LongType, true, null)
            });
    Map<String, String> p = new HashMap<>();
    p.put(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION);
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "42");
    p.put(LanceStatsKeys.SCHEMA_HASH, LanceStatsKeys.computeSchemaHash(schema));
    putVersion(p, "ok_col");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "ok_col" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN, "1");
    putVersion(p, "weird name");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "weird name" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN, "5");

    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, schema, schema, 42L);
    assertNotNull(result);
    assertTrue(
        result.containsKey(FieldReference.column("ok_col")), "plain identifier column is included");
    assertNull(
        result.get(FieldReference.column("weird name")),
        "quoting-needed column must be excluded — Spark's CBO cannot match its describe()");
  }

  @Test
  @DisplayName(
      "Per-column .version newer than this Spark's CatalogColumnStat.VERSION → column dropped")
  void perColumnVersionNewerThanSparkIsDropped() {
    // Simulate a payload written by a newer Spark whose CatalogColumnStat serialization format we
    // don't understand: the per-column version exceeds the connector's CatalogColumnStat.VERSION.
    // The reader skips it rather than risk mis-parsing a reformatted min/max into a wrong CBO
    // bound.
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_VERSION, "999");
    // id is the only analyzed column, so dropping it yields a null result map.
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Poisoned min on a String column surfaces as empty (no NPE — fail-safe)")
  void poisonedStringMinSurfacesAsEmpty() {
    // Spark's fromExternalString returns null (not a throw) for String/Binary, so a hand-edited
    // name.min yields a Some(null) bound internally. The V2 ColumnStatistics must surface it as an
    // empty min rather than NPE at plan time (Spark never computes min/max for string columns).
    Map<String, String> p = new HashMap<>();
    p.put(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION);
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "42");
    p.put(LanceStatsKeys.SCHEMA_HASH, LanceStatsKeys.computeSchemaHash(SCHEMA));
    putVersion(p, "name");
    p.put(
        LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN,
        "hand-edited");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsTestKeys.COLUMN_SUFFIX_NULL_COUNT, "3");

    StructType nameOnly = new StructType(new StructField[] {SCHEMA.fields()[1]});
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, nameOnly, 42L);
    assertNotNull(result);
    ColumnStatistics nameStats = result.get(FieldReference.column("name"));
    assertNotNull(nameStats);
    assertTrue(nameStats.min().isEmpty(), "String min must surface as empty, not throw");
    assertEquals(3L, nameStats.nullCount().getAsLong());
  }

  @Test
  @DisplayName("Oversized stat value is dropped (length cap), not handed to the parser")
  void oversizedStatValueIsDropped() {
    // A poisoned, very long min would otherwise reach Spark's parser (e.g. an O(n^2) BigDecimal
    // construction for a DecimalType). The length cap drops the oversized key, so the column still
    // decodes with its remaining stats and an empty min — fail-safe, no expensive parse.
    Map<String, String> p = validProps();
    StringBuilder huge = new StringBuilder();
    for (int i = 0; i < 300; i++) {
      huge.append('9');
    }
    p.put(
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN,
        huge.toString());

    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L);
    assertNotNull(result);
    ColumnStatistics idStats = result.get(FieldReference.column("id"));
    assertNotNull(idStats);
    assertTrue(idStats.min().isEmpty(), "oversized min must be dropped, leaving an empty min");
    assertEquals(100L, idStats.max().get(), "other stats still decode");
  }

  @Test
  @DisplayName(
      "TimestampNTZ column is skipped on read (its min/max would crash CBO FilterEstimation)")
  void timestampNtzColumnSkippedOnRead() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, true, null),
              new StructField("ts", DataTypes.TimestampNTZType, true, null)
            });
    Map<String, String> p = new HashMap<>();
    p.put(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION);
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "42");
    p.put(LanceStatsKeys.SCHEMA_HASH, LanceStatsKeys.computeSchemaHash(schema));
    putVersion(p, "id");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN, "1");
    // A hand-poisoned ts stat: the read path must skip TimestampNTZ entirely so its min/max never
    // reach Spark's CBO FilterEstimation (no toDouble case → MatchError on a filtered query).
    putVersion(p, "ts");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "ts" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN, "1000000");

    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, schema, schema, 42L);
    assertNotNull(result);
    assertTrue(result.containsKey(FieldReference.column("id")), "id stats present");
    assertNull(
        result.get(FieldReference.column("ts")), "TimestampNTZ column must be skipped on read");
  }
}
