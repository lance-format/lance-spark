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

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Constants and helpers that define the on-disk wire format of {@code ANALYZE TABLE} persisted
 * column statistics in TBLPROPERTIES. Centralizing these in one base-package utility keeps the
 * ANALYZE writer (in {@code org.apache.spark.sql.execution.datasources.v2}) and the scan-time
 * reader (in {@code org.lance.spark.read}) on the same source of truth without forcing the read
 * path to depend on a class inside Spark's internal package namespace.
 *
 * <h2>Wire format (lance.stats.version = 1)</h2>
 *
 * <ul>
 *   <li>{@code lance.stats.version} = "1" — bumping this invalidates older readers.
 *   <li>{@code lance.stats.computedAtVersion} = decimal Lance manifest version stats reflect.
 *   <li>{@code lance.stats.computedAt} = ISO-8601 instant.
 *   <li>{@code lance.stats.numRows} = decimal row count at ANALYZE time.
 *   <li>{@code lance.stats.schemaHash} = SHA-256 hex of the (name, dataType, nullable) tuple
 *       stream.
 *   <li>{@code lance.stats.column.<name>.{version,min,max,nullCount,distinctCount,avgLen,maxLen}} —
 *       per-column stats. The suffix set and string encoding are Spark's own {@code
 *       CatalogColumnStat} map form (written by {@code CatalogColumnStat.toMap}, read by {@code
 *       CatalogColumnStat.fromMap}); {@code <name>} is a top-level column name and the format does
 *       not currently support nested-leaf paths (see {@link #validateColumnName} below). The
 *       per-column {@code .version} is Spark's CatalogColumnStat serialization version, distinct
 *       from the table-level {@link #VERSION}. {@code distinctCount} is HLL-approximate (Spark's
 *       native ANALYZE behavior). No {@code histogram} entry is ever written: the ANALYZE writer
 *       forces {@code spark.sql.statistics.histogram.enabled} off for the aggregation, so {@code
 *       toMap} never produces one (the read path discards histograms anyway — the V2 {@code
 *       ColumnStatistics} histogram type differs from catalyst's).
 * </ul>
 *
 * <p>The format is a stability contract: existing tags must keep their semantics across connector
 * versions. New stats tags may be added at the same {@code lance.stats.version}; format-breaking
 * changes (existing tag's encoding altered, key prefix renamed) require bumping {@code
 * lance.stats.version} so older readers report no column stats rather than silently misinterpret
 * the payload.
 *
 * <h2>Design choices and known gaps vs. upstream conventions</h2>
 *
 * <ul>
 *   <li><b>Storage medium:</b> stats live in TBLPROPERTIES on the Lance manifest, not in a Puffin
 *       sidecar (Iceberg's choice) or a separate stats table (Delta's choice). The Lance manifest
 *       is the single source of truth; sidecar files would require a second read path and add a
 *       Puffin dependency. The trade-off is that very large per-column stats (histograms, sketches)
 *       are awkward — but at v1 we only persist scalar bounds and counts, which fit.
 *   <li><b>Per-column only, not table-level:</b> Spark's native {@code ANALYZE TABLE … COMPUTE
 *       STATISTICS} updates both row-count and on-disk-size. This implementation persists row count
 *       under {@code lance.stats.numRows} for human / tooling inspection only — the read path
 *       always takes BOTH {@code numRows} and {@code sizeInBytes} from {@link
 *       org.lance.ManifestSummary} (live), never from the persisted payload, so {@code
 *       lance.stats.numRows} never feeds the table-size or row-count estimate, regardless of the
 *       FOR clause.
 *   <li><b>{@code avgLen} / {@code maxLen} persisted:</b> Spark's CBO uses these for join size
 *       estimation on string/binary columns. They come for free from {@code
 *       CommandUtils.computeColumnStats} and are surfaced to the CBO via the read path's {@code
 *       ColumnStatistics}.
 *   <li><b>Output row deviates from native ANALYZE TABLE:</b> Spark's {@code AnalyzeTableCommand}
 *       returns zero rows; this command returns one row of {@code (columns_analyzed, rows_scanned,
 *       manifest_version)} for tooling and operator diagnostics.
 * </ul>
 */
public final class LanceStatsKeys {

  /** Top-level table-property keys. */
  public static final String VERSION = "lance.stats.version";

  public static final String COMPUTED_AT_VERSION = "lance.stats.computedAtVersion";
  public static final String COMPUTED_AT = "lance.stats.computedAt";
  public static final String NUM_ROWS = "lance.stats.numRows";
  public static final String SCHEMA_HASH = "lance.stats.schemaHash";

  /**
   * Per-column key prefix; a full key is {@code COLUMN_PREFIX + name + <suffix>}, where {@code
   * <suffix>} (e.g. {@code .min}, {@code .distinctCount}) is whatever Spark's own {@code
   * CatalogColumnStat.toMap} emits — the writer serializes each stat with {@code toMap} and the
   * reader deserializes with {@code CatalogColumnStat.fromMap}, so the per-column payload IS
   * Spark's native string form, prefixed with {@link #COLUMN_PREFIX}. Lance code never enumerates
   * the suffixes (the format is delegated to Spark); only tests assert the concrete key shape, via
   * {@code LanceStatsTestKeys}.
   */
  public static final String COLUMN_PREFIX = "lance.stats.column.";

  /** Format version this connector knows how to read and write. */
  public static final String SUPPORTED_VERSION = "1";

  // SparkConf keys below are part of the operator-facing configuration contract — once a
  // release ships, renaming requires a deprecation cycle. All share the prefix
  // `spark.lance.cbo.column.stats.` for namespace introspection.

  /**
   * Master kill-switch for the column-stats fast path. Overrides per-scan {@code
   * lance.cbo.column.stats.enabled}. Default: {@code true}.
   */
  public static final String SPARK_CONF_CBO_COLUMN_STATS_ENABLED =
      "spark.lance.cbo.column.stats.enabled";

  /**
   * If {@code true}, accept persisted stats whose {@code computedAtVersion} differs from the
   * current Lance manifest version. Default: {@code false}. Trades CBO correctness for fast-path
   * hit rate; only enable when stale estimates are acceptable.
   */
  public static final String SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE =
      "spark.lance.cbo.column.stats.allow.stale";

  private LanceStatsKeys() {}

  /** Maximum length of a column name surfaced in error messages, to bound exception size. */
  private static final int MAX_NAME_IN_ERROR = 256;

  /**
   * True when {@code name} produces a non-ambiguous TBLPROPERTIES key under {@link #COLUMN_PREFIX}.
   * Ambiguity sources today: dots in the name (collide with future nested-leaf paths) and the empty
   * string (would emit a key like {@code lance.stats.column..min} that orphan-cleanup would later
   * misattribute).
   */
  public static boolean isStatsCompatibleColumnName(String name) {
    return name != null && !name.isEmpty() && name.indexOf('.') < 0;
  }

  /**
   * Reject column names that would produce ambiguous TBLPROPERTIES keys: dotted names collide with
   * future nested-leaf paths (and would mis-attribute under the prefix-based orphan-cleanup sweep,
   * which splits the column segment on the first '.'), empty names produce a malformed {@code
   * lance.stats.column..<suffix>} key. Forbidding at write time is more conservative than
   * introducing wire-format escaping.
   *
   * @throws IllegalArgumentException if the name is null, empty, or contains a dot.
   */
  public static void validateColumnName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Column name must not be null");
    }
    if (name.isEmpty()) {
      throw new IllegalArgumentException(
          "ANALYZE TABLE does not support empty column names. Persisted-stats keys would be "
              + "malformed (lance.stats.column..<suffix>).");
    }
    if (!isStatsCompatibleColumnName(name)) {
      // Cap the echoed name in the message to bound exception size — a hostile or accidental
      // 10MB column name should not produce a 10MB error string that flows back to the driver.
      String shown =
          name.length() > MAX_NAME_IN_ERROR ? name.substring(0, MAX_NAME_IN_ERROR) + "…" : name;
      throw new IllegalArgumentException(
          "ANALYZE TABLE does not support column names containing '.': '"
              + shown
              + "'. Persisted-stats keys would be ambiguous with future nested-leaf paths. "
              + "Rename the column or omit it from FOR COLUMNS.");
    }
  }

  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

  /**
   * Per-thread {@link MessageDigest} so {@link #computeSchemaHash} avoids the JCE provider lookup
   * on every call; reset() before reuse keeps it thread-safe.
   */
  private static final ThreadLocal<MessageDigest> SHA256 =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new IllegalStateException("SHA-256 not available", e);
            }
          });

  /**
   * SHA-256 fingerprint of the schema's "field name + data-type + nullable" triples in declared
   * order. Lets the read path detect schema drift between ANALYZE and a subsequent scan: if any
   * column was renamed, retyped, had its nullability flipped, dropped, or reordered in a way that
   * changes resolution, the hash diverges and persisted stats are rejected.
   *
   * <p><b>Schema-mutation monotonicity (intentional):</b> the hash covers ALL fields, so adding,
   * dropping, retyping, reordering, or flipping nullability of any single column invalidates the
   * persisted stats for ALL columns — not just the one that changed. This is the safe direction:
   * the read path reports no column stats, never reports stats it cannot prove still apply.
   * Operators must re-run ANALYZE TABLE after any {@code ALTER TABLE} that changes the schema, even
   * if only one column was touched.
   *
   * <p>Stable across JVM instances because we hash a deterministic UTF-8 byte stream rather than
   * relying on Scala or Java's hashCode (which is salted in some implementations).
   */
  public static String computeSchemaHash(StructType schema) {
    MessageDigest md = SHA256.get();
    md.reset();
    for (StructField f : schema.fields()) {
      md.update(f.name().getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(f.dataType().catalogString().getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(f.nullable() ? (byte) 1 : (byte) 0);
      md.update((byte) 0);
    }
    byte[] digest = md.digest();
    char[] hex = new char[digest.length * 2];
    for (int i = 0; i < digest.length; i++) {
      int b = digest[i] & 0xff;
      hex[i * 2] = HEX_CHARS[b >>> 4];
      hex[i * 2 + 1] = HEX_CHARS[b & 0x0f];
    }
    return new String(hex);
  }

  /**
   * CR/LF/tab-strip and length-cap a user-controllable value before embedding it in a log line.
   * TBLPROPERTIES values and SQL text flow into SLF4J {@code {}} interpolation, which does not
   * strip control characters; capping at 256 chars bounds log-line size. Shared by the codec and
   * the read path so the control-character set lives in one place.
   */
  public static String sanitizeForLog(String value) {
    if (value == null) {
      return null;
    }
    String capped = value.length() > 256 ? value.substring(0, 256) + "…" : value;
    return capped.replace('\r', '_').replace('\n', '_').replace('\t', '_');
  }
}
