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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.connector.read.colstats.{ColumnStatistics, Histogram}
import org.apache.spark.sql.types.DataType
import org.lance.spark.read.LanceStatsKeys

import java.util.{Optional, OptionalLong}

import scala.collection.JavaConverters._

/**
 * Reader-side bridge from persisted `lance.stats.column.<col>.<suffix>` table properties back into a
 * V2 [[ColumnStatistics]], reusing Spark's own [[CatalogColumnStat#fromMap]] + [[CatalogColumnStat#toPlanStat]].
 *
 * <p>The writer ([[LanceAnalyzeTableExec]]) serializes each internal-representation [[ColumnStat]]
 * with [[org.apache.spark.sql.catalyst.plans.logical.ColumnStat#toCatalogColumnStat]] →
 * [[CatalogColumnStat#toMap]], producing Spark's canonical `<col>.{version,distinctCount,min,max,
 * nullCount,avgLen,maxLen}` string keys (prefixed here with `lance.stats.column.`; `toMap` can also
 * emit a `histogram` key, but the writer disables histogram computation so none is produced, and
 * this codec defensively drops any it sees in a legacy/hand-edited payload — see below). This codec
 * strips the prefix, feeds the sub-map to `fromMap`, and `toPlanStat` returns min/max in the
 * internal catalyst representation — exactly what Spark's CBO {@code FilterEstimation} expects and
 * what {@code DataSourceV2Relation.transformV2Stats} copies verbatim into a catalyst {@code ColumnStat}
 * (across Spark 3.4–4.1 that path performs no {@code CatalystTypeConverters} round-trip on the V2
 * min/max, so the internal rep is what reaches the optimizer unchanged).
 *
 * <p>Java-friendly: returns {@link java.util.Optional} so the Java {@code LanceScanBuilder} reader can
 * call it directly. Every decode failure (absent keys, Spark codec rejection, poisoned min/max that
 * {@code fromExternalString} can't parse) is fail-safe → {@code Optional.empty} so the scan simply
 * reports no stats for that column rather than feeding the optimizer a degenerate value.
 */
object LanceColumnStatCodec {

  /**
   * Hard cap on the length of a single per-column stat value accepted from TBLPROPERTIES. Every
   * legitimate value is short (a Decimal(38,x) min/max is ~40 chars; counts / lengths / version are
   * small integers), so this never rejects a real value — it only bounds the cost of parsing a
   * poisoned, oversized min/max (see the BigDecimal note in {@link #decode}).
   */
  private val MAX_STAT_VALUE_LEN = 256

  /**
   * Decode persisted stats for a single column.
   *
   * @param tableName  table identifier, used only in Spark's internal warn logging on bad input
   * @param colName    the (top-level, unquoted, dot-free) column name
   * @param dataType   the column's current Spark type (drives `fromExternalString` parsing)
   * @param props      the full table-properties map (all `lance.stats.*` keys)
   * @return a V2 ColumnStatistics, or empty if no decodable stats exist for the column
   */
  def decode(
      tableName: String,
      colName: String,
      dataType: DataType,
      props: java.util.Map[String, String]): Optional[ColumnStatistics] = {
    if (props == null || props.isEmpty) {
      return Optional.empty()
    }
    val keyPrefix = LanceStatsKeys.COLUMN_PREFIX + colName + "."
    val histogramPrefix = keyPrefix + "histogram"
    val envelopeLen = LanceStatsKeys.COLUMN_PREFIX.length
    // Strip the `lance.stats.column.` envelope so keys read as `<col>.<suffix>` — the exact shape
    // CatalogColumnStat.fromMap consumes. The trailing '.' in keyPrefix prevents a column whose
    // name is a prefix of another (e.g. "id" vs "id2") from capturing the sibling's keys.
    //
    // Drop any `<col>.histogram[.partN]` key: the writer no longer persists histograms, and we
    // discard them on read anyway. Excluding them here also closes a DoS vector — fromMap eagerly
    // calls HistogramSerializer.deserialize, which allocates arrays sized by an int read from the
    // (user-editable) payload before any bin data, so a poisoned/legacy histogram value could force
    // a multi-GB driver allocation at planning time.
    //
    // Also drop any value longer than MAX_STAT_VALUE_LEN. Every legitimate per-column value here is
    // short (a Decimal(38,x) min/max is ~40 chars; counts/lengths/version are small integers), so
    // this is a no-op in practice. It bounds a second poisoned-payload cost: toPlanStat parses a
    // DecimalType min/max via `new BigDecimal(String)`, whose construction is O(n^2) in digit count
    // and runs to completion before any validation — a multi-MB digit string would stall the driver
    // at plan time, re-incurred on every scan. Dropping the oversized key degrades to "no stats" for
    // that bound (fail-safe), consistent with the rest of this codec.
    val subMap: Map[String, String] = props.asScala.iterator.collect {
      case (k, v)
          if k != null && v != null && k.startsWith(keyPrefix) &&
            !k.startsWith(histogramPrefix) &&
            v.length <= LanceColumnStatCodec.MAX_STAT_VALUE_LEN =>
        k.substring(envelopeLen) -> v
    }.toMap
    if (subMap.isEmpty) {
      return Optional.empty()
    }
    val catalogStat: Option[CatalogColumnStat] =
      try {
        CatalogColumnStat.fromMap(tableName, colName, subMap)
      } catch {
        // fromMap already swallows NonFatal internally and returns None, but guard defensively so
        // a future Spark that throws here still degrades to "no stats" rather than failing the scan.
        case _: Throwable => None
      }
    catalogStat match {
      case Some(ccs) =>
        // Forward-compat guard: the per-column `<col>.version` is Spark's own CatalogColumnStat
        // serialization version, NOT gated by the envelope `lance.stats.version`. If a payload was
        // written by a newer Spark whose CatalogColumnStat format we don't understand, skip it
        // rather than risk toPlanStat silently mis-parsing a reformatted min/max into a
        // plausible-but-wrong bound that would mislead the CBO. (Cross-version reads only; within a
        // deployment the Spark version is fixed, so this never fires.)
        if (ccs.version > CatalogColumnStat.VERSION) {
          return Optional.empty()
        }
        val stat: ColumnStat =
          try {
            ccs.toPlanStat(colName, dataType)
          } catch {
            // toPlanStat calls fromExternalString, which throws on a poisoned min/max that doesn't
            // parse for the column's type. Skip the column rather than surface a planning failure.
            case _: Throwable => return Optional.empty()
          }
        // CBO contract: a negative nullCount or distinctCount is corrupt — skip the whole column.
        // distinctCount == 0 (e.g. an all-null column whose NDV is 0) is legitimate: the wrapper
        // drops it to empty but still surfaces the column for its nullCount, which feeds
        // null-fraction selectivity.
        if (stat.nullCount.exists(_ < 0) || stat.distinctCount.exists(_ < 0)) {
          return Optional.empty()
        }
        Optional.of(new LanceV2ColumnStatistics(stat))
      case None => Optional.empty()
    }
  }
}

/**
 * Trivial [[ColumnStatistics]] view over a catalyst [[ColumnStat]]. min/max are surfaced in the
 * internal catalyst representation (e.g. UTF8String, Decimal, days-as-Int, micros-as-Long) that
 * {@code transformV2Stats} copies straight into a catalyst {@code ColumnStat}. distinctCount is
 * suppressed when non-positive (CBO requires > 0). Histogram is intentionally dropped — the catalyst
 * {@code Histogram} type differs from the V2 {@code Histogram}, and uniform-within-range selectivity
 * is an acceptable fallback.
 */
final class LanceV2ColumnStatistics(stat: ColumnStat)
  extends ColumnStatistics
  with java.io.Serializable {

  override def distinctCount(): OptionalLong =
    stat.distinctCount.filter(_ > 0).map(b => OptionalLong.of(b.toLong)).getOrElse(
      OptionalLong.empty())

  // Optional.ofNullable, not Optional.of: fromExternalString returns null (not a throw) for
  // String/Binary, so a poisoned `<col>.min`/`.max` on such a column yields Some(null) here; of()
  // would NPE at plan time, breaking the fail-safe contract. ofNullable surfaces it as empty.
  override def min(): Optional[Object] =
    Optional.ofNullable(stat.min.map(_.asInstanceOf[Object]).orNull)

  override def max(): Optional[Object] =
    Optional.ofNullable(stat.max.map(_.asInstanceOf[Object]).orNull)

  override def nullCount(): OptionalLong =
    stat.nullCount.map(b => OptionalLong.of(b.toLong)).getOrElse(OptionalLong.empty())

  override def avgLen(): OptionalLong =
    stat.avgLen.map(l => OptionalLong.of(l)).getOrElse(OptionalLong.empty())

  override def maxLen(): OptionalLong =
    stat.maxLen.map(l => OptionalLong.of(l)).getOrElse(OptionalLong.empty())

  override def histogram(): Optional[Histogram] = Optional.empty()
}
