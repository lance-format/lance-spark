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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.LanceAnalyzeTableOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.types.{AnsiIntervalType, ArrayType, CalendarIntervalType, DataType, MapType, NullType, StructField, StructType, UserDefinedType}
import org.lance.spark.LanceDataset
import org.lance.spark.read.LanceStatsKeys

import scala.collection.JavaConverters._

/**
 * Executor for `ANALYZE TABLE`. Delegates the per-column computation to Spark's own
 * {@code CommandUtils.computeColumnStats} (via {@link LanceNativeColumnStats}), which runs a single
 * aggregation job producing min / max / nullCount / HLL-approximate distinctCount / avgLen / maxLen
 * as internal-representation {@code ColumnStat}s. Each is serialized with Spark's {@code
 * CatalogColumnStat.toMap} (minus the histogram — see the persist loop) and written to the Lance
 * table's properties under {@code lance.stats.column.<name>.<suffix>}, plus the table-level
 * {@code lance.stats.computedAtVersion} = Lance manifest version. {@link LanceScanBuilder} reads these
 * back via {@code CatalogColumnStat.fromMap} / {@code toPlanStat} and surfaces them to Spark's CBO
 * directly — no per-scan re-aggregation.
 *
 * <p>NDV is always HLL-approximate (Spark's native ANALYZE behavior). This exec is produced by
 * {@link LanceAnalyzeTableResolution}, which rewrites Spark's native ANALYZE plans for Lance tables;
 * there is no custom ANALYZE grammar.
 */
case class LanceAnalyzeTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    columns: Seq[String],
    forAllColumns: Boolean) extends LeafV2CommandExec with Logging {

  override def output: Seq[Attribute] = LanceAnalyzeTableOutputType.SCHEMA

  /**
   * Returns true when Spark's column-stat computation supports this type and the resulting stat is
   * safe for the CBO. Complex containers (StructType, ArrayType, MapType, UserDefinedType, NullType)
   * and interval types (CalendarIntervalType, AnsiIntervalType — the parent of YearMonthIntervalType
   * / DayTimeIntervalType) are not analyzable by Spark's `computeColumnStats` and are skipped up
   * front with a log line rather than allowed to fail mid-job. TimestampNTZType is also excluded
   * (the final `case` below) for a different reason: its min/max would crash Spark's CBO
   * FilterEstimation, not because the stat can't be computed.
   */
  private def isStatSupported(dt: DataType): Boolean = dt match {
    case _: StructType | _: ArrayType | _: MapType | _: UserDefinedType[_] | _: NullType => false
    case _: CalendarIntervalType | _: AnsiIntervalType => false
    // TimestampNTZ min/max would reach Spark's CBO FilterEstimation.toDouble, which (through at
    // least Spark 4.x) has no TimestampNTZType case and throws a scala.MatchError at query-planning
    // time whenever a filter references the column. Skip it so ANALYZE never turns a previously
    // working query into a hard planning failure under spark.sql.cbo.enabled=true.
    case _ if dt == org.apache.spark.sql.types.DataTypes.TimestampNTZType => false
    case _ => true
  }

  override protected def run(): Seq[InternalRow] = {
    // Sanitized table name for all subsequent error / log lines (CR/LF stripped, length capped).
    val tableName = LanceAnalyzeTableExec.sanitizeForLog(ident.toString)

    // The persisted-stats payload requires single-manifest atomicity: BaseLanceNamespace-
    // SparkCatalog issues one Dataset.updateConfig per alterTable call. Other catalogs may
    // batch the change list across multiple commits, leaving a half-written stats set visible to
    // readers between commits — reject up front.
    if (!catalog.isInstanceOf[org.lance.spark.BaseLanceNamespaceSparkCatalog]) {
      throw new UnsupportedOperationException(
        s"ANALYZE TABLE $tableName: the Lance SQL extension intercepts ANALYZE TABLE and supports " +
          "only Lance tables (this table is on " + catalog.getClass.getName + ", not a " +
          "BaseLanceNamespaceSparkCatalog). The persisted-stats wire format requires the catalog " +
          "to commit the change list as a single atomic manifest write, which other catalogs do " +
          "not guarantee. To run Spark's native ANALYZE TABLE on a non-Lance table, use a session " +
          "without the Lance SQL extension enabled.")
    }
    val lanceDataset = catalog.loadTable(ident) match {
      case ds: LanceDataset => ds
      case other =>
        throw new UnsupportedOperationException(
          s"ANALYZE TABLE $tableName: only LanceDataset tables are supported; got " +
            other.getClass.getName)
    }

    val schema: StructType = lanceDataset.schema()
    // Reject duplicate field names BEFORE type/name-shape filters: persisted-stats keys would
    // collide regardless of column type, so this is the most fundamental constraint and should
    // surface first.
    val duplicateNames = schema.fields.groupBy(_.name).collect {
      case (name, fields) if fields.length > 1 => name
    }
    if (duplicateNames.nonEmpty) {
      throw new IllegalArgumentException(
        s"ANALYZE TABLE $tableName: schema has duplicate field names: " +
          duplicateNames.toSeq.sorted.map(LanceAnalyzeTableExec.sanitizeForLog).mkString(", ") +
          ". Persisted-stats keys would collide; rename the conflicting columns first.")
    }
    val requestedCols: Seq[StructField] =
      if (forAllColumns) schema.fields.toSeq
      else {
        // Resolve FOR COLUMNS names honoring spark.sql.caseSensitive (default false) to match
        // Spark's native column resolution; map to the canonical StructField so the persisted key
        // uses the table's stored column case.
        val caseSensitive = org.apache.spark.sql.internal.SQLConf.get.caseSensitiveAnalysis
        val byName =
          if (caseSensitive) schema.fields.map(f => f.name -> f).toMap
          else schema.fields.map(f => f.name.toLowerCase(java.util.Locale.ROOT) -> f).toMap
        // distinct: a duplicate column in the FOR COLUMNS list would otherwise be aggregated twice
        // (wasted compute).
        columns.distinct.map { c =>
          val key = if (caseSensitive) c else c.toLowerCase(java.util.Locale.ROOT)
          byName.getOrElse(
            key,
            throw new IllegalArgumentException(
              s"ANALYZE TABLE $tableName: column not found: " +
                LanceAnalyzeTableExec.sanitizeForLog(c)))
        }
      }
    // Filter complex types (Struct/Array/Map/UDT/Null) and interval types: df.agg(min(col))
    // raises AnalysisException at planning time on these. FOR ALL COLUMNS skips silently to
    // avoid crashing ANALYZE on tables that happen to contain one. FOR COLUMNS fails fast.
    val (typeSupported, complexSkipped) = requestedCols.partition(f => isStatSupported(f.dataType))
    if (complexSkipped.nonEmpty) {
      // Sanitize per-name BEFORE mkString so the per-name length cap is honored and a hostile
      // name can't inject CR/LF into either log lines or the driver query-error response.
      val sanitizedSkippedNames =
        complexSkipped.map(f => LanceAnalyzeTableExec.sanitizeForLog(f.name)).mkString(", ")
      if (forAllColumns) {
        log.info(
          s"ANALYZE TABLE $tableName skipping unsupported-type columns: $sanitizedSkippedNames")
      } else {
        throw new IllegalArgumentException(
          s"ANALYZE TABLE $tableName FOR COLUMNS: unsupported column type(s). Stats are not " +
            "collected for complex types (StructType / ArrayType / MapType / UserDefinedType / " +
            "NullType), interval types (CalendarIntervalType / AnsiIntervalType), or " +
            "TimestampNTZType (its min/max would crash Spark's CBO FilterEstimation). Unsupported " +
            "columns: " + sanitizedSkippedNames)
      }
    }

    // Reject names that would produce ambiguous stats keys: empty (yields a malformed
    // `lance.stats.column..<suffix>` key) or containing '.' (collides with future nested-leaf paths
    // and mis-attributes under the prefix-based orphan-cleanup sweep). FOR-ALL skips and logs
    // (separately for empty vs dotted, so operators see the actual cause); FOR-COLUMNS delegates to
    // validateColumnName for a fail-fast exception with the canonical message.
    val (targetCols, nameSkipped) =
      typeSupported.partition(f => LanceStatsKeys.isStatsCompatibleColumnName(f.name))
    if (nameSkipped.nonEmpty) {
      if (forAllColumns) {
        val (emptyNames, dottedNames) = nameSkipped.partition(_.name.isEmpty)
        if (emptyNames.nonEmpty) {
          log.info(s"ANALYZE TABLE skipping ${emptyNames.size} column(s) with empty names " +
            "(persisted-stats keys would be malformed: lance.stats.column..<suffix>).")
        }
        if (dottedNames.nonEmpty) {
          // Per-name sanitize before join (see complexSkipped path above for rationale).
          val sanitizedNames =
            dottedNames.map(f => LanceAnalyzeTableExec.sanitizeForLog(f.name)).mkString(", ")
          log.info("ANALYZE TABLE skipping columns whose names contain '.' (key ambiguity " +
            "with nested-leaf paths): " + sanitizedNames)
        }
      } else {
        // FOR COLUMNS must fail fast. Use the first bad name as the exception's example;
        // validateColumnName picks the right message for empty vs dotted.
        LanceStatsKeys.validateColumnName(nameSkipped.head.name)
      }
    }

    // SparkSession.active reads a ThreadLocal; under AQE re-plan the calling thread may have
    // no session bound. Wrap the generic IllegalStateException with a command-specific message.
    val spark =
      try {
        SparkSession.active
      } catch {
        case _: IllegalStateException =>
          throw new IllegalStateException(
            "ANALYZE TABLE requires an active SparkSession on the calling thread; ensure the " +
              "command runs from the driver query thread (AQE re-plan threads may not have a " +
              "session bound).")
      }
    // If every requested column is unsupported (e.g., a struct-only table under FOR ALL COLUMNS),
    // there is nothing to analyze. Return an empty result rather than write a stats payload of
    // just version / numRows / hash with no per-column entries. This branch issues no alterTable at
    // all, so any pre-existing valid stats are preserved untouched.
    //
    // Output-row contract for this branch: columns_analyzed=0, rows_scanned=0, manifest_version=0.
    // The manifest_version=0 here is a sentinel meaning "no write occurred"; the table's actual
    // manifest version is unchanged. Tooling that reads the command output should use
    // columns_analyzed==0 as the signal that no stats were persisted, not the manifest_version value.
    if (targetCols.isEmpty) {
      log.info(
        s"ANALYZE TABLE $tableName: no analyzable columns " +
          s"(${requestedCols.size} requested, all unsupported types) — skipping write.")
      return Seq(new GenericInternalRow(Array[Any](
        0.asInstanceOf[java.lang.Integer],
        0L.asInstanceOf[java.lang.Long],
        0L.asInstanceOf[java.lang.Long])))
    }

    // Pass the per-table storage options (cloud credentials / endpoint config the namespace
    // catalog injects into readOptions) to the aggregation read. Without them, ANALYZE fails to
    // authenticate against cloud-hosted tables. On local-FS tables this map is empty (no-op). The
    // native dataset opened below for the version capture already receives them via readOptions.
    val df = spark.read
      .format("lance")
      .options(lanceDataset.readOptions().getStorageOptions())
      .load(lanceDataset.readOptions().getDatasetUri)

    // Capture the manifest version BEFORE the aggregation read. A concurrent writer between
    // this line and alterTable means the stats reflect a version the table never directly
    // held; the read path's exact-equality check then rejects them and falls back to live
    // aggregation. Safer-stale-than-wrong.
    val readVersion = {
      val ds = org.lance.spark.utils.Utils.openDatasetBuilder(lanceDataset.readOptions()).build()
      try {
        ds.getVersion.getId
      } finally {
        ds.close()
      }
    }

    // Reuse Spark's own column-stat computation (single job): min / max / nullCount /
    // HLL-approximate distinctCount / avgLen / maxLen (+ histogram when
    // spark.sql.statistics.histogram.enabled), in the internal catalyst representation that
    // CatalogColumnStat.toMap serializes and the CBO consumes. NDV is always HLL-approximate
    // (Spark's native ANALYZE behavior).
    val analyzed = df.queryExecution.analyzed
    val targetColNames: Set[String] = targetCols.map(_.name).toSet
    val targetAttrs = analyzed.output.filter(a => targetColNames.contains(a.name))
    val (rowCount, columnStats) =
      LanceNativeColumnStats.computeColumnStats(spark, analyzed, targetAttrs)

    // Tag stats with the expected post-write manifest version (readVersion + 1, since Lance
    // increments by 1 per write). If a concurrent writer interleaves, the actual version will
    // be readVersion + N (N >= 2) and the read path's exact-equality check rejects these stats
    // as stale — reporting no column stats rather than feeding inconsistent values to CBO.
    // Safer-stale-than-wrong.
    val manifestVersion = readVersion + 1

    // Build the change array. Step 1: GC orphan keys for dropped/omitted columns. Step 2:
    // table-level tags (version / computedAtVersion / numRows / schemaHash). Step 3: fresh
    // per-column stats. The whole array is committed as one atomic manifest write (see the
    // alterTable comment below), so there is no partial-write window to guard against — the
    // payload's own version / computedAtVersion / schemaHash tags are what the read path validates.
    val changes = scala.collection.mutable.ArrayBuffer[TableChange]()

    // Step 1: GC orphan column stats. A partial ANALYZE (FOR COLUMNS subset) REPLACES the persisted
    // stat set: remove every `lance.stats.column.<col>.*` key for a column NOT analyzed in this run
    // (dropped from the schema, or simply omitted from FOR COLUMNS). This keeps the single
    // table-level computedAtVersion an accurate freshness tag for EVERY surviving column — without
    // it, a column analyzed earlier whose data later changed would retain stats the reader wrongly
    // accepts as fresh once this run re-advances computedAtVersion to the current manifest version.
    //
    // The sweep is prefix-based (the column-name segment up to the first '.'), NOT a fixed suffix
    // list: the per-column payload format is delegated to Spark's CatalogColumnStat, so a key shape
    // we don't enumerate — a split-large-prop histogram's `.histogram.part.N`, or any suffix a
    // future Spark adds — is still swept. Column names are dot-free (validateColumnName), so the
    // first '.' after the prefix always terminates the column segment.
    val columnPrefixLen = LanceStatsKeys.COLUMN_PREFIX.length
    // Reuse the loadTable snapshot already in scope; avoids a redundant catalog round-trip.
    val existingProps: Map[String, String] =
      try {
        Option(lanceDataset.properties()).map(_.asScala.toMap).getOrElse(Map.empty)
      } catch {
        case _: Exception => Map.empty[String, String]
      }
    val staleKeys = existingProps.keys.filter { k =>
      k.startsWith(LanceStatsKeys.COLUMN_PREFIX) && {
        val rest = k.substring(columnPrefixLen)
        val dot = rest.indexOf('.')
        val colName = if (dot < 0) rest else rest.substring(0, dot)
        !targetColNames.contains(colName)
      }
    }.toSeq
    staleKeys.foreach(k => changes += TableChange.removeProperty(k))
    if (staleKeys.nonEmpty) {
      log.info(
        s"ANALYZE TABLE $tableName: removing ${staleKeys.size} stale column-stats key(s) for " +
          s"columns not analyzed in this run")
    }

    // Step 2: write table-level tags for this run.
    changes += TableChange.setProperty(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION)
    changes += TableChange.setProperty(
      LanceStatsKeys.COMPUTED_AT_VERSION,
      manifestVersion.toString)
    // numRows captures the count at ANALYZE time; the read path uses ManifestSummary's count
    // (always live) for sizeInBytes/numRows, but we persist it here for human inspection and
    // for future use cases that may want a "count at last ANALYZE" reference.
    changes += TableChange.setProperty(LanceStatsKeys.NUM_ROWS, rowCount.toString)
    val computedAt = java.time.Instant.now().toString
    changes += TableChange.setProperty(LanceStatsKeys.COMPUTED_AT, computedAt)

    // Schema fingerprint guards against schema drift (column rename or type change between
    // ANALYZE and read). Read path verifies hash equality before trusting any persisted stat.
    val schemaHash = LanceStatsKeys.computeSchemaHash(schema)
    changes += TableChange.setProperty(LanceStatsKeys.SCHEMA_HASH, schemaHash)

    // Step 3: per-column stats via Spark's CatalogColumnStat codec: serialize each ColumnStat
    // (internal catalyst rep) to Spark's `<col>.{version,distinctCount,min,max,nullCount,avgLen,maxLen}`
    // string map, under our column prefix. Re-ANALYZE correctness: write exactly the keys this run
    // produced and REMOVE any prior key for the column this run did NOT produce (e.g. min/max gone
    // after the column became all-NULL), so the reader can't serve a stale value under the fresh
    // manifest version. (Columns not analyzed at all are cleared by staleKeys.)
    //
    // No histogram appears in the map: LanceNativeColumnStats forces
    // spark.sql.statistics.histogram.enabled off for the aggregation, so toMap never emits a
    // `<col>.histogram[.partN]` payload (the read path discards histograms anyway — the V2
    // ColumnStatistics histogram type differs from catalyst's). The per-column removal below still
    // clears any histogram key a prior run, written before this change, may have left behind.
    val byName: Map[String, org.apache.spark.sql.catalyst.plans.logical.ColumnStat] =
      columnStats.map { case (a, s) => a.name -> s }
    targetCols.foreach { f =>
      val keyPrefix = LanceStatsKeys.COLUMN_PREFIX + f.name + "."
      val produced: Map[String, String] =
        byName
          .get(f.name)
          .map { stat =>
            try {
              stat
                .toCatalogColumnStat(f.name, f.dataType)
                .toMap(f.name)
                .map { case (k, v) => (LanceStatsKeys.COLUMN_PREFIX + k) -> v }
            } catch {
              // A type Spark's codec can't externalize: skip this column's stats rather than fail
              // ANALYZE (graceful degradation, matching the old null-skip behavior).
              case _: Exception => Map.empty[String, String]
            }
          }
          .getOrElse(Map.empty[String, String])
      produced.foreach { case (k, v) => changes += TableChange.setProperty(k, v) }
      existingProps.keys
        .filter(k => k.startsWith(keyPrefix) && !produced.contains(k))
        .foreach(k => changes += TableChange.removeProperty(k))
    }

    // Atomicity: BaseLanceNamespaceSparkCatalog merges the entire `changes` array into one
    // property map and issues a single Dataset.updateConfig — the manifest commits atomically, so a
    // reader never observes a half-written stats set. The catalog instanceof check above enforces
    // this single-commit contract; because the write is atomic there is no separate completeness
    // sentinel to maintain — the payload's version / computedAtVersion / schemaHash tags fully gate
    // validity on the read side.
    catalog.alterTable(ident, changes.toArray: _*)
    // Include computedAt + numRows so cron operators can grep logs to answer "when did this table's
    // stats last refresh?".
    log.info(
      s"ANALYZE TABLE $tableName: stats persisted for ${targetCols.size} columns at " +
        s"manifest version $manifestVersion (rows_scanned=$rowCount, " +
        s"computedAt=$computedAt, schemaHash=${schemaHash.take(8)}…)")

    Seq(new GenericInternalRow(Array[Any](
      targetCols.size.asInstanceOf[java.lang.Integer],
      rowCount.asInstanceOf[java.lang.Long],
      manifestVersion.asInstanceOf[java.lang.Long])))
  }
}

object LanceAnalyzeTableExec {

  /**
   * Strip CR/LF/tab from a value before interpolating into a log message or exception message, and
   * bound length. Delegates to {@link LanceStatsKeys#sanitizeForLog} so the writer-side (Scala),
   * codec, and reader-side (Java) paths share one canonical control-character/cap definition. Used
   * for any user-controllable input flowing into a Scala s-string (table identifier, column name).
   */
  private[v2] def sanitizeForLog(value: String): String =
    LanceStatsKeys.sanitizeForLog(value)
}
