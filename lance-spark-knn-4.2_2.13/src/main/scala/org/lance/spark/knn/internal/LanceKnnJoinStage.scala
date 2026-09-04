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
package org.lance.spark.knn.internal

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.lance.Dataset
import org.lance.spark.{LanceRef, LanceSparkReadOptions}
import org.lance.spark.utils.Utils

/**
 * The whole indexed nearest-by join, done per Spark partition with NO shuffle.
 *
 * A single native `LanceProbe.probe(...)` call is already a complete distributed search: Lance
 * probes the IVF index and scans the candidate fragments across its own threads, heap-merges in
 * process, and returns the final top-K for one query. Handing that orchestration to Spark (a
 * probe → shuffle → merge → materialize pipeline) only adds a shuffle round-trip, a redundant
 * merge stage, a second materialize scan, and `M × N_frag × K` refs crossing the Rust→JVM
 * boundary. So this stage keeps everything local:
 *
 * {{{
 *   left.rdd.mapPartitions { rows =>
 *     val probe = new LanceProbe(uri, fragmentIds = None, version)   // whole-index, once per task
 *     rows.flatMap { leftRow =>
 *       // No over-fetch (SQL path, internalK == k): search + project payload in ONE scan.
 *       val hits = probe.probeRows(query(leftRow), k, projection, ...)
 *       hits.map(hit => assembleRow(leftRow, hit.row, hit.score))
 *       // Over-fetch path (internalK > k): search cheap refs → trim → materialize survivors.
 *       //   val topK = trimToK(probe.probe(query, internalK, ...))
 *       //   val payloads = probe.materialize(topK.map(_.rowAddr))
 *       //   topK.map(ref => assembleRow(leftRow, payloads(ref), ref.score))
 *     }
 *   }
 * }}}
 *
 * No `requiredChildDistribution`, no Exchange. Each task opens R's whole index (`fragmentIds =
 * None`) — Lance does the cross-fragment merge internally — so per-executor resident memory grows
 * with `|R|`.
 *
 * The SQL Catalyst node ([[org.lance.spark.knn.catalyst.LanceKnnJoinExec]]) drives this
 * `runPartition`, so probe/trim/materialize semantics stay defined in exactly one place.
 */
object LanceKnnJoinStage {

  /**
   * Everything a probe task needs, shipped from the driver. `internalK` is the overfetch count
   * handed to Lance (`k × overfetch`); `k` is the final per-left-row cut applied after the native
   * search. `leftVecIdx` is the position of the query-vector column in the left row.
   *
   * The read context — `readOptions`, `relationOptions`, `initialStorageOptions`, `namespaceImpl`,
   * `namespaceProperties` — is the full Lance scan context captured from the connector's
   * `LanceDataset` plus the DataSourceV2 relation options (where a DataFrame read carries branch /
   * version / storage credentials). [[resolveReadContext]] merges + pins these once on the driver
   * before the probe RDD launches; the resolved `readOptions` carry the pinned ref so every task
   * probes one consistent snapshot. All five fields are serializable so they ship to executors.
   */
  final case class Conf(
      readOptions: LanceSparkReadOptions,
      relationOptions: java.util.Map[String, String],
      initialStorageOptions: java.util.Map[String, String],
      namespaceImpl: String,
      namespaceProperties: java.util.Map[String, String],
      vectorColumn: String,
      metric: Metric,
      k: Int,
      internalK: Int,
      nprobes: Option[Int],
      refineFactor: Option[Int],
      ef: Option[Int],
      prefilter: Option[String],
      leftVecIdx: Int,
      rightProjection: Seq[String],
      rightFields: Seq[StructField],
      leftFieldCount: Int,
      outerJoin: Boolean,
      smallerIsBetter: Boolean)
    extends Serializable

  /**
   * Resolve and pin the Lance read context ONCE, on the driver, before the probe RDD is launched —
   * the same thing the connector's `LanceScanBuilder` does at scan-build time. Merges the
   * DataSourceV2 relation options over the base table read options (branch / version / storage
   * credentials), opens the dataset to read its current version, and pins that version so every
   * executor probe sees one consistent snapshot even under concurrent writes.
   *
   * Returns a [[Conf]] whose `readOptions` carry the pinned ref; `relationOptions` is cleared since
   * it has been folded in. Call this from the physical operator's `doExecute` (driver side) — never
   * from the Catalyst rule, which must stay I/O-free so it can pattern-match against fake-URI
   * relations in unit tests.
   */
  def resolveReadContext(conf: Conf): Conf = {
    val merged = mergeReadOptions(conf.readOptions, conf.relationOptions)
    val builder = Utils.openDatasetBuilder(merged)
    if (conf.initialStorageOptions != null) {
      builder.initialStorageOptions(conf.initialStorageOptions)
    }
    builder.runtimeNamespace(conf.namespaceImpl, conf.namespaceProperties, merged.getTableId())
    val dataset: Dataset = builder.build()
    val pinned =
      try merged.withRef(Utils.pinOpenedRef(dataset, merged.getRef()))
      finally dataset.close()
    conf.copy(readOptions = pinned, relationOptions = new java.util.HashMap[String, String]())
  }

  /**
   * Port of the connector's `LanceDataset.mergeScanOptions`: overlay the DataSourceV2 relation
   * options on the base table's read options. Storage options merge with the relation winning, and
   * any stale `version` / `branch` keys are stripped from the base first so the relation's ref is
   * not shadowed by a leftover storage entry. An empty relation returns the base options untouched.
   *
   * Preserves the connector's incompatible-ref guard: if the base options already carry a pinned
   * `ref` (e.g. a catalog table time-travelled at load) and the relation ALSO sets `version` /
   * `branch`, a same-named branch keeps the table ref while an incompatible combination is rejected
   * with an `IllegalArgumentException` rather than silently letting the relation value win. On the
   * plain DataFrame-relation path this rule usually matches, the base carries no ref and the guard
   * is inert — but keeping it makes the merge behave identically to `LanceDataset.mergeScanOptions`
   * for a pinned base, so a caller cannot smuggle a conflicting snapshot past the merge.
   */
  private[knn] def mergeReadOptions(
      base: LanceSparkReadOptions,
      relationOptions: java.util.Map[String, String]): LanceSparkReadOptions = {
    if (relationOptions == null || relationOptions.isEmpty) {
      return base
    }
    val tableRef = base.getRef
    val scanSetsBranchOrVersion =
      relationOptions.containsKey(LanceSparkReadOptions.CONFIG_VERSION) ||
        relationOptions.containsKey(LanceSparkReadOptions.CONFIG_BRANCH)
    val merged = new java.util.HashMap[String, String](base.getStorageOptions)
    merged.remove(LanceSparkReadOptions.CONFIG_VERSION)
    merged.remove(LanceSparkReadOptions.CONFIG_BRANCH)
    merged.putAll(relationOptions)
    val scanOptions = LanceSparkReadOptions
      .builder()
      .datasetUri(base.getDatasetUri)
      .namespace(base.getNamespace)
      .tableId(base.getTableId)
      .catalogName(base.getCatalogName)
      .indexCacheBackend(base.getIndexCacheBackend)
      .metadataCacheBackend(base.getMetadataCacheBackend)
      .ref(tableRef)
      .fromOptions(merged)
      .build()
    if (tableRef != null && scanSetsBranchOrVersion) {
      val scanRef = scanOptions.getRef
      if (sameNamedBranch(tableRef, scanRef)) {
        // Same named branch, different version → keep the table's pinned ref (snapshot isolation).
        return scanOptions.withRef(tableRef)
      }
      require(
        tableRef == scanRef,
        s"Cannot combine $tableRef with $scanRef")
    }
    scanOptions
  }

  /** Same-named-branch check, mirroring the connector's `LanceDataset.sameNamedBranch`. */
  private def sameNamedBranch(tableRef: LanceRef, scanRef: LanceRef): Boolean =
    tableRef.isBranch &&
      scanRef != null &&
      scanRef.isBranch &&
      tableRef.getBranchName.equals(scanRef.getBranchName)

  /**
   * Run the join for one partition of left rows. Opens the probe once, then streams the output: each
   * left row expands to its (≤k) join rows on demand via [[lazyJoinIterator]], so the whole
   * partition is never buffered.
   *
   * Because Spark pulls from `mapPartitions` lazily, the returned iterator can outlive this method —
   * so the probe is closed on task completion (success OR failure) via the `TaskContext` listener,
   * NOT a `try`/`finally` here (which would release the native handle before the consumer reads it).
   * When there is no `TaskContext` (a direct call outside a Spark task, e.g. a JVM-only test), we
   * fall back to draining eagerly and closing before returning so the handle cannot leak.
   */
  def runPartition(leftRows: Iterator[Row], conf: Conf): Iterator[Row] = {
    if (leftRows.isEmpty) return Iterator.empty

    val probe = new LanceProbe(
      conf.readOptions,
      conf.initialStorageOptions,
      conf.namespaceImpl,
      conf.namespaceProperties,
      fragmentIds = None)

    val output = lazyJoinIterator(leftRows, leftRow => processRow(leftRow, probe, conf))
    TaskContext.get() match {
      case null =>
        try output.toList.iterator
        finally probe.close()
      case tc =>
        tc.addTaskCompletionListener[Unit](_ => probe.close())
        output
    }
  }

  /**
   * Lazily compose per-partition output: each left row expands to its (≤k) join rows on demand.
   * Extracted so a unit test can assert laziness — the left iterator is pulled element-by-element,
   * not drained up front — with a stub expander and no Lance dataset. [[runPartition]] wires the
   * real per-row probe / trim / materialize expansion through here.
   */
  private[knn] def lazyJoinIterator(
      leftRows: Iterator[Row],
      expand: Row => Iterator[Row]): Iterator[Row] =
    leftRows.flatMap(expand)

  /**
   * Whether [[processRow]] may take the folded one-scan fast path. True when there is no JVM-side
   * over-fetch (`internalK <= k`, so every probed row is kept and no trim is needed), so probe and
   * materialize collapse into a single native scan; false when the caller over-fetches candidates
   * (`internalK > k`) and must trim before paying to materialize only the survivors.
   *
   * This decision is ONLY about over-fetch. Reserved-column collisions (a right schema owning
   * `_rowid` / `_distance` / `_score`) are handled upstream: the Catalyst rule declines the indexed
   * rewrite for such a table (see [[LanceProbe.schemaSupportsNearest]]) and [[LanceProbe]] enforces
   * the same contract defensively — so by the time a row reaches here the projection is always
   * fusible. Extracted so the routing decision is unit-testable without a Lance dataset.
   */
  private[knn] def foldsInOneScan(internalK: Int, k: Int): Boolean =
    internalK <= k

  /**
   * Expand one left row into its join output rows: probe R's index, trim to `k`, late-materialize
   * the surviving right rows by `_rowid`, and assemble `left ++ right ++ score`. Returns an empty
   * iterator (inner join) or a single null-right row (outer join) when there is no query vector or
   * no hit.
   */
  private def processRow(leftRow: Row, probe: LanceProbe, conf: Conf): Iterator[Row] = {
    val q = extractVector(leftRow, conf.leftVecIdx)
    if (q == null) {
      // Null query vector: nothing to search. Emit a null-right row only for an outer join.
      if (conf.outerJoin) {
        Iterator.single(assembleRow(leftRow, conf.leftFieldCount, conf.rightFields, null, null))
      } else {
        Iterator.empty
      }
    } else if (foldsInOneScan(conf.internalK, conf.k)) {
      // No JVM-side over-fetch (the SQL path: internalK == k); see [[foldsInOneScan]]. Every probed
      // row is kept, so probe and materialize in ONE native scan: Lance searches the index AND
      // projects the payload
      // columns in a single pass. A split probe → trim → materialize would re-scan the exact rows
      // the search already found, for nothing. Lance returns them best-first, so no trim is needed.
      val hits = probe.probeRows(
        conf.vectorColumn,
        q,
        conf.internalK,
        conf.metric,
        conf.rightProjection,
        conf.rightFields,
        conf.nprobes,
        conf.refineFactor,
        conf.ef,
        conf.prefilter)
      if (hits.isEmpty) {
        if (conf.outerJoin) {
          Iterator.single(assembleRow(leftRow, conf.leftFieldCount, conf.rightFields, null, null))
        } else {
          Iterator.empty
        }
      } else {
        hits.iterator.map { hit =>
          assembleRow(leftRow, conf.leftFieldCount, conf.rightFields, hit.row, hit.score)
        }
      }
    } else {
      // Split probe → trim → materialize path. Taken when the caller over-fetches (internalK > k):
      // fetch `internalK` cheap refs natively, trim to the final `k` with the top-K heap, then
      // late-materialize ONLY the survivors by `_rowid` — so the payload fetch is paid for just the
      // rows that make the cut. Lance already returns refs best-first.
      val refs = probe
        .probe(
          conf.vectorColumn,
          q,
          conf.internalK,
          conf.metric,
          conf.nprobes,
          conf.refineFactor,
          conf.ef,
          conf.prefilter)
        .toArray
      val trimmed =
        if (refs.length <= conf.k) refs
        else {
          val heap = new TopKHeap(conf.k, conf.smallerIsBetter)
          heap.offerAll(refs)
          heap.drain()
        }

      if (trimmed.isEmpty) {
        if (conf.outerJoin) {
          Iterator.single(assembleRow(leftRow, conf.leftFieldCount, conf.rightFields, null, null))
        } else {
          Iterator.empty
        }
      } else {
        // Late materialization: point-fetch the surviving right rows by `_rowid`. Building the
        // `rowAddr -> row` map collapses any duplicate rowAddr to one payload; we still emit one
        // output row per surviving ref. Bounded by `k`, so this stays per-row, not per-partition.
        val materialized: Map[Long, Map[String, Any]] = probe
          .materialize(
            trimmed.iterator.map(_.rowAddr).toSeq,
            conf.rightProjection,
            conf.rightFields)
          .map(m => extractRowAddr(m) -> m)
          .toMap
        trimmed.iterator.map { ref =>
          val rightMap = materialized.getOrElse(ref.rowAddr, null)
          assembleRow(leftRow, conf.leftFieldCount, conf.rightFields, rightMap, ref.score)
        }
      }
    }
  }

  /**
   * Pull a query vector out of a Spark `Row`'s ArrayType column. The Scala 2.13 `Seq` gotcha is
   * real: `Row.get` on `ArrayType` returns `mutable.ArraySeq`, which `case s: Seq[_]` only matches
   * against the root `scala.collection.Seq` trait (the default `Seq` alias is `immutable.Seq` on
   * 2.13).
   */
  private[knn] def extractVector(row: Row, idx: Int): Array[Float] = {
    if (row.isNullAt(idx)) return null
    row.get(idx) match {
      case s: scala.collection.Seq[_] =>
        s.iterator.map {
          case f: java.lang.Float => f.floatValue()
          case f: Float => f
          case d: java.lang.Double => d.doubleValue().toFloat
          case d: Double => d.toFloat
          case other =>
            throw new IllegalStateException(
              s"Unsupported vector element type: ${other.getClass.getName}")
        }.toArray
      case arr: Array[Float] => arr
      case arr: Array[java.lang.Float] => arr.map(_.floatValue())
      case other =>
        throw new IllegalStateException(
          s"Unsupported vector column representation: ${other.getClass.getName}")
    }
  }

  /** Read the `_rowid` key out of a materialized row map (tolerating boxed / stringy longs). */
  private def extractRowAddr(m: Map[String, Any]): Long =
    m.get(LanceProbe.RowIdColumn) match {
      case Some(l: java.lang.Long) => l.longValue()
      case Some(l: Long) => l
      case Some(other) => other.toString.toLong
      case None =>
        throw new IllegalStateException(
          s"Materialized row missing ${LanceProbe.RowIdColumn}; " +
            s"got keys: ${m.keys.mkString(", ")}")
    }

  /**
   * Assemble one output row: `left fields ++ right fields ++ score`. A null `rightValues` (outer
   * join with no hit) fills the right side with nulls. Each right value is shaped to its target
   * Spark type via [[coerceToSpark]] so the join's `ExpressionEncoder` accepts it.
   */
  private def assembleRow(
      leftRow: Row,
      leftFieldCount: Int,
      rightFields: Seq[StructField],
      rightValues: Map[String, Any],
      score: Any): Row = {
    val arr = new Array[Any](leftFieldCount + rightFields.size + 1)
    var i = 0
    while (i < leftFieldCount) { arr(i) = leftRow.get(i); i += 1 }
    var j = 0
    while (j < rightFields.size) {
      val field = rightFields(j)
      arr(leftFieldCount + j) =
        if (rightValues == null) null
        else coerceToSpark(rightValues.getOrElse(field.name, null), field.dataType)
      j += 1
    }
    arr(leftFieldCount + rightFields.size) = score
    Row.fromSeq(arr.toSeq)
  }

  /**
   * Shape a materialized right-side value to match its target Spark [[DataType]] so the assembled
   * row satisfies the join's `ExpressionEncoder`. [[LanceProbe]] returns payloads Spark-agnostically
   * — an Arrow struct cell arrives as a `Map[String, Any]` keyed by child-field name and a list cell
   * as a `Seq` — but a Spark `StructType` slot expects a positional `Row`, not a `Map`. Without this
   * coercion a nested-struct payload column is handed to the encoder as a `Map` and either fails
   * encoding or materializes as garbage.
   *
   * Recurses so nested shapes all land correctly:
   *  - `StructType` → `Row` built in declared field order, each field coerced to its type
   *  - `ArrayType`  → `Seq` with every element coerced to the element type
   *  - `MapType`    → map with keys and values coerced
   *  - anything else (numeric / string / boolean primitives) → passed through unchanged
   */
  private[knn] def coerceToSpark(value: Any, dataType: DataType): Any = {
    if (value == null) return null
    dataType match {
      case s: StructType =>
        value match {
          case m: scala.collection.Map[_, _] =>
            val byName = m.asInstanceOf[scala.collection.Map[String, Any]]
            Row.fromSeq(
              s.fields.map(f => coerceToSpark(byName.getOrElse(f.name, null), f.dataType)).toSeq)
          case r: Row => r
          case _ => value
        }
      case ArrayType(elementType, _) =>
        value match {
          case seq: scala.collection.Seq[_] => seq.map(v => coerceToSpark(v, elementType))
          case arr: Array[_] => arr.toSeq.map(v => coerceToSpark(v, elementType))
          case _ => value
        }
      case MapType(keyType, valueType, _) =>
        value match {
          case m: scala.collection.Map[_, _] =>
            m.map { case (k, v) => coerceToSpark(k, keyType) -> coerceToSpark(v, valueType) }
          case entries: scala.collection.Seq[_] =>
            // The shape a real Arrow map cell actually arrives in. Arrow represents a `MapType`
            // cell as a LIST of `{key, value}` entry structs, so `LanceProbe.toSparkValue` turns it
            // into a `Seq(Map("key" -> …, "value" -> …), …)` — NOT a Scala map. Left uncoerced the
            // encoder would see a sequence where a `MapType` slot is expected and either fail or
            // materialize garbage. Rebuild a real map from the entries. (The `Map` case above stays
            // for direct-map payloads and JVM-only tests.)
            entries.iterator.map { entry =>
              val (k, v) = mapEntryKeyValue(entry)
              coerceToSpark(k, keyType) -> coerceToSpark(v, valueType)
            }.toMap
          case _ => value
        }
      case _ => value
    }
  }

  /**
   * Pull `(key, value)` out of a single Arrow map entry. `LanceProbe.toSparkValue` renders each
   * entry as a `Map("key" -> …, "value" -> …)` (Arrow's `MapVector` names the entry-struct children
   * `key` / `value`); a positional `Row(key, value)` is tolerated defensively.
   */
  private def mapEntryKeyValue(entry: Any): (Any, Any) = entry match {
    case m: scala.collection.Map[_, _] =>
      val byName = m.asInstanceOf[scala.collection.Map[String, Any]]
      (byName.getOrElse("key", null), byName.getOrElse("value", null))
    case r: Row if r.length >= 2 => (r.get(0), r.get(1))
    case other =>
      throw new IllegalStateException(
        s"Unexpected Arrow map-entry representation: ${other.getClass.getName}")
  }
}
