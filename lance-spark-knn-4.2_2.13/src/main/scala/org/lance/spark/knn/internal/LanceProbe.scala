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

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, Float4Vector, Float8Vector, UInt8Vector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.lance.Dataset
import org.lance.ipc.{LanceScanner, Query, ScanOptions}
import org.lance.spark.{LanceConstant, LanceRef, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.utils.Utils
import org.lance.spark.vectorized.LanceArrowColumnVector

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Per-task vector-index probe primitive. Opens a Lance dataset once and serves many queries against
 * a fixed set of fragments. Two query shapes:
 *  - [[probe]] returns row references + scores only (no payload); the payload is fetched later via
 *    [[materialize]]. This split late-materialization is the right shape when the caller OVER-FETCHES
 *    candidates and trims before materializing — only the survivors are paid for.
 *  - [[probeRows]] folds the nearest search AND the payload projection into a SINGLE scan. This is
 *    the right shape when there is NO JVM-side over-fetch (every probed row is kept), so a separate
 *    materialize scan would just re-fetch the exact rows the search already found.
 *
 * This is the core primitive Phase 0 of the indexed nearest-by design depends on. Validating its
 * cost profile is the first thing to do on a new Lance build:
 *  - dataset open should be one-time cost
 *  - per-probe cost should be index traversal + small overhead, not full fragment scan
 *  - returning top-K row addrs should match Lance's native nearest search recall
 *
 * Lifecycle: instantiate per task, call `probe(...)` repeatedly, close at end.
 *
 * @param readOptions           Fully resolved Lance read context — dataset URI, storage
 *                              credentials, catalog / cache backends, and the pinned branch /
 *                              version ref. Inside a join this is resolved and pinned once on the
 *                              driver (see [[LanceKnnJoinStage.resolveReadContext]]) so every task
 *                              probes the same snapshot.
 * @param initialStorageOptions Driver-side storage options from `namespace.describeTable()`, merged
 *                              into the base storage options at open. May be `null`.
 * @param namespaceImpl         Namespace implementation type, for reconnecting the namespace on an
 *                              executor where the live handle did not survive serialization. May be
 *                              `null` for a plain URI dataset.
 * @param namespaceProperties   Namespace connection properties for that reconnection. May be `null`.
 * @param fragmentIds           Fragments this probe is restricted to. Pass `None` for whole-dataset
 *                              search.
 * @param allocator             Arrow allocator. Defaults to lance-spark's shared
 *                              `LanceRuntime.allocator()`.
 */
final class LanceProbe(
    readOptions: LanceSparkReadOptions,
    initialStorageOptions: java.util.Map[String, String],
    namespaceImpl: String,
    namespaceProperties: java.util.Map[String, String],
    fragmentIds: Option[Seq[Int]],
    allocator: BufferAllocator = LanceRuntime.allocator())
  extends AutoCloseable {

  /**
   * URI + optional pinned-version convenience constructor. Builds read options straight from a bare
   * dataset URI and pins `version` on the main branch when present. For callers / tests that have
   * only a plain URI and no namespace or storage context.
   */
  def this(datasetUri: String, fragmentIds: Option[Seq[Int]], version: Option[Long]) =
    this(LanceProbe.readOptionsFor(datasetUri, version), null, null, null, fragmentIds)

  def this(datasetUri: String, fragmentIds: Option[Seq[Int]]) =
    this(datasetUri, fragmentIds, None)

  // Open the dataset once. Lance's Java binding caches index metadata against the Dataset handle,
  // so reusing it across probes keeps subsequent calls index-warm. Opening through
  // `Utils.openDatasetBuilder` (rather than a bare `Dataset.open().uri(...)`) makes the probe honor
  // the full resolved read context — storage credentials, catalog / cache backends, the pinned
  // branch / version ref, and (on executors, where the live namespace handle is transient) the
  // runtime-namespace reconnection — exactly as the connector's own scan path does.
  private val dataset: Dataset = openDataset()

  private val javaFragmentIds: Option[util.List[Integer]] = fragmentIds.map { ids =>
    val javaList = new util.ArrayList[Integer](ids.size)
    ids.foreach(i => javaList.add(Integer.valueOf(i)))
    javaList: util.List[Integer]
  }

  private def openDataset(): Dataset = {
    // Apply the connector's exact worker-open namespace policy (`LanceFragmentScanner.create`):
    // touch the namespace ONLY when a namespace impl is configured AND executor credential refresh
    // is enabled. When refresh is on, either rebuild the namespace client (impls that must run on
    // workers) or clear it (impls that must not, so the open falls back to the URI + initial storage
    // options). When refresh is OFF we leave the read options' namespace exactly as shipped — this
    // is the whole point of `executor_credential_refresh=false`, and forcing a rebuild here would
    // turn that policy into a namespace class-load / RPC that can fail before the dataset even opens.
    //
    // Notably this does NOT call `builder.runtimeNamespace(namespaceImpl, ...)` unconditionally
    // (which always loads the namespace impl class) — matching the fragment scanner, which reaches
    // the namespace only through the guarded `setNamespace` path above.
    if (namespaceImpl != null && readOptions.isExecutorCredentialRefresh()) {
      if (LanceRuntime.useNamespaceOnWorkers(namespaceImpl)) {
        readOptions.setNamespace(
          LanceRuntime.getOrCreateNamespace(namespaceImpl, namespaceProperties))
      } else {
        readOptions.setNamespace(null)
      }
    }
    val builder = Utils.openDatasetBuilder(readOptions)
    if (initialStorageOptions != null) {
      builder.initialStorageOptions(initialStorageOptions)
    }
    builder.build()
  }

  /**
   * Run a single nearest-neighbor query. Returns up to `k` row references for the configured
   * fragments, ordered best-first by `metric`.
   *
   * Implementation note: lance-spark mandates `prefilter = true` for fragmented vector queries
   * (see `LanceFragmentScanner.create`). We mirror that here — Lance's index probe semantics
   * require it when fragment scope is restricted.
   *
   * `vectorColumn` is a per-call argument (not a constructor field) because the same
   * `LanceProbe` instance also serves the materialize stage via [[materialize]], which
   * doesn't reference any vector column. Keeping it on the call sidesteps the smell of
   * passing a placeholder string when constructing for materialize-only use.
   *
   * `prefilter` is a Lance SQL filter string (DataFusion-flavored). Lance applies it BEFORE the
   * vector index lookup when `prefilter = true` (which we always set), so the top-K is computed
   * over only the rows matching the filter — exactly what a `Filter(cond, lance) RIGHT JOIN ...
   * APPROX NEAREST K` should do. Without prefilter pushdown, a per-fragment vector probe could
   * return K rows that are all later filtered out post-join, masking truly-nearest-but-also-
   * matching rows further down the index — a recall bug. The translator in
   * `IndexedNearestByJoinRule` is responsible for producing only safely-translated SQL; here we
   * just hand it through.
   */
  def probe(
      vectorColumn: String,
      query: Array[Float],
      k: Int,
      metric: Metric,
      nprobes: Option[Int] = None,
      refineFactor: Option[Int] = None,
      ef: Option[Int] = None,
      prefilter: Option[String] = None): Seq[ScoredRowRef] = {
    require(vectorColumn != null && vectorColumn.nonEmpty, "vectorColumn must be non-empty")
    require(query != null && query.length > 0, "Query vector must be non-empty")
    require(k > 0, "k must be positive")

    val q = buildNearestQuery(vectorColumn, query, k, metric, nprobes, refineFactor, ef)

    val opts = new ScanOptions.Builder()
      .nearest(q)
      // EXPERIMENT: drop prefilter(true). The single-machine reference path
      // doesn't set it; this LanceProbe call does. Comparing wallclock with
      // and without isolates whether the prefilter branch in
      // vector_search_source forces a slower index plan than the postfilter
      // (default) branch. Re-enable when fragmented probe + prefilter
      // pushdown is needed (we know fragmentIds requires prefilter from the
      // Lance-side error, but at probeParallelism=1 there are no fragments).
      .withRowId(true)
      // Project only what we need into the result. The vector column is implied by `nearest`;
      // requesting an empty user column list keeps the Arrow batch narrow (just the rowid +
      // distance metadata). Materialization fetches payload columns later.
      .columns(java.util.Collections.emptyList[String]())

    if (prefilter.nonEmpty || javaFragmentIds.nonEmpty) {
      // Real prefilter or fragment scope is requested — keep prefilter(true)
      // so Lance applies the filter / restricts to fragments correctly.
      opts.prefilter(true)
    }

    prefilter.filter(_.nonEmpty).foreach(opts.filter)
    javaFragmentIds.foreach(opts.fragmentIds)

    val scanner: LanceScanner = LanceScanner.create(dataset, opts.build(), allocator)
    try {
      readScored(scanner.scanBatches())
    } finally {
      scanner.close()
    }
  }

  /**
   * Nearest search AND payload projection in a SINGLE scan. Runs the top-`k` search projecting the
   * requested payload `projection` columns directly into the result batch, so each hit comes back
   * with its row-id, ranking score, AND materialized payload — no second point-fetch scan.
   *
   * This is the no-overfetch fast path. When the caller does not over-fetch at the JVM level (the
   * SQL path: `internalK == k`), every probed row is kept, so the split [[probe]] → trim →
   * [[materialize]] would just re-scan the exact rows the search already found — a second Lance scan
   * plus a `k`-element `_rowid IN (arrow_cast …)` filter parse, per query, for nothing. Folding the
   * two removes both. Rows come back best-first (Lance's native ordering), same as [[probe]].
   *
   * Use [[probe]] + [[materialize]] instead when the caller OVER-fetches candidates and trims before
   * materializing — there, deferring the payload fetch to only the survivors is the win. `projection`
   * / `projectionFields` follow [[materialize]]'s contract: an empty `projection` means "all columns",
   * and each projected cell is converted to the Spark EXTERNAL value its declared type expects (see
   * [[readRows]]).
   */
  def probeRows(
      vectorColumn: String,
      query: Array[Float],
      k: Int,
      metric: Metric,
      projection: Seq[String],
      projectionFields: Seq[StructField],
      nprobes: Option[Int] = None,
      refineFactor: Option[Int] = None,
      ef: Option[Int] = None,
      prefilter: Option[String] = None): Seq[MaterializedHit] = {
    require(vectorColumn != null && vectorColumn.nonEmpty, "vectorColumn must be non-empty")
    require(query != null && query.length > 0, "Query vector must be non-empty")
    require(k > 0, "k must be positive")
    // The fused scan injects `_rowid` (via withRowId) and the score column (via nearest). A payload
    // column that shares one of those names would collide with the injected column inside the SAME
    // scan — Lance fails the scan with "merge incompatible fields". Such a schema must go through the
    // split probe + materialize path (whose materialize scan injects no score column), so reject it
    // here rather than let the collision surface as an opaque Arrow error. The join stage checks
    // [[LanceProbe.fusesCleanly]] and routes accordingly.
    require(
      LanceProbe.fusesCleanly(projection),
      "probeRows cannot project a column whose name collides with Lance search metadata " +
        s"(${LanceProbe.ReservedProjectionColumns.toSeq.sorted.mkString(", ")}); the nearest scan " +
        "injects those columns itself. Route such schemas through the split probe + materialize path.")

    val q = buildNearestQuery(vectorColumn, query, k, metric, nprobes, refineFactor, ef)

    val opts = new ScanOptions.Builder()
      .nearest(q)
      .withRowId(true)
    // Project the payload columns into the nearest scan itself. `_distance` is added by `nearest`
    // regardless (Lance includes it even when explicit columns omit it), so the drain still finds a
    // score column. An empty projection leaves columns unset → all columns, matching `materialize`.
    if (projection.nonEmpty) {
      opts.columns(projection.toList.asJava)
    }

    if (prefilter.nonEmpty || javaFragmentIds.nonEmpty) {
      opts.prefilter(true)
    }
    prefilter.filter(_.nonEmpty).foreach(opts.filter)
    javaFragmentIds.foreach(opts.fragmentIds)

    val scanner: LanceScanner = LanceScanner.create(dataset, opts.build(), allocator)
    try {
      readScoredRows(scanner.scanBatches(), projectionFields)
    } finally {
      scanner.close()
    }
  }

  /**
   * Build the Lance nearest-neighbor query. Shared by [[probe]] (refs only) and [[probeRows]]
   * (refs + folded payload) so both search the index identically.
   */
  private def buildNearestQuery(
      vectorColumn: String,
      query: Array[Float],
      k: Int,
      metric: Metric,
      nprobes: Option[Int],
      refineFactor: Option[Int],
      ef: Option[Int]): Query = {
    val b = new Query.Builder()
      .setColumn(vectorColumn)
      .setKey(query)
      .setK(k)
      .setDistanceType(metric.lanceType)
    nprobes.foreach(b.setNprobes(_))
    // refineFactor: IVF-PQ recall knob. Lance fetches `k * refineFactor` approximate
    // candidates, then re-ranks them with exact distance and trims to k. Bigger factor =
    // better recall, more compute. None leaves Lance's default (= 1, no re-rank).
    refineFactor.foreach(b.setRefineFactor(_))
    // ef: HNSW search depth. Higher = better recall, more compute. None leaves Lance's
    // index-default. Only meaningful for HNSW indexes; ignored for IVF-PQ.
    ef.foreach(b.setEf(_))
    b.build()
  }

  /**
   * Drain the Arrow stream from a nearest-search scan into `(rowId, score)` pairs.
   *
   * Expected schema:
   *   - `_rowid`   : UInt8 / BigInt — Lance logical row identifier
   *   - `_distance` (or score column added by `nearest`) : Float4 / Float8 — ranking value
   *
   * We resolve columns by name to be encoding-version-agnostic; the underlying primitive type
   * (UInt8 vs BigInt for the id, Float4 vs Float8 for score) varies across Arrow / Lance combos
   * and we tolerate both.
   */
  private def readScored(reader: ArrowReader): Seq[ScoredRowRef] = {
    val out = mutable.ArrayBuffer.empty[ScoredRowRef]
    try {
      while (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        val addrVec: FieldVector = root.getVector(LanceProbe.RowIdColumn)
        val scoreVec: FieldVector = resolveScoreVector(root)

        val n = root.getRowCount
        var i = 0
        while (i < n) {
          out += ScoredRowRef(rowAddrAt(addrVec, i), scoreAt(scoreVec, i))
          i += 1
        }
      }
    } finally {
      reader.close()
    }
    out.toSeq
  }

  /**
   * Drain a nearest-search scan that ALSO projected payload columns into `(rowId, score, payload)`
   * hits — the folded counterpart of [[readScored]] + [[readRows]]. Row-id and score are read out of
   * the `_rowid` / score vectors directly and excluded from the payload map. Every OTHER column the
   * scan returned is payload and follows [[readRows]]' contract exactly: a column with a Spark target
   * type in `projectionFields` goes through the canonical
   * [[org.lance.spark.vectorized.LanceArrowColumnVector]] adapter and back through
   * [[CatalystTypeConverters]] to the external value the encoder expects; a projected column WITHOUT
   * a supplied type falls back to the generic Arrow conversion ([[LanceProbe.toSparkValue]]) rather
   * than being silently dropped.
   */
  private def readScoredRows(
      reader: ArrowReader,
      projectionFields: Seq[StructField]): Seq[MaterializedHit] = {
    val schemaByName: Map[String, StructField] =
      projectionFields.iterator.map(f => f.name -> f).toMap
    val out = mutable.ArrayBuffer.empty[MaterializedHit]
    try {
      while (reader.loadNextBatch()) {
        val root: VectorSchemaRoot = reader.getVectorSchemaRoot
        val n = root.getRowCount
        val fields = root.getSchema.getFields.asScala.toIndexedSeq

        val addrVec: FieldVector = root.getVector(LanceProbe.RowIdColumn)
        val scoreVec: FieldVector = resolveScoreVector(root)

        // `_rowid` and the injected score column are read out-of-band (above) and excluded from the
        // payload. Everything else the scan returned IS payload: columns with a Spark target type go
        // through the canonical adapter, the rest fall back to the generic Arrow conversion — the
        // same split `readRows` / `materialize` apply, so a projected column with no supplied type is
        // surfaced (via `toSparkValue`) instead of being silently dropped.
        val reserved = Set(LanceProbe.RowIdColumn, scoreVec.getField.getName)
        val payloadFields = fields.filterNot(af => reserved.contains(af.getName))
        val mapped = payloadFields.filter(af => schemaByName.contains(af.getName))
        val unmapped = payloadFields.filterNot(af => schemaByName.contains(af.getName))
        val mappedNames: Array[String] = mapped.iterator.map(_.getName).toArray
        val mappedTypes: Array[DataType] =
          mapped.iterator.map(af => schemaByName(af.getName).dataType).toArray
        val mappedConverters: Array[Any => Any] = mapped.iterator
          .map(af =>
            CatalystTypeConverters.createToScalaConverter(schemaByName(af.getName).dataType))
          .toArray
        val mappedVectors: Array[ColumnVector] = mapped.iterator
          .map(af =>
            new LanceArrowColumnVector(root.getVector(af.getName), false, schemaByName(af.getName))
              .asInstanceOf[ColumnVector])
          .toArray
        // Thin view over the reader-owned Arrow vectors (closeVectorOnClose=false): `reader.close()`
        // in the finally frees the buffers once the values below are copied out.
        val batch = new ColumnarBatch(mappedVectors, n)

        var i = 0
        while (i < n) {
          val rowMap = mutable.LinkedHashMap.empty[String, Any]
          val internalRow = batch.getRow(i)
          var j = 0
          while (j < mappedNames.length) {
            val internal = internalRow.get(j, mappedTypes(j))
            rowMap(mappedNames(j)) = if (internal == null) null else mappedConverters(j)(internal)
            j += 1
          }
          unmapped.foreach { af =>
            val v = root.getVector(af.getName)
            rowMap(af.getName) = if (v.isNull(i)) null else LanceProbe.toSparkValue(v.getObject(i))
          }
          out += MaterializedHit(rowAddrAt(addrVec, i), scoreAt(scoreVec, i), rowMap.toMap)
          i += 1
        }
      }
    } finally {
      reader.close()
    }
    out.toSeq
  }

  /** Locate the nearest-search score column by name, tolerant of `_distance` vs `_score`. */
  private def resolveScoreVector(root: VectorSchemaRoot): FieldVector =
    LanceProbe.ScoreColumns.iterator
      .map(name => Option(root.getVector(name)).orNull)
      .find(_ != null)
      .getOrElse(throw new IllegalStateException(
        "Lance nearest scan did not return a score column. Got: " +
          root.getSchema.getFields.asScala.map(_.getName).mkString(", ")))

  /** Read a Lance row id out of an Arrow `_rowid` vector (UInt8 or BigInt, encoding-dependent). */
  private def rowAddrAt(addrVec: FieldVector, i: Int): Long = addrVec match {
    case v: UInt8Vector => v.get(i)
    case v: BigIntVector => v.get(i)
    case other =>
      throw new IllegalStateException(
        s"Unexpected row-address vector type: ${other.getClass.getName}")
  }

  /** Read a ranking score out of an Arrow score vector (Float4 or Float8, encoding-dependent). */
  private def scoreAt(scoreVec: FieldVector, i: Int): Float = scoreVec match {
    case v: Float4Vector => v.get(i)
    case v: Float8Vector => v.get(i).toFloat
    case other =>
      throw new IllegalStateException(
        s"Unexpected score vector type: ${other.getClass.getName}")
  }

  /**
   * Materialize a set of right-side rows by their `_rowaddr`s. Used by the join's materialize
   * stage to fetch full payloads after the probe + merge has decided which rows survive.
   *
   * The row addresses are pushed down as a `_rowaddr IN (...)` filter, which Lance executes via
   * its row-address index — the natural point-fetch path. The result is unordered with respect
   * to the input list; the caller re-aligns by `_rowaddr`.
   *
   * @param rowAddrs   list of Lance `_rowid` values (parameter name retained for source
   *                   compatibility with callers — semantically these are now row IDs).
   * @param projection projected column list. `Seq.empty` means "all columns".
   * @param projectionFields Spark target fields (name + dataType) for the projected columns. When
   *                   non-empty, each projected payload cell is converted to the Spark EXTERNAL
   *                   value its declared type expects (see [[readRows]]); when empty, cells fall
   *                   back to a generic Arrow-object conversion.
   * @return a sequence of materialized rows, each a `Map[String, Any]` keyed by column name. With
   *         `projectionFields` supplied, each projected value is already the Spark external
   *         representation of its target type — the shape the join's `ExpressionEncoder` accepts —
   *         plus an entry under `LanceProbe.RowIdColumn` (a plain long) so the caller can re-key.
   *         Building those values into a `Row` / `InternalRow` happens in the join stage.
   */
  def materialize(
      rowAddrs: Seq[Long],
      projection: Seq[String] = Seq.empty,
      projectionFields: Seq[StructField] = Seq.empty): Seq[Map[String, Any]] = {
    if (rowAddrs.isEmpty) return Seq.empty

    val opts = new ScanOptions.Builder().withRowId(true)
    if (projection.nonEmpty) {
      opts.columns(projection.toList.asJava)
    }
    // `_rowid IN (a, b, c)` — Lance lowers this to its row-id lookup path. Same point-fetch
    // semantics as `_rowaddr IN (...)` previously used here, but `_rowid` is the universal
    // identifier (works on indexed + non-indexed scan paths alike).
    //
    // Each row ID is rendered as `arrow_cast('<unsigned-string>', 'UInt64')` for two
    // compounding reasons:
    //
    //   1. Lance row IDs are 64-bit UNSIGNED; storing them as Java signed `long` means
    //      values >= 2^63 come back negative. `mkString(", ")` would render them as
    //      negative integer literals and Lance/DataFusion would reject (`Int64(-...)
    //      cannot convert to UInt64`).
    //   2. Even after `Long.toUnsignedString` produces a positive 20-digit decimal,
    //      DataFusion's SQL parser tries `Int64` first, overflows, then falls back to
    //      `Float64`. `Float64` loses precision past 2^53 — the literal becomes a
    //      different number — and DataFusion then can't downcast `Float64` to `UInt64`.
    //
    // `arrow_cast(string, 'UInt64')` bypasses both: the string literal goes through
    // `arrow_cast`'s own coercion, which is precision-preserving for UInt64.
    //
    // At 100K rows row IDs stay below 2^53 and both layers of the bug are invisible; at
    // 1M+ rows they bite. Caught when the DataFrame benchmark hit 1M-row scale.
    val rowIdLiterals = rowAddrs.iterator
      .map(addr => s"arrow_cast('${java.lang.Long.toUnsignedString(addr)}', 'UInt64')")
      .mkString(", ")
    opts.filter(s"${LanceProbe.RowIdColumn} IN ($rowIdLiterals)")
    javaFragmentIds.foreach(opts.fragmentIds)

    val scanner: LanceScanner = LanceScanner.create(dataset, opts.build(), allocator)
    try {
      readRows(scanner.scanBatches(), projectionFields)
    } finally {
      scanner.close()
    }
  }

  /**
   * Drain the materialize scan into row maps, converting each projected payload cell to the Spark
   * EXTERNAL value its target type expects — the shape the join's `ExpressionEncoder` (external
   * `Row` → `InternalRow`) accepts.
   *
   * Projected data columns are materialized through the connector's canonical Arrow→Spark adapter
   * [[org.lance.spark.vectorized.LanceArrowColumnVector]] — the same vector-and-schema-aware path
   * the connector's own reader uses — so every payload type Lance can store round-trips to the
   * value Spark produces for that column on an ordinary read: DateType, the various timestamp / time
   * units, unsigned integers, decimals, fixed/large binary and varchar, and nested structs / lists /
   * maps. A raw Arrow `getObject` would instead hand back e.g. a `LocalDate` for a DateType column,
   * which the encoder then rejects. The adapter yields Spark INTERNAL values (days for a date,
   * micros for a timestamp, …), so each is run back through [[CatalystTypeConverters]] to the
   * external representation the `Row` encoder wants.
   *
   * Columns absent from `projectionFields` — notably the `_rowid` virtual column, which carries no
   * Spark type here — keep the generic Arrow `getObject` fallback; the caller reads `_rowid` only as
   * a plain long.
   */
  private def readRows(
      reader: ArrowReader,
      projectionFields: Seq[StructField]): Seq[Map[String, Any]] = {
    val schemaByName: Map[String, StructField] =
      projectionFields.iterator.map(f => f.name -> f).toMap
    val out = mutable.ArrayBuffer.empty[Map[String, Any]]
    try {
      while (reader.loadNextBatch()) {
        val root: VectorSchemaRoot = reader.getVectorSchemaRoot
        val n = root.getRowCount
        val fields = root.getSchema.getFields.asScala.toIndexedSeq

        // Columns with a Spark target type are converted through the canonical connector adapter;
        // the rest (e.g. `_rowid`) keep the raw Arrow-object fallback.
        val mapped = fields.filter(af => schemaByName.contains(af.getName))
        val unmapped = fields.filterNot(af => schemaByName.contains(af.getName))
        val mappedNames: Array[String] = mapped.iterator.map(_.getName).toArray
        val mappedTypes: Array[DataType] =
          mapped.iterator.map(af => schemaByName(af.getName).dataType).toArray
        val mappedConverters: Array[Any => Any] = mapped.iterator
          .map(af =>
            CatalystTypeConverters.createToScalaConverter(schemaByName(af.getName).dataType))
          .toArray
        val mappedVectors: Array[ColumnVector] = mapped.iterator
          .map(af =>
            new LanceArrowColumnVector(root.getVector(af.getName), false, schemaByName(af.getName))
              .asInstanceOf[ColumnVector])
          .toArray
        // The batch is a thin view over the (reader-owned) Arrow vectors; closeVectorOnClose=false
        // above means neither the batch nor its column vectors free the underlying buffers — the
        // `reader.close()` in the finally does, once the values below have been copied out.
        val batch = new ColumnarBatch(mappedVectors, n)

        var i = 0
        while (i < n) {
          val rowMap = mutable.LinkedHashMap.empty[String, Any]
          val internalRow = batch.getRow(i)
          var j = 0
          while (j < mappedNames.length) {
            val internal = internalRow.get(j, mappedTypes(j))
            rowMap(mappedNames(j)) = if (internal == null) null else mappedConverters(j)(internal)
            j += 1
          }
          unmapped.foreach { af =>
            val v = root.getVector(af.getName)
            rowMap(af.getName) = if (v.isNull(i)) null else LanceProbe.toSparkValue(v.getObject(i))
          }
          out += rowMap.toMap
          i += 1
        }
      }
    } finally {
      reader.close()
    }
    out.toSeq
  }

  override def close(): Unit = dataset.close()
}

object LanceProbe {

  /**
   * Lance row-identity virtual column name. We use `_rowid` rather than `_rowaddr` because
   * Lance's INDEXED nearest-search path materializes `_rowid` but not `_rowaddr`, while
   * non-indexed scans materialize both. `_rowid` therefore works on every code path that
   * calls `probe()` (with or without a vector index built on the column). Sourced from
   * `LanceConstant` to keep the literal defined in exactly one place.
   */
  val RowIdColumn: String = LanceConstant.ROW_ID

  /**
   * Candidate names for the score column in a Lance nearest-search result. Lance's vector indexes
   * have used `_distance` historically; tolerate `_score` too in case future versions rename it.
   * The lookup is name-based so the consumer is agnostic to where Lance puts the column in its
   * output schema.
   */
  val ScoreColumns: Seq[String] = Seq("_distance", "_score")

  /**
   * Column names Lance's nearest scan injects itself: `_rowid` (from `withRowId`) and the score
   * column (from `nearest`). A right-side payload column with one of these names collides with the
   * injected column inside the fused [[LanceProbe.probeRows]] scan, which Lance rejects with a
   * "merge incompatible fields" error. A schema that projects one of them must be served through the
   * split [[LanceProbe.probe]] + [[LanceProbe.materialize]] path instead — the materialize scan does
   * not inject a score column, so there is no collision. See [[fusesCleanly]].
   */
  val ReservedProjectionColumns: Set[String] = ScoreColumns.toSet + RowIdColumn

  /**
   * True if `projection` can be served by the fused [[LanceProbe.probeRows]] scan — i.e. it names no
   * column the nearest scan injects (see [[ReservedProjectionColumns]]). The join stage calls this to
   * decide between the fold and the split probe + materialize path.
   */
  def fusesCleanly(projection: Seq[String]): Boolean =
    !projection.exists(ReservedProjectionColumns.contains)

  /**
   * Build read options from a bare dataset URI, pinning `version` on the main branch when present.
   * Backs the URI convenience constructor.
   */
  private[knn] def readOptionsFor(
      datasetUri: String,
      version: Option[Long]): LanceSparkReadOptions = {
    val base = LanceSparkReadOptions.from(datasetUri)
    version match {
      case Some(v) => base.withRef(LanceRef.ofMain(v))
      case None => base
    }
  }

  /**
   * Convert an Arrow-returned cell value into something Spark's encoders accept when stuffed
   * into a `Row`. Arrow's `FieldVector.getObject` returns Java types (boxed primitives,
   * `JsonStringArrayList` for list cells, `Text` for utf8) which Spark's `RowEncoder` does not
   * always understand directly — most painfully, a `java.util.ArrayList` can't satisfy a Spark
   * `ArrayType` slot, which expects a `scala.collection.Seq`.
   *
   * Conversion rules, in order:
   *  - `java.util.List` → recursively-converted `Seq`
   *  - `java.util.Map`  → recursively-converted Scala `Map`
   *  - `org.apache.arrow.vector.util.Text` → `String`
   *  - `Number` boxed primitives → returned as-is (Spark handles them)
   *  - everything else → returned as-is (caller's responsibility)
   *
   * Recursive on lists/maps to handle nested types (arrays of structs, etc.) without surprises
   * for callers.
   */
  def toSparkValue(value: Any): Any = value match {
    case null => null
    case list: java.util.List[_] =>
      val out = scala.collection.mutable.ArrayBuffer.empty[Any]
      val it = list.iterator
      while (it.hasNext) out += toSparkValue(it.next())
      out.toSeq
    case map: java.util.Map[_, _] =>
      val out = scala.collection.mutable.LinkedHashMap.empty[Any, Any]
      val it = map.entrySet().iterator
      while (it.hasNext) {
        val e = it.next()
        out(toSparkValue(e.getKey)) = toSparkValue(e.getValue)
      }
      out.toMap
    case t: org.apache.arrow.vector.util.Text => t.toString
    case other => other
  }
}
