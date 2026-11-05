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
 * a fixed set of fragments. [[probeRows]] folds the nearest search AND the payload projection into a
 * SINGLE native scan, so each hit comes back with its row-id, ranking score, AND materialized
 * payload — no second point-fetch scan. This is the right shape for the indexed nearest-by join,
 * where every probed row is kept (no JVM-side over-fetch) so a separate materialize scan would just
 * re-fetch the exact rows the search already found.
 *
 * Validating its cost profile is the first thing to do on a new Lance build:
 *  - dataset open should be one-time cost
 *  - per-probe cost should be index traversal + small overhead, not full fragment scan
 *  - returning top-K row addrs should match Lance's native nearest search recall
 *
 * Lifecycle: instantiate per task, call `probeRows(...)` repeatedly, close at end.
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

  // The dataset's own top-level column names, read once from the open handle. Used by the schema
  // eligibility backstop below; cheap (metadata only) and stable for the probe's lifetime.
  private lazy val datasetColumnNames: Seq[String] =
    dataset.getSchema.getFields.asScala.map(_.getName).toSeq

  /**
   * Reject a dataset whose own schema collides with a column the nearest scan injects
   * (`_rowid` / `_distance` / `_score`). [[probeRows]] runs a `nearest` scan that injects those
   * columns, so such a table cannot be served by an indexed route: the injected metadata shadows the
   * physical column and it is read out-of-band as the ranking score (silently dropped from an
   * all-columns payload) or collides outright when projected. The Catalyst rule is expected to
   * DECLINE the indexed rewrite for such a table via [[LanceProbe.schemaSupportsNearest]] and fall
   * back to the engine's default nearest-by execution; this is the defensive backstop for any caller
   * that reached the probe anyway.
   */
  private def requireNearestCompatibleSchema(): Unit = {
    val collisions = LanceProbe.reservedSchemaColumns(datasetColumnNames)
    require(
      collisions.isEmpty,
      s"Lance dataset schema has column(s) ${collisions.toSeq.sorted.mkString(", ")} whose name(s) " +
        "collide with the metadata a nearest scan injects (_rowid, _distance, _score). No indexed " +
        "probe can serve this table — the injected metadata shadows the physical column. Decline the " +
        "indexed rewrite (see LanceProbe.schemaSupportsNearest) and fall back to default nearest-by " +
        "execution.")
  }

  /**
   * Nearest search AND payload projection in a SINGLE scan. Runs the top-`k` search projecting the
   * requested payload `projection` columns directly into the result batch, so each hit comes back
   * with its row-id, ranking score, AND materialized payload — no second point-fetch scan. Rows come
   * back best-first (Lance's native ordering).
   *
   * The indexed nearest-by join keeps every probed row (no JVM-side over-fetch), so folding search
   * and payload projection into one scan avoids a second Lance scan that would just re-fetch the
   * exact rows the search already found. An empty `projection` means "all columns"; each projected
   * cell is converted to the Spark EXTERNAL value its declared type expects (see [[readScoredRows]]).
   *
   * `prefilter` is a Lance SQL filter string (DataFusion-flavored). Lance applies it BEFORE the
   * vector index lookup when `prefilter = true`, so the top-K is computed over only the rows matching
   * the filter — exactly what a `Filter(cond, lance) RIGHT JOIN ... APPROX NEAREST K` should do.
   * Without prefilter pushdown, a per-fragment vector probe could return K rows that are all later
   * filtered out post-join, masking truly-nearest-but-also-matching rows further down the index — a
   * recall bug. The translator in `IndexedNearestByJoinRule` produces only safely-translated SQL;
   * here we just hand it through.
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
      prefilter: Option[String] = None): Seq[MaterializedHit] = {
    require(vectorColumn != null && vectorColumn.nonEmpty, "vectorColumn must be non-empty")
    require(query != null && query.length > 0, "Query vector must be non-empty")
    require(k > 0, "k must be positive")
    // The nearest scan injects `_rowid` (via withRowId) and the score columns (via nearest). If the
    // dataset's own schema has a column by one of those names the injected metadata shadows it, and
    // NO projection shape recovers the physical column: an empty (all-columns) projection reads it
    // out-of-band as the ranking score and silently drops it from the payload, while an explicit
    // projection of it collides inside the scan. So the eligibility is schema-level, not
    // projection-level: reject the whole table. The Catalyst rule declines such a table up front via
    // [[LanceProbe.schemaSupportsNearest]]; this is the defensive backstop.
    requireNearestCompatibleSchema()

    val q = buildNearestQuery(vectorColumn, query, k, metric, nprobes, refineFactor)

    val opts = new ScanOptions.Builder()
      .nearest(q)
      .withRowId(true)
    // Project the payload columns into the nearest scan itself. `_distance` is added by `nearest`
    // regardless (Lance includes it even when explicit columns omit it), so the drain still finds a
    // score column. An empty projection leaves columns unset → all columns.
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

  /** Build the Lance nearest-neighbor query for [[probeRows]]. */
  private def buildNearestQuery(
      vectorColumn: String,
      query: Array[Float],
      k: Int,
      metric: Metric,
      nprobes: Option[Int],
      refineFactor: Option[Int]): Query = {
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
    b.build()
  }

  /**
   * Drain a nearest-search scan that projected payload columns into `(rowId, score, payload)` hits.
   * Row-id and score are read out of the `_rowid` / score vectors directly and excluded from the
   * payload map. Every OTHER column the scan returned is payload: a column with a Spark target type
   * in `projectionFields` goes through the canonical
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
        // through the canonical adapter, the rest fall back to the generic Arrow conversion, so a
        // projected column with no supplied type is surfaced (via `toSparkValue`) instead of being
        // silently dropped.
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

  override def close(): Unit = dataset.close()
}

object LanceProbe {

  /**
   * Lance row-identity virtual column name. We use `_rowid` rather than `_rowaddr` because
   * Lance's INDEXED nearest-search path materializes `_rowid` but not `_rowaddr`, while
   * non-indexed scans materialize both. `_rowid` therefore works on every code path that
   * calls `probeRows()` (with or without a vector index built on the column). Sourced from
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
   * columns (from `nearest`). These are metadata a nearest scan always produces; a right-side
   * table column sharing one of these names collides with the injected column. See
   * [[reservedSchemaColumns]] / [[schemaSupportsNearest]].
   */
  val ReservedProjectionColumns: Set[String] = ScoreColumns.toSet + RowIdColumn

  /**
   * The subset of `schemaColumnNames` that collide with a column the nearest scan injects
   * (`_rowid` / `_distance` / `_score`). Empty means the table is nearest-compatible.
   */
  def reservedSchemaColumns(schemaColumnNames: Iterable[String]): Set[String] =
    schemaColumnNames.iterator.filter(ReservedProjectionColumns.contains).toSet

  /**
   * Whether an indexed nearest scan can run against a right-side table with these column names.
   *
   * Lance's nearest scan ALWAYS injects `_rowid` and the `_distance` / `_score` metadata. If the
   * table's own schema already has a column by one of those names, the injected metadata shadows
   * it and no projection shape recovers the physical column: the fused scan reads it out-of-band as
   * the ranking score and silently drops it from the payload (observed with an empty / all-columns
   * projection), and an explicit projection of it collides outright. Every indexed route runs the
   * same nearest scan, so there is no safe routing around this. The indexed rewrite must therefore
   * DECLINE such a table and fall back to the engine's default nearest-by execution.
   *
   * This is the shared eligibility contract the Catalyst rule consults before intercepting; the
   * probe enforces it defensively too (see the schema guard in [[LanceProbe.probeRows]]).
   */
  def schemaSupportsNearest(schemaColumnNames: Iterable[String]): Boolean =
    reservedSchemaColumns(schemaColumnNames).isEmpty

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
