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
import org.lance.{Dataset, ReadOptions}
import org.lance.ipc.{LanceScanner, Query, ScanOptions}
import org.lance.spark.{LanceConstant, LanceRuntime}

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Per-task vector-index probe primitive. Opens a Lance dataset once and runs many `probe()` calls
 * against a fixed set of fragments. Returns row references + scores only — no payload. Late
 * materialization happens elsewhere (`LanceMaterialize`).
 *
 * This is the core primitive Phase 0 of the indexed nearest-by design depends on. Validating its
 * cost profile is the first thing to do on a new Lance build:
 *  - dataset open should be one-time cost
 *  - per-probe cost should be index traversal + small overhead, not full fragment scan
 *  - returning top-K row addrs should match Lance's native nearest search recall
 *
 * Lifecycle: instantiate per task, call `probe(...)` repeatedly, close at end.
 *
 * @param datasetUri  Lance dataset URI (passed straight to `Dataset.open`).
 * @param fragmentIds Fragments this probe is restricted to. Pass `None` for whole-dataset search.
 * @param version     Optional Lance version to pin. Required when used inside a join, so all
 *                    probe / materialize stages see the same snapshot.
 * @param allocator   Arrow allocator. Defaults to lance-spark's shared `LanceRuntime.allocator()`.
 */
final class LanceProbe(
    datasetUri: String,
    fragmentIds: Option[Seq[Int]],
    version: Option[Long] = None,
    allocator: BufferAllocator = LanceRuntime.allocator())
  extends AutoCloseable {

  // Open the dataset once. Lance's Java binding caches index metadata against the Dataset handle,
  // so reusing it across probes keeps subsequent calls index-warm.
  private val dataset: Dataset = openDataset()

  private val javaFragmentIds: Option[util.List[Integer]] = fragmentIds.map { ids =>
    val javaList = new util.ArrayList[Integer](ids.size)
    ids.foreach(i => javaList.add(Integer.valueOf(i)))
    javaList: util.List[Integer]
  }

  private def openDataset(): Dataset = {
    val readOpts = {
      val b = new ReadOptions.Builder()
      version.foreach(v => b.setVersion(v))
      b.build()
    }
    Dataset.open()
      .uri(datasetUri)
      .allocator(allocator)
      .readOptions(readOpts)
      .build()
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

    val q = {
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
        val scoreVec: FieldVector = LanceProbe.ScoreColumns.iterator
          .map(name => Option(root.getVector(name)).orNull)
          .find(_ != null)
          .getOrElse(throw new IllegalStateException(
            s"Lance nearest scan did not return a score column. Got: " +
              root.getSchema.getFields.asScala.map(_.getName).mkString(", ")))

        val n = root.getRowCount
        var i = 0
        while (i < n) {
          val addr = addrVec match {
            case v: UInt8Vector => v.get(i)
            case v: BigIntVector => v.get(i)
            case other =>
              throw new IllegalStateException(
                s"Unexpected row-address vector type: ${other.getClass.getName}")
          }
          val score = scoreVec match {
            case v: Float4Vector => v.get(i)
            case v: Float8Vector => v.get(i).toFloat
            case other =>
              throw new IllegalStateException(
                s"Unexpected score vector type: ${other.getClass.getName}")
          }
          out += ScoredRowRef(addr, score)
          i += 1
        }
      }
    } finally {
      reader.close()
    }
    out.toSeq
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
   * @return a sequence of materialized rows, each represented as a `Map[String, Any]` for the
   *         projected columns plus an entry under `LanceProbe.RowIdColumn` so the caller can
   *         re-key. Returning a Map keeps this primitive Spark-agnostic; conversion to
   *         `InternalRow` happens in the API layer.
   */
  def materialize(
      rowAddrs: Seq[Long],
      projection: Seq[String] = Seq.empty): Seq[Map[String, Any]] = {
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
      readRows(scanner.scanBatches())
    } finally {
      scanner.close()
    }
  }

  private def readRows(reader: ArrowReader): Seq[Map[String, Any]] = {
    val out = mutable.ArrayBuffer.empty[Map[String, Any]]
    try {
      while (reader.loadNextBatch()) {
        val root: VectorSchemaRoot = reader.getVectorSchemaRoot
        val n = root.getRowCount
        var i = 0
        while (i < n) {
          val rowMap = mutable.LinkedHashMap.empty[String, Any]
          val fields = root.getSchema.getFields.asScala
          var f = 0
          while (f < fields.size) {
            val name = fields(f).getName
            val v = root.getVector(name)
            rowMap(name) = if (v.isNull(i)) null else LanceProbe.toSparkValue(v.getObject(i))
            f += 1
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
