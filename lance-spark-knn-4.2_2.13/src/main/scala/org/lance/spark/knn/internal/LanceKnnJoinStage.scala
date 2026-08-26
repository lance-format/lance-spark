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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField

import scala.collection.mutable

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
 *       val hits = probe.probe(query(leftRow), internalK, ...)       // native top-K search
 *       val topK = trimToK(hits)                                     // overfetch → K
 *       val payloads = probe.materialize(topK.map(_.rowAddr))        // late point-fetch by _rowid
 *       topK.map(ref => assembleRow(leftRow, payloads(ref), ref.score))
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
   */
  final case class Conf(
      datasetUri: String,
      version: Option[Long],
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
   * Run the join for one partition of left rows. Opens the probe once, probes + materializes per
   * row, and returns assembled join rows. Materializing into an `ArrayBuffer` before returning the
   * iterator is deliberate: Spark pulls from `mapPartitions` lazily, so a bare lazy iterator would
   * let the consumer outlive the `try`/`finally` and read from a closed probe handle.
   */
  def runPartition(leftRows: Iterator[Row], conf: Conf): Iterator[Row] = {
    if (leftRows.isEmpty) return Iterator.empty

    val probe = new LanceProbe(conf.datasetUri, fragmentIds = None, version = conf.version)
    val out = mutable.ArrayBuffer.empty[Row]
    try {
      leftRows.foreach { leftRow =>
        val q = extractVector(leftRow, conf.leftVecIdx)
        if (q == null) {
          // Null query vector: nothing to search. Emit a null-right row only for an outer join.
          if (conf.outerJoin) {
            out += assembleRow(leftRow, conf.leftFieldCount, conf.rightFields, null, null)
          }
        } else {
          // Overfetch `internalK` candidates natively, then trim to the final `k`. Lance already
          // returns them best-first, so when it hands back no more than `k` we keep them as-is and
          // skip the heap entirely.
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
              out += assembleRow(leftRow, conf.leftFieldCount, conf.rightFields, null, null)
            }
          } else {
            // Late materialization: point-fetch the surviving right rows by `_rowid`. Building the
            // `rowAddr -> row` map collapses any duplicate rowAddr to one payload; the loop below
            // still emits one output row per surviving ref.
            val materialized: Map[Long, Map[String, Any]] = probe
              .materialize(trimmed.iterator.map(_.rowAddr).toSeq, conf.rightProjection)
              .map(m => extractRowAddr(m) -> m)
              .toMap
            trimmed.foreach { ref =>
              val rightMap = materialized.getOrElse(ref.rowAddr, null)
              out += assembleRow(
                leftRow,
                conf.leftFieldCount,
                conf.rightFields,
                rightMap,
                ref.score)
            }
          }
        }
      }
    } finally probe.close()
    out.iterator
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
   * join with no hit) fills the right side with nulls.
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
      arr(leftFieldCount + j) =
        if (rightValues == null) null else rightValues.getOrElse(rightFields(j).name, null)
      j += 1
    }
    arr(leftFieldCount + rightFields.size) = score
    Row.fromSeq(arr.toSeq)
  }
}
