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
package org.lance.spark.write

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}
import org.apache.arrow.vector.util.OversizedAllocationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.LanceArrowUtils
import org.lance.spark.arrow.LanceArrowWriter

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Accumulates [[InternalRow]]s into a sequence of narrow Arrow [[VectorSchemaRoot]] batches,
 * rolling to a new batch before any variable-width (Utf8/Binary) column would push its data
 * buffer past Arrow's 32-bit-offset 2 GiB cap. The whole partition would otherwise be buffered
 * into a single `VectorSchemaRoot` and overflow that cap the moment a value column exceeds 2 GiB;
 * splitting the same rows across capped batches keeps peak memory unchanged while staying within
 * the cap. Index-type agnostic — any build that pre-processes rows into an Arrow stream can use
 * it.
 *
 * Drive it with one [[write]] per row, then [[finish]] to get the batches; on the error path call
 * [[close]]. The caller owns and must close the returned batches.
 *
 * @param sparkSchema   schema of the rows written (Arrow batches use the narrow encoding of it)
 * @param allocator     allocator for the batch vectors
 * @param maxBatchBytes soft per-column data-buffer budget; default just under the 2 GiB hard cap
 */
class ArrowBatchChunker(
    sparkSchema: StructType,
    allocator: BufferAllocator,
    maxBatchBytes: Long = ArrowBatchChunker.DefaultMaxBatchBytes) {

  require(maxBatchBytes > 0, s"maxBatchBytes must be positive, got $maxBatchBytes")

  /** Narrow Arrow schema every emitted batch uses. */
  val arrowSchema: Schema =
    LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", errorOnDuplicatedFieldNames = false)

  // Ordinals of narrow var-width columns (Utf8/Binary) subject to the 32-bit data-buffer cap,
  // each tagged Utf8 (string) vs Binary so the per-row byte size is read from the right accessor.
  // 64-bit Large* columns (blob / large_var_char) and fixed-width columns are excluded: Large*
  // offsets don't hit the 32-bit cap, and fixed-width buffers are bounded by maxRows below.
  private val (cappedOrdinals, cappedIsString): (Array[Int], Array[Boolean]) = {
    val ords = ArrayBuffer[Int]()
    val isStr = ArrayBuffer[Boolean]()
    arrowSchema.getFields.asScala.zipWithIndex.foreach {
      case (field, i) =>
        field.getType match {
          case _: ArrowType.Utf8 => ords += i; isStr += true
          case _: ArrowType.Binary => ords += i; isStr += false
          case _ =>
        }
    }
    (ords.toArray, isStr.toArray)
  }

  // Cap each batch's row count so fixed-width buffers and var-width offset/validity buffers also
  // stay under the 32-bit cap, not just the var-width data buffers. perRowFixedCost is the widest
  // per-row fixed contribution across columns (e.g. an 8-byte row id, a 4-byte narrow offset).
  private val maxRows: Int = {
    val perRowFixedCost = math.max(
      1L,
      arrowSchema.getFields.asScala
        .map(f => ArrowBatchChunker.fixedWidthBytes(f.getType).toLong)
        .max)
    math.max(1, (maxBatchBytes / perRowFixedCost).min(Int.MaxValue.toLong).toInt)
  }

  private val batches = ArrayBuffer[VectorSchemaRoot]()
  private var current: VectorSchemaRoot = _
  private var writer: LanceArrowWriter = _
  private var rowCount: Int = 0
  private val running: Array[Long] = new Array[Long](cappedOrdinals.length)
  private var finished: Boolean = false

  private def startBatch(): Unit = {
    current = VectorSchemaRoot.create(arrowSchema, allocator)
    writer = LanceArrowWriter.create(current, sparkSchema)
    rowCount = 0
    java.util.Arrays.fill(running, 0L)
  }

  private def sealBatch(): Unit = {
    writer.finish()
    batches += current
    current = null
    writer = null
  }

  // Var-width byte size this row contributes to each capped column (0 for nulls).
  private def rowVarBytes(row: InternalRow): Array[Long] = {
    val out = new Array[Long](cappedOrdinals.length)
    var k = 0
    while (k < cappedOrdinals.length) {
      val ord = cappedOrdinals(k)
      out(k) =
        if (row.isNullAt(ord)) 0L
        else if (cappedIsString(k)) row.getUTF8String(ord).numBytes().toLong
        else row.getBinary(ord).length.toLong
      k += 1
    }
    out
  }

  /**
   * Append one row, first rolling to a new batch if it would push a capped column's data buffer
   * past `maxBatchBytes` or the batch past `maxRows`. A single value larger than the 32-bit cap
   * can never fit a narrow vector, so it fails fast rather than overflowing to a negative offset.
   */
  def write(row: InternalRow): Unit = {
    require(!finished, "write called after finish")
    if (current == null) {
      startBatch()
    }

    val rowBytes = rowVarBytes(row)
    var k = 0
    while (k < rowBytes.length) {
      if (rowBytes(k) > ArrowBatchChunker.HardCapBytes) {
        throw new OversizedAllocationException(
          s"row holds ${rowBytes(k)} bytes in column " +
            s"'${arrowSchema.getFields.get(cappedOrdinals(k)).getName}', over the " +
            s"${ArrowBatchChunker.HardCapBytes}-byte limit of a 32-bit-offset Arrow vector; " +
            "a single index value cannot exceed 2 GiB")
      }
      k += 1
    }

    var wouldExceedBytes = false
    var i = 0
    while (i < running.length && !wouldExceedBytes) {
      if (running(i) + rowBytes(i) > maxBatchBytes) {
        wouldExceedBytes = true
      }
      i += 1
    }

    if (rowCount > 0 && (wouldExceedBytes || rowCount >= maxRows)) {
      sealBatch()
      startBatch()
    }

    writer.write(row)
    rowCount += 1
    var j = 0
    while (j < running.length) {
      running(j) += rowBytes(j)
      j += 1
    }
  }

  /**
   * Seal the final batch and return all batches, each within budget. Empty if no rows were
   * written. The caller owns the returned roots and must close them.
   */
  def finish(): Array[VectorSchemaRoot] = {
    require(!finished, "finish called twice")
    finished = true
    if (current != null) {
      if (rowCount > 0) {
        sealBatch()
      } else {
        current.close()
        current = null
        writer = null
      }
    }
    batches.toArray
  }

  /** Close the in-progress batch and every sealed batch. Use on the error path before rethrow. */
  def close(): Unit = {
    if (current != null) {
      current.close()
      current = null
      writer = null
    }
    batches.foreach(_.close())
    batches.clear()
  }
}

object ArrowBatchChunker {

  /** A 32-bit-offset Arrow vector addresses at most `Integer.MAX_VALUE` data bytes. */
  val HardCapBytes: Long = Integer.MAX_VALUE.toLong

  /**
   * Default soft budget per batch: just under the 2 GiB hard cap, leaving headroom so one more
   * typical row won't cross it before the chunker rolls.
   */
  val DefaultMaxBatchBytes: Long = HardCapBytes - (64L << 20)

  // Per-row byte cost of a column's fixed-width buffers (data for fixed types, offset slot for
  // var types) — used only to bound rows per batch so non-data buffers stay under the 32-bit cap.
  private def fixedWidthBytes(t: ArrowType): Int = t match {
    case _: ArrowType.Utf8 | _: ArrowType.Binary => 4
    case _: ArrowType.LargeUtf8 | _: ArrowType.LargeBinary => 8
    case i: ArrowType.Int => math.max(1, i.getBitWidth / 8)
    case fp: ArrowType.FloatingPoint =>
      fp.getPrecision match {
        case FloatingPointPrecision.HALF => 2
        case FloatingPointPrecision.SINGLE => 4
        case FloatingPointPrecision.DOUBLE => 8
      }
    case _: ArrowType.Bool => 1
    case d: ArrowType.Decimal => math.max(1, d.getBitWidth / 8)
    case _: ArrowType.Date => 8
    case _: ArrowType.Timestamp => 8
    case fsb: ArrowType.FixedSizeBinary => math.max(1, fsb.getByteWidth)
    case _ => 8
  }
}
