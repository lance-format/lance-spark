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
package org.lance.spark.arrow

import org.apache.arrow.vector.LargeVarBinaryVector
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.lance.spark.utils.{BlobReference, BlobReferenceResolver}

/**
 * Writer for binary columns backed by a [[LargeVarBinaryVector]].
 *
 * When a blob column flows through a Spark shuffle, its values arrive as serialized
 * [[BlobReference]]s rather than the actual bytes. This writer detects those and resolves them to
 * real blob bytes via the injected (shared, per-write-task) [[BlobReferenceResolver]].
 *
 * Buffering is lazy: rows are written straight to the vector in ascending order until the first blob
 * reference is seen, after which every subsequent row is buffered and the tail is emitted in a
 * single ascending pass in [[finish]]. Buffering only the tail is required for correctness once
 * references are present: resolving references produces bytes for arbitrary, non-contiguous indices,
 * and writing into the middle of an already-populated variable-width Arrow vector corrupts its
 * offset buffer (`setBytes` reads the start offset from the entry being overwritten and only
 * rewrites the next offset, shifting every following row's bytes). The common case — a binary column
 * with no shuffled references — buffers nothing and writes directly.
 *
 * Resolved blobs can be orders of magnitude larger than the buffered references, so the buffered
 * tail must stay bounded by bytes, not just row count: [[estimatedBufferedBytes]] reports the
 * resolved size carried by each reference, which the write buffer adds to its per-batch byte budget
 * so the batch flushes (and this tail resolves) before materialization exceeds `maxBatchBytes`.
 */
private[arrow] class LargeBinaryWriter(
    val valueVector: LargeVarBinaryVector,
    injectedResolver: BlobReferenceResolver) extends LanceArrowFieldWriter {

  // Buffered tail entries, in row order, starting at absolute row index `bufferStart`. Each is:
  //   null          -> SQL NULL (validity bit left unset)
  //   Array[Byte]   -> literal binary (possibly empty)
  //   BlobReference -> a reference to resolve to actual blob bytes
  // `bufferStart` is the absolute index of the first buffered row, or -1 while still writing direct.
  private val entries = new java.util.ArrayList[AnyRef]()
  private var bufferStart = -1

  // Sum of the bytes the buffered tail will occupy in the vector once emitted in `finish`: each
  // reference's resolved blob size (carried in the reference) plus each buffered literal's length.
  // The write buffer reads this via `estimatedBufferedBytes` to budget against maxBatchBytes — the
  // buffered references are tiny (~200 bytes) but resolve to potentially huge blobs, so without this
  // the byte guard sizes the references and the batch can balloon to tens of GB at resolution time.
  private var pendingBytes = 0L

  // Only created when no resolver is injected (e.g. non-shuffle build paths). Owned and closed here.
  private var localResolver: BlobReferenceResolver = _

  private def resolver: BlobReferenceResolver = {
    if (injectedResolver != null) {
      injectedResolver
    } else {
      if (localResolver == null) {
        localResolver = new BlobReferenceResolver()
      }
      localResolver
    }
  }

  // `count` (from the base class) is the absolute index of the row currently being written.
  override def setNull(): Unit = {
    if (bufferStart >= 0) {
      entries.add(null)
    }
    // Direct mode: leave the validity bit unset; setValueCount fills the offset hole on finish.
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    if (bytes != null && BlobReference.isBlobReference(bytes)) {
      // First reference flips us into buffering mode, starting at this row.
      if (bufferStart < 0) {
        bufferStart = count
      }
      val ref = BlobReference.deserialize(bytes)
      entries.add(ref)
      pendingBytes += ref.getSize
    } else if (bufferStart >= 0) {
      // Buffering mode: defer literals (and null bytes) so the whole tail emits in one pass.
      entries.add(bytes)
      if (bytes != null) pendingBytes += bytes.length
    } else if (bytes != null) {
      // Direct mode: write literals straight through in ascending order, no buffering needed.
      valueVector.setSafe(count, bytes)
    }
    // Direct mode + null bytes: leave the validity bit unset (SQL null), same as setNull.
  }

  // Resolved blob bytes (plus deferred literals) the buffered tail will add to the vector on finish.
  override def estimatedBufferedBytes: Long = pendingBytes

  override def finish(): Unit = {
    try {
      if (bufferStart >= 0) {
        val resolved: java.util.Map[Integer, Array[Byte]] = resolveReferences()

        // Single ascending pass over the buffered tail (rows bufferStart .. count-1).
        var j = 0
        while (j < entries.size()) {
          val rowId = bufferStart + j
          entries.get(j) match {
            case null => // SQL NULL: leave the validity bit unset
            case _: BlobReference =>
              val data = resolved.get(rowId)
              valueVector.setSafe(rowId, if (data != null) data else Array.emptyByteArray)
            case bytes: Array[Byte] =>
              valueVector.setSafe(rowId, bytes)
            case other =>
              throw new IllegalStateException(s"Unexpected buffered binary entry: $other")
          }
          j += 1
        }
      }
      super.finish()
    } finally {
      entries.clear()
      bufferStart = -1
      pendingBytes = 0L
      if (localResolver != null) {
        localResolver.close()
        localResolver = null
      }
    }
  }

  /** Collects the buffered references and resolves them to bytes keyed by their absolute row index. */
  private def resolveReferences(): java.util.Map[Integer, Array[Byte]] = {
    val indices = new java.util.ArrayList[Integer]()
    val refs = new java.util.ArrayList[BlobReference]()
    var j = 0
    while (j < entries.size()) {
      entries.get(j) match {
        case ref: BlobReference =>
          indices.add(bufferStart + j)
          refs.add(ref)
        case _ =>
      }
      j += 1
    }
    try {
      resolver.resolveBatch(indices, refs)
    } catch {
      case e: java.io.IOException =>
        throw new RuntimeException("Failed to resolve blob references", e)
    }
  }

  override def reset(): Unit = {
    super.reset()
    entries.clear()
    bufferStart = -1
    pendingBytes = 0L
  }
}
