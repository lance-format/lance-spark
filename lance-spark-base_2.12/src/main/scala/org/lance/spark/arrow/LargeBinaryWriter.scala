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
 * All per-row values are buffered and the vector is emitted in a single ascending pass in
 * [[finish]]. This ordering is required for correctness: resolving references produces bytes for
 * arbitrary, non-contiguous indices, and writing into the middle of an already-populated
 * variable-width Arrow vector corrupts its offset buffer (`setBytes` reads the start offset from the
 * entry being overwritten and only rewrites the next offset, shifting every following row's bytes).
 * Buffering one batch's values is bounded by the batch size.
 */
private[arrow] class LargeBinaryWriter(
    val valueVector: LargeVarBinaryVector,
    injectedResolver: BlobReferenceResolver) extends LanceArrowFieldWriter {

  // One buffered entry per row, in row order. Each is one of:
  //   null          -> SQL NULL (validity bit left unset)
  //   Array[Byte]   -> literal binary (possibly empty)
  //   BlobReference -> a reference to resolve to actual blob bytes
  private val entries = new java.util.ArrayList[AnyRef]()
  private var hasRefs = false

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

  override def setNull(): Unit = entries.add(null)

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    if (bytes != null && BlobReference.isBlobReference(bytes)) {
      entries.add(BlobReference.deserialize(bytes))
      hasRefs = true
    } else {
      entries.add(bytes)
    }
  }

  override def finish(): Unit = {
    try {
      val resolved: java.util.Map[Integer, Array[Byte]] =
        if (hasRefs) resolveReferences() else java.util.Collections.emptyMap()

      // Single ascending pass over the batch: write literals and resolved references in order.
      var i = 0
      while (i < entries.size()) {
        entries.get(i) match {
          case null => // SQL NULL: leave the validity bit unset
          case _: BlobReference =>
            val data = resolved.get(i)
            valueVector.setSafe(i, if (data != null) data else Array.emptyByteArray)
          case bytes: Array[Byte] =>
            valueVector.setSafe(i, bytes)
          case other =>
            throw new IllegalStateException(s"Unexpected buffered binary entry: $other")
        }
        i += 1
      }
      super.finish()
    } finally {
      entries.clear()
      hasRefs = false
      if (localResolver != null) {
        localResolver.close()
        localResolver = null
      }
    }
  }

  /** Collects the buffered references and resolves them to bytes keyed by their row index. */
  private def resolveReferences(): java.util.Map[Integer, Array[Byte]] = {
    val indices = new java.util.ArrayList[Integer]()
    val refs = new java.util.ArrayList[BlobReference]()
    var i = 0
    while (i < entries.size()) {
      entries.get(i) match {
        case ref: BlobReference =>
          indices.add(i)
          refs.add(ref)
        case _ =>
      }
      i += 1
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
    hasRefs = false
  }
}
