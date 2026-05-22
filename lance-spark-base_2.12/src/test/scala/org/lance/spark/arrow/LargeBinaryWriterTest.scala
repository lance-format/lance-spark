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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.LargeVarBinaryVector
import org.apache.spark.sql.catalyst.InternalRow
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.spark.utils.{BlobReference, BlobReferenceResolver}

import java.nio.charset.StandardCharsets.UTF_8

/**
 * Regression tests for [[LargeBinaryWriter]]'s buffering and reference-resolution logic. These guard
 * the offset-buffer ordering contract (writes must land in ascending order) and the boundary between
 * direct writes and the buffered tail that begins at the first blob reference.
 *
 * Written with JUnit 5 (not ScalaTest) so surefire actually executes them.
 */
class LargeBinaryWriterTest {

  /** A resolver that records what it was asked to resolve and returns deterministic bytes. */
  private class RecordingResolver extends BlobReferenceResolver {
    var capturedIndices: java.util.List[Integer] = _
    var capturedRefs: java.util.List[BlobReference] = _

    override def resolveBatch(
        indices: java.util.List[Integer],
        refs: java.util.List[BlobReference]): java.util.Map[Integer, Array[Byte]] = {
      capturedIndices = indices
      capturedRefs = refs
      val out = new java.util.HashMap[Integer, Array[Byte]]()
      var i = 0
      while (i < indices.size()) {
        out.put(indices.get(i), resolvedBytes(refs.get(i).getRowAddress))
        i += 1
      }
      out
    }
  }

  private def resolvedBytes(addr: Long): Array[Byte] = s"resolved-$addr".getBytes(UTF_8)

  private def withVector(body: LargeVarBinaryVector => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new LargeVarBinaryVector("blob", allocator)
    vector.allocateNew()
    try body(vector)
    finally {
      vector.close()
      allocator.close()
    }
  }

  /** Builds a single-column binary row; `null` produces a SQL NULL. */
  private def row(bytes: Array[Byte]): InternalRow = InternalRow(bytes)

  private def ref(addr: Long): Array[Byte] =
    new BlobReference("file:///src.lance", "blob", addr).serialize()

  private def ref(addr: Long, size: Long): Array[Byte] =
    new BlobReference("file:///src.lance", "blob", addr, size).serialize()

  private def assertBytes(vector: LargeVarBinaryVector, index: Int, expected: Array[Byte]): Unit = {
    assertFalse(vector.isNull(index), s"row $index should not be null")
    assertArrayEquals(expected, vector.get(index), s"row $index bytes mismatch")
  }

  @Test
  def directWritesAscendingWhenNoReferences(): Unit = {
    withVector { vector =>
      val writer = new LargeBinaryWriter(vector, null)
      writer.write(row("a".getBytes(UTF_8)), 0)
      writer.write(row(null), 0)
      writer.write(row("bb".getBytes(UTF_8)), 0)
      writer.write(row(Array.emptyByteArray), 0)
      writer.write(row(null), 0)
      writer.finish()

      assertEquals(5, vector.getValueCount)
      assertBytes(vector, 0, "a".getBytes(UTF_8))
      assertTrue(vector.isNull(1))
      assertBytes(vector, 2, "bb".getBytes(UTF_8))
      assertBytes(vector, 3, Array.emptyByteArray) // empty is distinct from null
      assertTrue(vector.isNull(4))
    }
  }

  @Test
  def referencesBufferOnlyTheTailAndResolveAtRightIndices(): Unit = {
    withVector { vector =>
      val resolver = new RecordingResolver
      val writer = new LargeBinaryWriter(vector, resolver)
      writer.write(row("x".getBytes(UTF_8)), 0) // direct
      writer.write(row(ref(10)), 0) // first reference -> buffering starts at index 1
      writer.write(row("y".getBytes(UTF_8)), 0) // buffered literal
      writer.write(row(ref(20)), 0) // buffered reference
      writer.write(row(null), 0) // buffered null
      writer.finish()

      // References were collected in ascending order, with their absolute row indices, before
      // resolution — and only the references (not the interleaved literal/null) were handed over.
      assertEquals(java.util.Arrays.asList[Integer](1, 3), resolver.capturedIndices)
      assertEquals(2, resolver.capturedRefs.size())
      assertEquals(10L, resolver.capturedRefs.get(0).getRowAddress)
      assertEquals(20L, resolver.capturedRefs.get(1).getRowAddress)

      assertEquals(5, vector.getValueCount)
      assertBytes(vector, 0, "x".getBytes(UTF_8))
      assertBytes(vector, 1, resolvedBytes(10))
      assertBytes(vector, 2, "y".getBytes(UTF_8))
      assertBytes(vector, 3, resolvedBytes(20))
      assertTrue(vector.isNull(4))
    }
  }

  @Test
  def referenceInFirstRowBuffersWholeBatch(): Unit = {
    withVector { vector =>
      val resolver = new RecordingResolver
      val writer = new LargeBinaryWriter(vector, resolver)
      writer.write(row(ref(5)), 0)
      writer.write(row("z".getBytes(UTF_8)), 0)
      writer.finish()

      assertEquals(java.util.Arrays.asList[Integer](0), resolver.capturedIndices)
      assertBytes(vector, 0, resolvedBytes(5))
      assertBytes(vector, 1, "z".getBytes(UTF_8))
    }
  }

  @Test
  def ioExceptionDuringResolutionIsPropagatedAsRuntimeException(): Unit = {
    withVector { vector =>
      val resolver = new BlobReferenceResolver {
        override def resolveBatch(
            indices: java.util.List[Integer],
            refs: java.util.List[BlobReference]): java.util.Map[Integer, Array[Byte]] =
          throw new java.io.IOException("boom")
      }
      val writer = new LargeBinaryWriter(vector, resolver)
      writer.write(row(ref(1)), 0)

      val ex = assertThrows(classOf[RuntimeException], () => writer.finish())
      assertTrue(ex.getMessage.contains("Failed to resolve blob references"))
      assertTrue(ex.getCause.isInstanceOf[java.io.IOException])
    }
  }

  @Test
  def estimatedBufferedBytesTracksResolvedSizesNotReferenceSizes(): Unit = {
    withVector { vector =>
      val writer = new LargeBinaryWriter(vector, new RecordingResolver)
      // Direct mode (no references yet): nothing buffered, so the byte budget sees nothing extra.
      writer.write(row("x".getBytes(UTF_8)), 0)
      assertEquals(0L, writer.estimatedBufferedBytes)

      // First reference flips into buffering: the budget must reflect the resolved blob size
      // (1 MB here), not the ~200-byte reference that is actually buffered.
      writer.write(row(ref(10, 1024 * 1024)), 0)
      assertEquals(1024L * 1024, writer.estimatedBufferedBytes)

      // Buffered literals count their own length; a second reference adds its resolved size.
      writer.write(row("yz".getBytes(UTF_8)), 0)
      writer.write(row(ref(20, 512)), 0)
      writer.write(row(null), 0) // buffered null contributes nothing
      assertEquals(1024L * 1024 + 2 + 512, writer.estimatedBufferedBytes)

      // finish() drains the tail and clears the running total for the next batch.
      writer.finish()
      assertEquals(0L, writer.estimatedBufferedBytes)
    }
  }

  @Test
  def resetClearsBufferingStateForNextBatch(): Unit = {
    withVector { vector =>
      val resolver = new RecordingResolver
      val writer = new LargeBinaryWriter(vector, resolver)
      // First batch flips into buffering mode via a reference.
      writer.write(row(ref(7)), 0)
      writer.finish()
      assertBytes(vector, 0, resolvedBytes(7))

      // After reset, a reference-free batch must write directly (no stale buffer offset).
      writer.reset()
      resolver.capturedIndices = null
      writer.write(row("p".getBytes(UTF_8)), 0)
      writer.write(row("qq".getBytes(UTF_8)), 0)
      writer.finish()

      assertNull(resolver.capturedIndices, "resolver must not be invoked without references")
      assertEquals(2, vector.getValueCount)
      assertBytes(vector, 0, "p".getBytes(UTF_8))
      assertBytes(vector, 1, "qq".getBytes(UTF_8))
    }
  }
}
