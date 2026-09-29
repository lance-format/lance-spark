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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarBinaryVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{BinaryType, LongType, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Unit tests for [[ArrowBatchChunker]]: the byte-budget and row-count boundaries that split a
 * row stream into narrow Arrow batches, each under Arrow's 32-bit-offset (2 GiB) cap. Pure JVM
 * (no SparkSession, no Lance native library).
 */
class ArrowBatchChunkerSuite {

  private val binarySchema = new StructType()
    .add("value", BinaryType, nullable = false)
    .add("id", LongType, nullable = false)

  private val longSchema = new StructType()
    .add("value", LongType, nullable = false)
    .add("id", LongType, nullable = false)

  private def binaryRow(id: Long, size: Int): GenericInternalRow =
    new GenericInternalRow(Array[Any](Array.fill[Byte](size)(id.toByte), id))

  private def longRow(id: Long): GenericInternalRow =
    new GenericInternalRow(Array[Any](id, id))

  // Per-batch row counts and the flattened ids across all batches, in emission order.
  private def drain(batches: Array[VectorSchemaRoot]): (List[Int], List[Long]) = {
    val counts = batches.map(_.getRowCount).toList
    val ids = ArrayBuffer[Long]()
    batches.foreach { root =>
      val idVec = root.getVector("id").asInstanceOf[BigIntVector]
      var r = 0
      while (r < root.getRowCount) {
        ids += idVec.get(r)
        r += 1
      }
    }
    (counts, ids.toList)
  }

  @Test
  def splitsBinaryAtByteBudget(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val chunker = new ArrowBatchChunker(binarySchema, allocator, maxBatchBytes = 250L)
    try {
      (0 until 6).foreach(i => chunker.write(binaryRow(i.toLong, 100)))
      val batches = chunker.finish()
      try {
        val (counts, ids) = drain(batches)
        assertEquals(List(2, 2, 2), counts, "250B budget holds two 100B rows per batch")
        assertEquals((0L until 6L).toList, ids, "rows must survive in order")
        assertTrue(
          batches.forall(_.getVector("value").isInstanceOf[VarBinaryVector]),
          "value column stays narrow Binary (no widening, no narrowing)")
      } finally {
        batches.foreach(_.close())
      }
    } finally {
      allocator.close()
    }
  }

  @Test
  def singleBatchWhenUnderBudget(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val chunker = new ArrowBatchChunker(binarySchema, allocator, maxBatchBytes = 10000L)
    try {
      (0 until 6).foreach(i => chunker.write(binaryRow(i.toLong, 100)))
      val batches = chunker.finish()
      try {
        val (counts, ids) = drain(batches)
        assertEquals(List(6), counts)
        assertEquals((0L until 6L).toList, ids)
      } finally {
        batches.foreach(_.close())
      }
    } finally {
      allocator.close()
    }
  }

  @Test
  def isolatesOverBudgetRow(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val chunker = new ArrowBatchChunker(binarySchema, allocator, maxBatchBytes = 250L)
    try {
      chunker.write(binaryRow(0L, 300)) // over budget -> its own batch
      chunker.write(binaryRow(1L, 100))
      chunker.write(binaryRow(2L, 100))
      val batches = chunker.finish()
      try {
        val (counts, ids) = drain(batches)
        assertEquals(List(1, 2), counts)
        assertEquals(List(0L, 1L, 2L), ids)
      } finally {
        batches.foreach(_.close())
      }
    } finally {
      allocator.close()
    }
  }

  @Test
  def chunksFixedWidthByRowCap(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    // No var-width column, so only the row-count ceiling can roll. perRowFixedCost = 8 (BigInt),
    // so maxBatchBytes=16 yields maxRows=2.
    val chunker = new ArrowBatchChunker(longSchema, allocator, maxBatchBytes = 16L)
    try {
      (0 until 5).foreach(i => chunker.write(longRow(i.toLong)))
      val batches = chunker.finish()
      try {
        val (counts, ids) = drain(batches)
        assertEquals(List(2, 2, 1), counts)
        assertEquals((0L until 5L).toList, ids)
      } finally {
        batches.foreach(_.close())
      }
    } finally {
      allocator.close()
    }
  }

  @Test
  def emptyWhenNoRows(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val chunker = new ArrowBatchChunker(binarySchema, allocator)
    try {
      assertEquals(0, chunker.finish().length)
    } finally {
      allocator.close()
    }
  }

  @Test
  def writeAfterFinishThrows(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val chunker = new ArrowBatchChunker(binarySchema, allocator)
    try {
      chunker.finish()
      assertThrows(
        classOf[IllegalArgumentException],
        () => chunker.write(binaryRow(0L, 1)))
    } finally {
      allocator.close()
    }
  }
}
