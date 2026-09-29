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
import org.apache.spark.sql.types.{BinaryType, LongType, StructType}
import org.apache.spark.sql.util.LanceArrowUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Unit tests for [[ChunkedArrowReader]]: emits each pre-built batch once, in order, then reports
 * exhaustion. Pure JVM (no SparkSession, no Lance native library, no Arrow C interface).
 */
class ChunkedArrowReaderSuite {

  private val sparkSchema = new StructType()
    .add("value", BinaryType, nullable = false)
    .add("id", LongType, nullable = false)

  private def newBatch(allocator: RootAllocator, ids: Seq[Long]): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(
      LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false),
      allocator)
    val value = root.getVector("value").asInstanceOf[VarBinaryVector]
    val id = root.getVector("id").asInstanceOf[BigIntVector]
    value.allocateNew()
    id.allocateNew()
    ids.zipWithIndex.foreach {
      case (v, i) =>
        value.setSafe(i, Array.fill[Byte](1)(v.toByte))
        id.setSafe(i, v)
    }
    root.setRowCount(ids.length)
    root
  }

  @Test
  def emitsEachBatchInOrder(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val schema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val batches = Array(
      newBatch(allocator, Seq(0L, 1L)),
      newBatch(allocator, Seq(2L, 3L)),
      newBatch(allocator, Seq(4L)))
    val reader = new ChunkedArrowReader(allocator, schema, batches)
    try {
      val counts = ArrayBuffer[Int]()
      val ids = ArrayBuffer[Long]()
      while (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        counts += root.getRowCount
        val idVec = root.getVector("id").asInstanceOf[BigIntVector]
        var r = 0
        while (r < root.getRowCount) {
          ids += idVec.get(r)
          r += 1
        }
      }
      assertEquals(List(2, 2, 1), counts.toList)
      assertEquals(List(0L, 1L, 2L, 3L, 4L), ids.toList)
      assertFalse(reader.loadNextBatch(), "an exhausted reader stays exhausted")
    } finally {
      reader.close()
      batches.foreach(_.close())
      allocator.close()
    }
  }

  @Test
  def emptyReaderLoadsNoBatches(): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val schema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val reader = new ChunkedArrowReader(allocator, schema, Array.empty[VectorSchemaRoot])
    try {
      assertFalse(reader.loadNextBatch())
    } finally {
      reader.close()
      allocator.close()
    }
  }
}
