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
package org.lance.spark.read

import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.spark.{LanceConstant, LanceRuntime}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

import scala.collection.mutable.ArrayBuffer

class LanceSampleReaderTest {
  private def partition(
      schema: StructType,
      positions: Array[Long],
      batchSize: Int = 3): LanceSampleInputPartition = {
    LanceSampleTestProcess.partition(schema, positions, batchSize)
  }

  @Test
  def serializedTaskReplayPreservesRowsAcrossFragments(): Unit = {
    assertEquals(
      1L,
      ObjectStreamClass.lookup(classOf[LanceSampleInputPartition]).getSerialVersionUID)
    val schema = new StructType().add("x", DataTypes.LongType)
      .add(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType)
      .add(LanceConstant.ROW_ADDRESS, DataTypes.LongType)
    val original = partition(schema, Array[Long](0, 1, 2, 3))
    // The cross-compiled base module does not include the Java TestUtils sources.
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(original)
    finally output.close()
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val replay =
      try input.readObject().asInstanceOf[LanceSampleInputPartition]
      finally input.close()
    def collect(input: LanceSampleInputPartition): Seq[(Long, Int, Long)] = {
      val reader = new LanceSamplePartitionReader(input)
      val rows = ArrayBuffer.empty[(Long, Int, Long)]
      try {
        while (reader.next()) {
          val batch = reader.get()
          for (row <- 0 until batch.numRows()) {
            rows += ((
              batch.column(0).getLong(row),
              batch.column(1).getInt(row),
              batch.column(2).getLong(row)))
          }
        }
        val metrics = reader.currentMetricsValues().map(m => m.name() -> m.value()).toMap
        assertEquals(2L, metrics("numTakeCalls"))
        assertEquals(4L, metrics("numRowsRequested"))
        assertEquals(4L, metrics("numRowsReturned"))
        rows.toSeq
      } finally reader.close()
    }
    val expected = Seq((0L, 0, 0L), (1L, 0, 1L), (2L, 1, 1L << 32), (3L, 1, (1L << 32) + 1L))
    assertEquals(expected, collect(original))
    assertEquals(expected, collect(replay))
  }

  @Test
  def earlyCloseReleasesArrowBuffersAndDoesNotReadRemainingRequests(): Unit = {
    val input = partition(new StructType().add("x", DataTypes.LongType), Array[Long](0, 1, 2, 3), 1)
    val before = LanceRuntime.allocator().getAllocatedMemory
    val reader = new LanceSamplePartitionReader(input)
    try {
      assertTrue(reader.next())
      assertEquals(0L, reader.get().column(0).getLong(0))
      assertTrue(LanceRuntime.allocator().getAllocatedMemory > before)
    } finally reader.close()
    reader.close()
    assertFalse(reader.next())
    assertEquals(before, LanceRuntime.allocator().getAllocatedMemory)
    val calls = reader.currentMetricsValues().find(_.name() == "numTakeCalls").get.value()
    assertEquals(1L, calls)
  }

  @Test
  def emptyProjectionRetainsCardinalityAndRowFactoryReplaysPositions(): Unit = {
    val input = partition(new StructType(), Array[Long](1, 3), 1)
    val reader = new LanceSamplePartitionReader(input)
    var rows = 0
    try {
      while (reader.next()) {
        assertEquals(0, reader.get().numCols())
        rows += reader.get().numRows()
      }
    } finally reader.close()
    assertEquals(2, rows)

    val projected = partition(new StructType().add("x", DataTypes.LongType), Array[Long](1, 3), 1)
    val rowReader = new LanceSampleReaderFactory().createReader(projected)
    val values = ArrayBuffer.empty[Long]
    try {
      while (rowReader.next()) values += rowReader.get().getLong(0)
    } finally rowReader.close()
    assertEquals(Seq(1L, 3L), values.toSeq)
  }

  @Test
  def sparkTaskRetriesReplayTheSamePositions(): Unit = {
    val output = LanceSampleTestProcess.run("org.lance.spark.read.SampleRetryProbe")
    assertTrue(output.contains("RETRY_OK"), output)
  }

  @Test
  def exceptionBoundaryMatchesTheOrdinaryReader(): Unit = {
    val io = new IOException("io")
    assertSame(io, LanceColumnarPartitionReader.asIOException(io))
    val checked = new Exception("checked")
    assertSame(checked, LanceColumnarPartitionReader.asIOException(checked).getCause)
    val runtime = new IllegalStateException("runtime")
    assertSame(
      runtime,
      assertThrows(
        classOf[IllegalStateException],
        () =>
          LanceColumnarPartitionReader.asIOException(runtime)))
    val error = new AssertionError("error")
    assertSame(
      error,
      assertThrows(
        classOf[AssertionError],
        () =>
          LanceColumnarPartitionReader.asIOException(error)))
    for (method <- Seq("next", "close")) {
      assertTrue(classOf[LanceSamplePartitionReader].getMethod(method)
        .getExceptionTypes.contains(classOf[IOException]))
    }
  }
}
