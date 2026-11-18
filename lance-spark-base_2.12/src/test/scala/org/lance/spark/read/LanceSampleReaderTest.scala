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

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.spark.{LanceConstant, LanceRef, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.utils.Utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Collections

import scala.collection.mutable.ArrayBuffer

class LanceSampleReaderTest {
  private def partition(
      schema: StructType,
      positions: Array[Long],
      batchSize: Int = 3): LanceSampleInputPartition = {
    val uri = getClass.getResource("/example_db/test_dataset1.lance").toString
    val dataset = Utils.openDatasetBuilder(LanceSparkReadOptions.from(uri)).build()
    val version =
      try dataset.version()
      finally dataset.close()
    val options = LanceSparkReadOptions.builder()
      .datasetUri(uri)
      .ref(LanceRef.ofMain(version))
      .sampleSize(positions.length.toLong)
      .sampleSeed(42L)
      .sampleBatchSize(batchSize)
      .build()
    LanceSampleInputPartition(
      schema,
      options,
      positions,
      Collections.emptyMap[String, String](),
      null,
      Collections.emptyMap[String, String]())
  }

  @Test
  def serializedTaskReplayPreservesRowsAcrossFragments(): Unit = {
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
    val input = partition(new StructType().add("x", DataTypes.LongType), Array[Long](1, 3), 1)
    val context = new SparkContext(new SparkConf()
      .setAppName("exact-sampling-retry-test")
      .setMaster("local[1,2]")
      .set("spark.ui.enabled", "false"))
    try {
      val replayed = context.parallelize(Seq(input), 1).mapPartitions { inputs =>
        inputs.flatMap { partition =>
          val reader = new LanceSamplePartitionReader(partition)
          val values = ArrayBuffer.empty[Long]
          try {
            while (reader.next()) {
              val batch = reader.get()
              for (row <- 0 until batch.numRows()) values += batch.column(0).getLong(row)
            }
          } finally reader.close()
          val attempt = TaskContext.get().attemptNumber()
          if (attempt == 0)
            throw new IllegalStateException("Fail after consuming the first attempt")
          values.iterator.map(value => (value, attempt))
        }
      }.collect().toSeq
      assertEquals(Seq((1L, 1), (3L, 1)), replayed)
    } finally context.stop()
  }
}
