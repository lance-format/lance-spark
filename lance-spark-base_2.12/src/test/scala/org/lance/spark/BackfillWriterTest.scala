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
package org.lance.spark

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.vector.UInt8Vector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, WriterCommitMessage}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._
import org.lance.Fragment
import org.lance.spark.write.{AbstractBackfillWriter, AddColumnsBackfillWrite, UpdateColumnsBackfillWrite}

import java.util.Collections

import scala.collection.mutable.ArrayBuffer

@Timeout(30)
class BackfillWriterTest {
  private val schema = new StructType()
    .add("_fragid", IntegerType, nullable = false)
    .add("ignored", IntegerType)
    .add("payload", ArrayType(new StructType().add("label", StringType)))
    .add("_rowaddr", LongType, nullable = false)

  private def options(batchSize: Int = 2, maxBytes: Long = Long.MaxValue) =
    LanceSparkWriteOptions.builder()
      .datasetUri(TestUtils.TestTable1Config.datasetUri)
      .batchSize(batchSize)
      .maxBatchBytes(maxBytes)
      .build()

  private def row(fragment: Int, offset: Int, label: String = "value"): InternalRow =
    InternalRow(
      fragment,
      -1,
      new GenericArrayData(Array[Any](InternalRow(UTF8String.fromString(label)))),
      (fragment.toLong << 32) + offset)

  private class RecordingWriter(writeOptions: LanceSparkWriteOptions)
    extends AbstractBackfillWriter(
      writeOptions,
      schema,
      Collections.singletonList("payload"),
      null,
      null,
      null,
      null) {
    val completedFragments = new ArrayBuffer[Int]
    val batchSizes = new ArrayBuffer[Int]
    val addresses = new ArrayBuffer[Long]
    val payloads = new ArrayBuffer[String]

    override protected def processFragment(fragment: Fragment, stream: ArrowArrayStream): Unit = {
      val reader = Data.importArrayStream(LanceRuntime.allocator(), stream)
      try {
        while (reader.loadNextBatch()) {
          val root = reader.getVectorSchemaRoot
          assertEquals(2, root.getFieldVectors.size())
          assertNull(root.getVector("ignored"))
          assertNull(root.getVector("_fragid"))
          batchSizes += root.getRowCount
          val rowAddresses = root.getVector("_rowaddr").asInstanceOf[UInt8Vector]
          (0 until root.getRowCount).foreach { i =>
            addresses += rowAddresses.get(i)
            payloads += Option(root.getVector("payload").getObject(i)).map(_.toString).orNull
          }
        }
      } finally reader.close()
      completedFragments += fragment.getId
    }

    override protected def buildCommitMessage(): WriterCommitMessage = new WriterCommitMessage {}
  }

  @Test
  def respectsBatchSizeForNestedStrings(): Unit = {
    val writer = new RecordingWriter(options())
    try {
      (0 until 5).foreach(i => writer.write(row(0, i, s"label-$i")))
      writer.commit()
      assertEquals(Seq(2, 2, 1), writer.batchSizes.toSeq)
      assertEquals((0L until 5L), writer.addresses.toSeq)
      writer.payloads.zipWithIndex.foreach { case (payload, i) =>
        assertTrue(payload.contains(s"label-$i"))
      }
    } finally writer.close()
  }

  @Test
  def respectsByteBudgetForNestedStrings(): Unit = {
    val writer = new RecordingWriter(options(batchSize = 1000, maxBytes = 128))
    try {
      (0 until 5).foreach(i => writer.write(row(0, i, "x" * 1024)))
      writer.commit()
      assertTrue(writer.batchSizes.size > 1, "The byte budget must split a single fragment")
      assertEquals((0L until 5L), writer.addresses.toSeq)
      assertTrue(writer.payloads.forall(_.contains("x" * 1024)))
    } finally writer.close()
  }

  @Test
  def finishesPreviousFragmentBeforeTaskCommit(): Unit = {
    val writer = new RecordingWriter(options())
    try {
      writer.write(row(0, 0))
      writer.write(row(0, 1))
      writer.write(row(1, 0))
      assertEquals(Seq(0), writer.completedFragments.toSeq)
      writer.commit()
      assertEquals(Seq(0, 1), writer.completedFragments.toSeq)
      assertEquals(Seq(0L, 1L, 1L << 32), writer.addresses.toSeq)
    } finally writer.close()
  }

  @Test
  def preservesNullAndEmptyArraysAcrossBatches(): Unit = {
    val writer = new RecordingWriter(options())
    try {
      writer.write(InternalRow(0, -1, null, 0L))
      writer.write(InternalRow(0, -1, new GenericArrayData(Array.empty[Any]), 1L))
      writer.write(row(0, 2, "tail"))
      writer.commit()
      assertEquals(Seq(2, 1), writer.batchSizes.toSeq)
      assertNull(writer.payloads(0))
      assertEquals("[]", writer.payloads(1))
      assertTrue(writer.payloads(2).contains("tail"))
    } finally writer.close()
  }

  @Test
  def closesWithAnInterruptedTaskThread(): Unit = {
    val writer = new RecordingWriter(options(batchSize = 1000))
    writer.write(row(0, 0))
    Thread.currentThread().interrupt()
    try {
      writer.close()
      assertTrue(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
      writer.close()
    }
  }

  @Test
  def emptyInputDoesNotProcessAFragment(): Unit = {
    val writer = new RecordingWriter(options())
    try {
      writer.commit()
      assertTrue(writer.completedFragments.isEmpty)
      assertTrue(writer.batchSizes.isEmpty)
    } finally writer.close()
  }

  @Test
  def closesAnUnfinishedBatch(): Unit = {
    val before = LanceRuntime.allocator().getAllocatedMemory
    val writer = new RecordingWriter(options(batchSize = 1000))
    writer.write(row(0, 0))
    writer.abort()
    writer.close()
    assertEquals(before, LanceRuntime.allocator().getAllocatedMemory)
  }

  @Test
  def propagatesConsumerFailure(): Unit = {
    val writer = new RecordingWriter(options()) {
      override protected def processFragment(fragment: Fragment, stream: ArrowArrayStream): Unit =
        throw new IllegalStateException("injected consumer failure")
    }
    try {
      val error = assertThrows(
        classOf[Exception],
        () => {
          writer.write(row(0, 0))
          writer.commit()
          ()
        })
      val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
      assertTrue(causes.exists(_.getMessage == "injected consumer failure"))
    } finally writer.close()
  }

  @Test
  def requestsFragmentOrderingForBothOperations(): Unit = {
    val add = new AddColumnsBackfillWrite.AddColumnsWriteBuilder(
      schema,
      options(),
      Collections.singletonList("payload"),
      null,
      null,
      null,
      null,
      false)
      .build().asInstanceOf[RequiresDistributionAndOrdering]
    val update = new UpdateColumnsBackfillWrite.UpdateColumnsWriteBuilder(
      schema,
      options(),
      Collections.singletonList("payload"),
      null,
      null,
      null,
      null,
      false)
      .build().asInstanceOf[RequiresDistributionAndOrdering]
    Seq(add, update).foreach { write =>
      assertEquals(1, write.requiredOrdering().length)
      assertEquals("_fragid", write.requiredOrdering()(0).expression().describe())
    }
  }
}
