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
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.distributions.ClusteredDistribution
import org.apache.spark.sql.connector.expressions.SortDirection
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, WriterCommitMessage}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._
import org.lance.{Dataset, Fragment, FragmentMetadata, FragmentOperation, WriteParams}
import org.lance.spark.write.{AbstractBackfillWriter, AddColumnsBackfillWrite, UpdateColumnsBackfillWrite}

import java.io.IOException
import java.nio.file.{Files, FileVisitResult, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import java.util.{Collections, Optional}

import scala.collection.mutable.ArrayBuffer

/**
 * Locks the streaming contract of the column backfill writers: fragments arrive ordered by
 * `_fragid` (clustered and sorted), and a task must release each finished fragment's Arrow buffers
 * before it starts buffering the next one. An implementation that accumulates every fragment's
 * Arrow buffer until commit grows linearly with the task's fragment count and OOMs executors on
 * large backfills.
 */
@Timeout(120)
class BackfillMemoryTest {
  private val fragmentCount = 40
  private val rowsPerFragment = 2
  private val structsPerRow = 128
  private val labelLength = 2048

  /**
   * Per fragment: 2 rows x 128 structs x 2048-byte label ~= 512 KiB of Arrow string data, so a
   * task that buffers all 40 fragments linearly holds ~20 MiB. The streaming writer must stay one
   * fragment (and one batch) at a time, far below this 4 MiB guard.
   */
  private val maxBufferedBytes = 4L * 1024 * 1024

  /** Minimal Arrow schema for the temp dataset that only provides the fragment ids under test. */
  private val arrowDatasetSchema = new Schema(
    Collections.singletonList(
      new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)))

  private val schema = new StructType()
    .add("_fragid", IntegerType, nullable = false)
    .add("ignored", IntegerType)
    .add("payload", ArrayType(new StructType().add("label", StringType)))
    .add("_rowaddr", LongType, nullable = false)

  private def options(datasetUri: String) =
    LanceSparkWriteOptions.builder()
      .datasetUri(datasetUri)
      .batchSize(rowsPerFragment)
      .build()

  private val bigLabel = UTF8String.fromString("x" * labelLength)

  private def row(fragment: Int, offset: Int): InternalRow = {
    val structs = Array.fill[Any](structsPerRow)(InternalRow(bigLabel))
    InternalRow(
      fragment,
      -1,
      new GenericArrayData(structs),
      (fragment.toLong << 32) + offset)
  }

  /**
   * Creates a local temp Lance dataset with {@code count} single-row fragments and returns their
   * ids in ascending order, mirroring the fragment layout of a large backfill.
   */
  private def createFragmentDataset(datasetUri: String, count: Int): Seq[Int] = {
    val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)
    try {
      val created = Dataset.create(
        allocator,
        datasetUri,
        arrowDatasetSchema,
        new WriteParams.Builder().build())
      val readVersion = created.version()
      created.close()
      val fragments = new java.util.ArrayList[FragmentMetadata]()
      val root = VectorSchemaRoot.create(arrowDatasetSchema, allocator)
      try {
        root.allocateNew()
        val idVector = root.getVector("id").asInstanceOf[IntVector]
        var i = 0
        while (i < count) {
          idVector.setSafe(0, i)
          root.setRowCount(1)
          fragments.addAll(
            Fragment.write().datasetUri(datasetUri).allocator(allocator).data(root).execute())
          i += 1
        }
      } finally root.close()
      val dataset = new FragmentOperation.Append(fragments).commit(
        allocator,
        datasetUri,
        Optional.of[java.lang.Long](java.lang.Long.valueOf(readVersion)),
        Collections.emptyMap[String, String]())
      try {
        val metadata = dataset.getFragments
        val ids = (0 until metadata.size).map(metadata.get(_).getId).sorted
        assertEquals(count, ids.size, "the temp dataset must contain one fragment per row")
        ids
      } finally dataset.close()
    } finally allocator.close()
  }

  private def deleteRecursively(root: Path): Unit = {
    if (!Files.exists(root)) {
      return
    }
    Files.walkFileTree(
      root,
      new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.deleteIfExists(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, error: IOException): FileVisitResult = {
          Files.deleteIfExists(dir)
          FileVisitResult.CONTINUE
        }
      })
  }

  private class CountingWriter(writeOptions: LanceSparkWriteOptions)
    extends AbstractBackfillWriter(
      writeOptions,
      schema,
      Collections.singletonList("payload"),
      null,
      null,
      null,
      null) {
    val completedFragments = new ArrayBuffer[Int]
    var rowsRead = 0L

    override protected def processFragment(fragment: Fragment, stream: ArrowArrayStream): Unit = {
      val reader = Data.importArrayStream(LanceRuntime.allocator(), stream)
      try {
        while (reader.loadNextBatch()) {
          rowsRead += reader.getVectorSchemaRoot.getRowCount
        }
      } finally reader.close()
      completedFragments += fragment.getId
    }

    override protected def buildCommitMessage(): WriterCommitMessage = new WriterCommitMessage {}
  }

  @Test
  def releasesFinishedFragmentBuffersBeforeNext(): Unit = {
    val tempDir = Files.createTempDirectory("lance-backfill-memory")
    try {
      val datasetUri = TestUtils.getDatasetUri(tempDir.toString, "many_fragments")
      val fragmentIds = createFragmentDataset(datasetUri, fragmentCount)
      val allocator = LanceRuntime.allocator()
      val baseline = allocator.getAllocatedMemory
      val writer = new CountingWriter(options(datasetUri))
      var peakAllocated = baseline
      try {
        fragmentIds.foreach { fragmentId =>
          (0 until rowsPerFragment).foreach { offset =>
            writer.write(row(fragmentId, offset))
            peakAllocated = math.max(peakAllocated, allocator.getAllocatedMemory)
          }
        }
        // The old implementation still holds every fragment's Arrow buffer here, right before
        // commit flushes them one by one; the streaming implementation holds at most the fragment
        // currently being written.
        val preCommit = allocator.getAllocatedMemory
        writer.commit()
        val postCommit = allocator.getAllocatedMemory
        val peakDelta = peakAllocated - baseline
        val linearBytes = fragmentCount.toLong * rowsPerFragment * structsPerRow * labelLength
        println(
          s"[BackfillMemoryTest] fragments=${fragmentIds.size} baseline=$baseline " +
            s"peak=$peakAllocated peakDelta=$peakDelta preCommit=$preCommit " +
            s"postCommit=$postCommit linearAccumulation~=$linearBytes limit=$maxBufferedBytes")

        assertEquals(fragmentIds, writer.completedFragments.toSeq)
        assertEquals(fragmentCount.toLong * rowsPerFragment, writer.rowsRead)
        assertTrue(
          peakDelta < maxBufferedBytes,
          s"Finished fragment buffers must be released before the next fragment starts: " +
            s"baseline=$baseline peak=$peakAllocated peakDelta=$peakDelta preCommit=$preCommit " +
            s"limit=$maxBufferedBytes. Accumulating all ${fragmentIds.size} fragments linearly " +
            s"would hold ~$linearBytes bytes.")
        assertTrue(
          postCommit - baseline < maxBufferedBytes,
          s"All fragment buffers must be released by commit: baseline=$baseline " +
            s"postCommit=$postCommit delta=${postCommit - baseline} limit=$maxBufferedBytes")
      } finally writer.close()
    } finally {
      deleteRecursively(tempDir)
    }
  }

  @Test
  def requestsFragmentClusteringAndOrderingForBothOperations(): Unit = {
    val add = new AddColumnsBackfillWrite.AddColumnsWriteBuilder(
      schema,
      options(TestUtils.TestTable1Config.datasetUri),
      Collections.singletonList("payload"),
      null,
      null,
      null,
      null,
      false)
      .build().asInstanceOf[RequiresDistributionAndOrdering]
    val update = new UpdateColumnsBackfillWrite.UpdateColumnsWriteBuilder(
      schema,
      options(TestUtils.TestTable1Config.datasetUri),
      Collections.singletonList("payload"),
      null,
      null,
      null,
      null,
      false)
      .build().asInstanceOf[RequiresDistributionAndOrdering]
    Seq(add, update).foreach { write =>
      val distribution = write.requiredDistribution().asInstanceOf[ClusteredDistribution]
      assertEquals(1, distribution.clustering().length)
      assertEquals("_fragid", distribution.clustering()(0).describe())
      assertEquals(1, write.requiredOrdering().length)
      assertEquals("_fragid", write.requiredOrdering()(0).expression().describe())
      assertEquals(SortDirection.ASCENDING, write.requiredOrdering()(0).direction())
    }
  }
}
