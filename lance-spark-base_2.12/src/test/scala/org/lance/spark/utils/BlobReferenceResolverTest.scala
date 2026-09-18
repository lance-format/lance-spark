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
package org.lance.spark.utils

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{LargeVarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.lance.{BlobFile, Dataset}
import org.lance.spark.write.SingleBatchArrowReader

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
import java.util.{Arrays, Collections, List => JList}

import scala.collection.JavaConverters._

class BlobReferenceResolverTest {
  @TempDir var tempDir: Path = _

  private val data = Array[Byte](1, 2, 3)

  // JNI take_rust_field clears this field when close releases the native owner.
  private def handle(blob: BlobFile): Long = {
    val field = classOf[BlobFile].getDeclaredField("nativeBlobHandle")
    field.setAccessible(true)
    field.getLong(blob)
  }

  private class RecordingResolver(afterTake: JList[BlobFile] => Unit)
    extends BlobReferenceResolver {
    var acquired: Seq[BlobFile] = Seq.empty

    override def takeBlobs(
        uri: String,
        addresses: JList[java.lang.Long],
        column: String): JList[BlobFile] = {
      val blobs = super.takeBlobs(uri, addresses, column)
      acquired = blobs.asScala.filter(_ != null).toVector
      assertTrue(acquired.nonEmpty)
      acquired.foreach(blob => assertNotEquals(0L, handle(blob)))
      afterTake(blobs)
      blobs
    }

    def assertReleased(): Unit =
      acquired.foreach(blob => assertEquals(0L, handle(blob), "native blob handle leaked"))

    override def close(): Unit = {
      acquired.filter(blob => handle(blob) != 0L).foreach(_.close())
      super.close()
    }
  }

  private def withSource(nullable: Boolean = false)(
      body: (String, JList[BlobReference]) => Unit): Unit = {
    val uri = tempDir.resolve("source.lance").toString
    val field = new Field(
      "data",
      new FieldType(
        true,
        ArrowType.Struct.INSTANCE,
        null,
        Collections.singletonMap(
          BlobUtils.ARROW_EXTENSION_NAME_KEY,
          BlobUtils.ARROW_EXTENSION_BLOB_V2)),
      Arrays.asList(
        Field.nullable("data", ArrowType.LargeBinary.INSTANCE),
        Field.nullable("uri", ArrowType.Utf8.INSTANCE)))
    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator)
    val reader = new SingleBatchArrowReader(allocator, root)
    try {
      root.allocateNew()
      val vector = root.getVector(0).asInstanceOf[StructVector]
      val bytes = vector.getChild("data").asInstanceOf[LargeVarBinaryVector]
      (0 until 3).foreach { i =>
        if (nullable && i == 1) {
          vector.setNull(i)
          bytes.setNull(i)
        } else {
          vector.setIndexDefined(i)
          bytes.setSafe(i, data)
        }
      }
      root.setRowCount(3)
      val dataset = Dataset.write()
        .allocator(allocator)
        .reader(reader)
        .uri(uri)
        .dataStorageVersion("2.2")
        .execute()
      try {
        assertEquals(1, dataset.getFragments.size())
        val fragmentId = dataset.getFragments.get(0).getId.toLong
        val refs = (0 until 3)
          .map(i => new BlobReference(uri, "data", (fragmentId << 32) | i.toLong))
          .asJava
        body(uri, refs)
      } finally dataset.close()
    } finally {
      reader.close()
      root.close()
      allocator.close()
    }
  }

  private def indices(refs: JList[BlobReference]): JList[Integer] =
    (0 until refs.size()).map(Integer.valueOf).asJava

  private def deleteDataFiles(uri: String): Unit = {
    val paths = Files.walk(Paths.get(uri).resolve("data"))
    try {
      val files = paths.iterator().asScala.filter(Files.isRegularFile(_)).toVector
      assertFalse(files.isEmpty)
      files.foreach(Files.delete)
    } finally paths.close()
  }

  @Test
  def batchSuccessReleasesAllHandles(): Unit = withSource() { (_, refs) =>
    val resolver = new RecordingResolver(_ => ())
    try {
      val result = resolver.resolveBatch(indices(refs), refs)
      assertEquals(3, result.size())
      result.values().asScala.foreach(bytes => assertArrayEquals(data, bytes))
      resolver.assertReleased()
    } finally resolver.close()
  }

  @Test
  def nullBlobReleasesVisitedAndUnvisitedHandles(): Unit = withSource(nullable = true) {
    (_, refs) =>
      val resolver = new RecordingResolver(blobs => assertNull(blobs.get(1)))
      try {
        val error = assertThrows(
          classOf[IOException],
          () => resolver.resolveBatch(indices(refs), refs))
        assertTrue(error.getMessage.contains("takeBlobs returned a null blob"))
        resolver.assertReleased()
      } finally resolver.close()
  }

  @Test
  def countMismatchReleasesReturnedHandles(): Unit = withSource() { (_, refs) =>
    val resolver = new RecordingResolver(blobs => blobs.remove(blobs.size() - 1).close())
    try {
      val error = assertThrows(
        classOf[IOException],
        () => resolver.resolveBatch(indices(refs), refs))
      assertTrue(error.getMessage.contains("takeBlobs returned 2 blobs for 3 requested addresses"))
      resolver.assertReleased()
    } finally resolver.close()
  }

  @Test
  def batchReadFailureReleasesAllHandles(): Unit = withSource() { (uri, refs) =>
    val resolver = new RecordingResolver(_ => deleteDataFiles(uri))
    try {
      val error = assertThrows(
        classOf[IOException],
        () => resolver.resolveBatch(indices(refs), refs))
      assertTrue(error.getMessage.contains("Not found"), error.getMessage)
      resolver.assertReleased()
    } finally resolver.close()
  }

  @Test
  def singleReadFailureReleasesHandle(): Unit = withSource() { (uri, refs) =>
    val resolver = new RecordingResolver(_ => deleteDataFiles(uri))
    try {
      val error = assertThrows(classOf[IOException], () => resolver.resolve(refs.get(0)))
      assertTrue(error.getMessage.contains("Not found"), error.getMessage)
      resolver.assertReleased()
    } finally resolver.close()
  }
}
