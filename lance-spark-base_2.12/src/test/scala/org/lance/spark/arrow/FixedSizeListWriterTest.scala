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
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.LanceArrowUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class FixedSizeListWriterTest {

  @Test
  def nullElementsMustMatchTheDeclaredElementNullability(): Unit = {
    for (containsNull <- Seq(false, true)) {
      val metadata = new MetadataBuilder().putLong("arrow.fixed-size-list.size", 3L).build()
      val schema = new StructType().add(
        StructField("embedding", ArrayType(FloatType, containsNull), nullable = true, metadata))
      val allocator = new RootAllocator(Long.MaxValue)
      val root =
        VectorSchemaRoot.create(LanceArrowUtils.toArrowSchema(schema, "UTC", false), allocator)
      try {
        val writer = LanceArrowWriter.create(root, schema)
        val row = InternalRow(new GenericArrayData(Array[Any](1.0f, null, 3.0f)))
        if (containsNull) {
          writer.write(row)
          writer.finish()
          val vector = root.getVector(0).asInstanceOf[FixedSizeListVector]
          assertFalse(vector.isNull(0))
          assertTrue(vector.getDataVector.isNull(1))
        } else {
          val error = assertThrows(classOf[IllegalArgumentException], () => writer.write(row))
          assertTrue(error.getMessage.contains("embedding"))
          assertTrue(error.getMessage.contains("index 1"))
        }
      } finally {
        root.close()
        allocator.close()
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = Array(3, 1024))
  def nullVectorsPreserveChildOffsetsAndValidityAfterReset(dimensions: Int): Unit = {
    for (elementType <- Seq(FloatType, DoubleType); containsNull <- Seq(false, true)) {
      val metadata = new MetadataBuilder().putLong("arrow.fixed-size-list.size", dimensions).build()
      val schema = new StructType().add(
        StructField("embedding", ArrayType(elementType, containsNull), nullable = true, metadata))
      val allocator = new RootAllocator(Long.MaxValue)
      val root =
        VectorSchemaRoot.create(LanceArrowUtils.toArrowSchema(schema, "UTC", false), allocator)
      try {
        val writer = LanceArrowWriter.create(root, schema)
        val vector = root.getVector(0).asInstanceOf[FixedSizeListVector]
        val values = Array.tabulate[Any](dimensions)(index =>
          if (elementType == FloatType) Float.box(index.toFloat) else Double.box(index.toDouble))
        for (_ <- 0 until 2) {
          writer.write(InternalRow(null))
          writer.write(InternalRow(new GenericArrayData(values)))
          for (_ <- 0 until 5) writer.write(InternalRow(null))
          writer.write(InternalRow(new GenericArrayData(values)))
          writer.finish()

          assertEquals(8, vector.getValueCount)
          assertEquals(8 * dimensions, vector.getDataVector.getValueCount)
          for (rowIndex <- 0 until 8) {
            val isNull = rowIndex != 1 && rowIndex != 7
            assertEquals(isNull, vector.isNull(rowIndex))
            for (elementIndex <- 0 until dimensions) {
              val childIndex = rowIndex * dimensions + elementIndex
              assertEquals(isNull && containsNull, vector.getDataVector.isNull(childIndex))
              if (!isNull) {
                assertEquals(values(elementIndex), vector.getDataVector.getObject(childIndex))
              }
            }
          }
          writer.reset()
        }
      } finally {
        root.close()
        allocator.close()
      }
    }
  }
}
