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
package org.apache.spark.sql.vectorized

/*
 * The following code is originally from https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/vectorized/ArrowColumnVectorSuite.scala
 * and is licensed under the Apache license:
 *
 * License: Apache License 2.0, Copyright 2014 and onwards The Apache Software Foundation.
 * https://github.com/apache/spark/blob/master/LICENSE
 *
 * It has been modified by the Lance developers to fit the needs of the Lance project.
 */

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{ArrowUtils, LanceArrowUtils}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.spark.LanceConstant
import org.scalatest.funsuite.AnyFunSuite

class LanceArrowColumnVectorSuite extends AnyFunSuite {
  test("boolean") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("boolean", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("boolean", BooleanType, nullable = true, null)
      .createVector(allocator).asInstanceOf[BitVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, if (i % 2 == 0) 1 else 0)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === BooleanType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getBoolean(i) === (i % 2 == 0))
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getBooleans(0, 10) === (0 until 10).map(i => (i % 2 == 0)))

    columnVector.close()
    allocator.close()
  }

  test("byte") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("byte", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("byte", ByteType, nullable = true, null)
      .createVector(allocator).asInstanceOf[TinyIntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toByte)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === ByteType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getByte(i) === i.toByte)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getBytes(0, 10) === (0 until 10).map(i => i.toByte))

    columnVector.close()
    allocator.close()
  }

  test("short") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("short", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("short", ShortType, nullable = true, null)
      .createVector(allocator).asInstanceOf[SmallIntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toShort)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === ShortType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getShort(i) === i.toShort)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getShorts(0, 10) === (0 until 10).map(i => i.toShort))

    columnVector.close()
    allocator.close()
  }

  test("int") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("int", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("int", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === IntegerType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getInt(i) === i)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getInts(0, 10) === (0 until 10))

    columnVector.close()
    allocator.close()
  }

  test("long") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("long", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("long", LongType, nullable = true, null)
      .createVector(allocator).asInstanceOf[BigIntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toLong)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === LongType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getLong(i) === i.toLong)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getLongs(0, 10) === (0 until 10).map(i => i.toLong))

    columnVector.close()
    allocator.close()
  }

  test("unsigned long (UInt8)") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("unsigned long", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField(LanceConstant.ROW_ID, LongType, nullable = true, null)
      .createVector(allocator).asInstanceOf[UInt8Vector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toLong)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === LongType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getLong(i) === i.toLong)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getLongs(0, 10) === (0 until 10).map(i => i.toLong))

    columnVector.close()
    allocator.close()
  }

  test("unsigned byte (UInt1)") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("uint1", 0, Long.MaxValue)
    val vector = new UInt1Vector("uint1", allocator)
    vector.allocateNew()

    // Test values including values > 127 to verify unsigned handling
    val testValues = Array(0, 1, 127, 128, 200, 255)
    testValues.zipWithIndex.foreach { case (v, i) =>
      vector.setSafe(i, v.toByte)
    }
    vector.setNull(testValues.length)
    vector.setValueCount(testValues.length + 1)

    val columnVector = new LanceArrowColumnVector(vector)
    // UInt8 (8-bit unsigned) maps to ShortType
    assert(columnVector.dataType === ShortType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    testValues.zipWithIndex.foreach { case (expected, i) =>
      assert(columnVector.getShort(i) === expected.toShort)
    }
    assert(columnVector.isNullAt(testValues.length))

    columnVector.close()
    allocator.close()
  }

  test("unsigned short (UInt2)") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("uint2", 0, Long.MaxValue)
    val vector = new UInt2Vector("uint2", allocator)
    vector.allocateNew()

    // Test values including values > 32767 to verify unsigned handling
    val testValues = Array(0, 1, 32767, 32768, 50000, 65535)
    testValues.zipWithIndex.foreach { case (v, i) =>
      vector.setSafe(i, v.toChar)
    }
    vector.setNull(testValues.length)
    vector.setValueCount(testValues.length + 1)

    val columnVector = new LanceArrowColumnVector(vector)
    // UInt16 (16-bit unsigned) maps to IntegerType
    assert(columnVector.dataType === IntegerType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    testValues.zipWithIndex.foreach { case (expected, i) =>
      assert(columnVector.getInt(i) === expected)
    }
    assert(columnVector.isNullAt(testValues.length))

    columnVector.close()
    allocator.close()
  }

  test("unsigned int (UInt4)") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("uint4", 0, Long.MaxValue)
    val vector = new UInt4Vector("uint4", allocator)
    vector.allocateNew()

    // Test values including values > Int.MaxValue to verify unsigned handling
    val testValues =
      Array(0L, 1L, Int.MaxValue.toLong, Int.MaxValue.toLong + 1, 3000000000L, 4294967295L)
    testValues.zipWithIndex.foreach { case (v, i) =>
      vector.setSafe(i, v.toInt)
    }
    vector.setNull(testValues.length)
    vector.setValueCount(testValues.length + 1)

    val columnVector = new LanceArrowColumnVector(vector)
    // UInt32 (32-bit unsigned) maps to LongType
    assert(columnVector.dataType === LongType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    testValues.zipWithIndex.foreach { case (expected, i) =>
      assert(columnVector.getLong(i) === expected)
    }
    assert(columnVector.isNullAt(testValues.length))

    columnVector.close()
    allocator.close()
  }

  test("float") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("float", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("float", FloatType, nullable = true, null)
      .createVector(allocator).asInstanceOf[Float4Vector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toFloat)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === FloatType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getFloat(i) === i.toFloat)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getFloats(0, 10) === (0 until 10).map(i => i.toFloat))

    columnVector.close()
    allocator.close()
  }

  test("double") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("double", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("double", DoubleType, nullable = true, null)
      .createVector(allocator).asInstanceOf[Float8Vector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toDouble)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === DoubleType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getDouble(i) === i.toDouble)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getDoubles(0, 10) === (0 until 10).map(i => i.toDouble))

    columnVector.close()
    allocator.close()
  }

  test("string") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("string", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("string", StringType, nullable = true, null)
      .createVector(allocator).asInstanceOf[VarCharVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === StringType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getUTF8String(i) === UTF8String.fromString(s"str$i"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("large_string") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("string", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("string", StringType, nullable = true, null, true)
      .createVector(allocator).asInstanceOf[LargeVarCharVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === StringType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getUTF8String(i) === UTF8String.fromString(s"str$i"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("binary") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("binary", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("binary", BinaryType, nullable = true, null, false)
      .createVector(allocator).asInstanceOf[VarBinaryVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === BinaryType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getBinary(i) === s"str$i".getBytes("utf8"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("large_binary") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("binary", 0, Long.MaxValue)
    val vector = LanceArrowUtils.toArrowField("binary", BinaryType, nullable = true, null, true)
      .createVector(allocator).asInstanceOf[LargeVarBinaryVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === BinaryType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getBinary(i) === s"str$i".getBytes("utf8"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("array") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("array", 0, Long.MaxValue)
    val vector =
      LanceArrowUtils.toArrowField("array", ArrayType(IntegerType), nullable = true, null)
        .createVector(allocator).asInstanceOf[ListVector]
    vector.allocateNew()
    val elementVector = vector.getDataVector().asInstanceOf[IntVector]

    // [1, 2]
    vector.startNewValue(0)
    elementVector.setSafe(0, 1)
    elementVector.setSafe(1, 2)
    vector.endValue(0, 2)

    // [3, null, 5]
    vector.startNewValue(1)
    elementVector.setSafe(2, 3)
    elementVector.setNull(3)
    elementVector.setSafe(4, 5)
    vector.endValue(1, 3)

    // null

    // []
    vector.startNewValue(3)
    vector.endValue(3, 0)

    elementVector.setValueCount(5)
    vector.setValueCount(4)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === ArrayType(IntegerType))
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    val array0 = columnVector.getArray(0)
    assert(array0.numElements() === 2)
    assert(array0.getInt(0) === 1)
    assert(array0.getInt(1) === 2)

    val array1 = columnVector.getArray(1)
    assert(array1.numElements() === 3)
    assert(array1.getInt(0) === 3)
    assert(array1.isNullAt(1))
    assert(array1.getInt(2) === 5)

    assert(columnVector.isNullAt(2))

    val array3 = columnVector.getArray(3)
    assert(array3.numElements() === 0)

    columnVector.close()
    allocator.close()
  }

  test("non nullable struct") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("struct", 0, Long.MaxValue)
    val schema = new StructType().add("int", IntegerType).add("long", LongType)
    val vector = LanceArrowUtils.toArrowField("struct", schema, nullable = false, null)
      .createVector(allocator).asInstanceOf[StructVector]

    vector.allocateNew()
    val intVector = vector.getChildByOrdinal(0).asInstanceOf[IntVector]
    val longVector = vector.getChildByOrdinal(1).asInstanceOf[BigIntVector]

    vector.setIndexDefined(0)
    intVector.setSafe(0, 1)
    longVector.setSafe(0, 1L)

    vector.setIndexDefined(1)
    intVector.setSafe(1, 2)
    longVector.setNull(1)

    vector.setValueCount(2)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === schema)
    assert(!columnVector.hasNull)
    assert(columnVector.numNulls === 0)

    val row0 = columnVector.getStruct(0)
    assert(row0.getInt(0) === 1)
    assert(row0.getLong(1) === 1L)

    val row1 = columnVector.getStruct(1)
    assert(row1.getInt(0) === 2)
    assert(row1.isNullAt(1))

    columnVector.close()
    allocator.close()
  }

  test("struct") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("struct", 0, Long.MaxValue)
    val schema = new StructType().add("int", IntegerType).add("long", LongType)
    val vector = LanceArrowUtils.toArrowField("struct", schema, nullable = true, null)
      .createVector(allocator).asInstanceOf[StructVector]
    vector.allocateNew()
    val intVector = vector.getChildByOrdinal(0).asInstanceOf[IntVector]
    val longVector = vector.getChildByOrdinal(1).asInstanceOf[BigIntVector]

    // (1, 1L)
    vector.setIndexDefined(0)
    intVector.setSafe(0, 1)
    longVector.setSafe(0, 1L)

    // (2, null)
    vector.setIndexDefined(1)
    intVector.setSafe(1, 2)
    longVector.setNull(1)

    // (null, 3L)
    vector.setIndexDefined(2)
    intVector.setNull(2)
    longVector.setSafe(2, 3L)

    // null
    vector.setNull(3)

    // (5, 5L)
    vector.setIndexDefined(4)
    intVector.setSafe(4, 5)
    longVector.setSafe(4, 5L)

    intVector.setValueCount(5)
    longVector.setValueCount(5)
    vector.setValueCount(5)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === schema)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    val row0 = columnVector.getStruct(0)
    assert(row0.getInt(0) === 1)
    assert(row0.getLong(1) === 1L)

    val row1 = columnVector.getStruct(1)
    assert(row1.getInt(0) === 2)
    assert(row1.isNullAt(1))

    val row2 = columnVector.getStruct(2)
    assert(row2.isNullAt(0))
    assert(row2.getLong(1) === 3L)

    assert(columnVector.isNullAt(3))

    val row4 = columnVector.getStruct(4)
    assert(row4.getInt(0) === 5)
    assert(row4.getLong(1) === 5L)

    columnVector.close()
    allocator.close()
  }

  test("nested struct with array") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("nested_struct", 0, Long.MaxValue)
    // Schema: features: struct<feature_1: struct<feature_version: int, feature_values: array<double>>>
    val innerSchema = new StructType()
      .add("feature_version", IntegerType)
      .add("feature_values", ArrayType(DoubleType))
    val outerSchema = new StructType().add("feature_1", innerSchema)

    val vector = LanceArrowUtils.toArrowField("features", outerSchema, nullable = true, null)
      .createVector(allocator).asInstanceOf[StructVector]
    vector.allocateNew()

    // Get the inner struct vector (feature_1)
    val feature1Vector = vector.getChildByOrdinal(0).asInstanceOf[StructVector]
    val versionVector = feature1Vector.getChildByOrdinal(0).asInstanceOf[IntVector]
    val valuesVector = feature1Vector.getChildByOrdinal(1).asInstanceOf[ListVector]
    val valuesDataVector = valuesVector.getDataVector.asInstanceOf[Float8Vector]

    // Row 0: feature_1: {feature_version: 1, feature_values: [0.5, 0.8, 0.3]}
    vector.setIndexDefined(0)
    feature1Vector.setIndexDefined(0)
    versionVector.setSafe(0, 1)
    valuesVector.startNewValue(0)
    valuesDataVector.setSafe(0, 0.5)
    valuesDataVector.setSafe(1, 0.8)
    valuesDataVector.setSafe(2, 0.3)
    valuesVector.endValue(0, 3)

    // Row 1: feature_1: {feature_version: 2, feature_values: [0.9, 0.7]}
    vector.setIndexDefined(1)
    feature1Vector.setIndexDefined(1)
    versionVector.setSafe(1, 2)
    valuesVector.startNewValue(1)
    valuesDataVector.setSafe(3, 0.9)
    valuesDataVector.setSafe(4, 0.7)
    valuesVector.endValue(1, 2)

    valuesDataVector.setValueCount(5)
    versionVector.setValueCount(2)
    feature1Vector.setValueCount(2)
    vector.setValueCount(2)

    val columnVector = new LanceArrowColumnVector(vector)
    assert(columnVector.dataType === outerSchema)

    // Access nested struct: features.feature_1
    val row0 = columnVector.getStruct(0)
    val feature1Row0 = row0.getStruct(0, 2) // 2 fields in inner struct

    // Access array: features.feature_1.feature_values
    val arrayRow0 = feature1Row0.getArray(1)
    assert(arrayRow0.numElements() === 3)
    assert(arrayRow0.getDouble(0) === 0.5)
    assert(arrayRow0.getDouble(1) === 0.8)
    assert(arrayRow0.getDouble(2) === 0.3)

    // Row 1
    val row1 = columnVector.getStruct(1)
    val feature1Row1 = row1.getStruct(0, 2) // 2 fields in inner struct
    val arrayRow1 = feature1Row1.getArray(1)
    assert(arrayRow1.numElements() === 2)
    assert(arrayRow1.getDouble(0) === 0.9)
    assert(arrayRow1.getDouble(1) === 0.7)

    columnVector.close()
    allocator.close()
  }
}
