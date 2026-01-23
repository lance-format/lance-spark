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
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.LanceArrowUtils
import org.lance.spark.utils.VectorUtils

import scala.jdk.CollectionConverters._

/**
 * Custom ArrowWriter implementation that supports converting Spark DataFrame
 * Array<Float/Double> columns to Arrow FixedSizeList for vector embeddings.
 *
 * This class is copied and modified from Apache Spark's ArrowWriter
 * (https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowWriter.scala)
 * to enable writing vector columns (embeddings) as FixedSizeList, which is required for
 * Lance's vector indexing and similarity search capabilities.
 *
 * Key modifications from Spark's ArrowWriter:
 * - Detects when an Arrow field is FixedSizeListVector (created by LanceArrowUtils)
 * - Uses custom FixedSizeListWriter to write data directly to FixedSizeListVector
 * - Validates vector dimensions during write
 */
object LanceArrowWriter {
  def create(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean = true,
      largeVarTypes: Boolean = false): LanceArrowWriter = {
    val arrowSchema = LanceArrowUtils.toArrowSchema(
      schema,
      timeZoneId,
      errorOnDuplicatedFieldNames,
      largeVarTypes)
    val root = VectorSchemaRoot.create(arrowSchema, new RootAllocator(Long.MaxValue))
    create(root, schema)
  }

  def create(root: VectorSchemaRoot, sparkSchema: StructType): LanceArrowWriter = {
    val children = root.getFieldVectors().asScala.zipWithIndex.map { case (vector, index) =>
      vector.allocateNew()
      val sparkField = sparkSchema.fields(index)
      createFieldWriter(vector, sparkField.dataType, sparkField.metadata)
    }
    new LanceArrowWriter(root, children.toArray)
  }

  private[arrow] def createFieldWriter(
      vector: ValueVector,
      sparkType: DataType,
      metadata: org.apache.spark.sql.types.Metadata = null): LanceArrowFieldWriter = {
    (sparkType, vector) match {
      case (ArrayType(elementType: NumericType, _), vector: FixedSizeListVector) =>
        val elementWriter = createFieldWriter(vector.getDataVector(), elementType, null)
        new FixedSizeListWriter(vector, elementWriter)

      case (ArrayType(elementType, _), vector: ListVector) =>
        val elementWriter = createFieldWriter(vector.getDataVector(), elementType, null)
        new ArrayWriter(vector, elementWriter)

      case (BooleanType, vector: BitVector) => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector) => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new LongWriter(vector)
      case (LongType, vector: UInt8Vector) => new UnsignedLongWriter(vector)
      case (FloatType, vector: Float4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new DoubleWriter(vector)
      case (dt: DecimalType, vector: DecimalVector) =>
        new DecimalWriter(vector, dt.precision, dt.scale)
      case (StringType, vector: VarCharVector) => new StringWriter(vector)
      case (StringType, vector: LargeVarCharVector) => new LargeStringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (BinaryType, vector: LargeVarBinaryVector) => new LargeBinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new TimestampWriter(vector)
      case (TimestampNTZType, vector: TimeStampMicroVector) => new TimestampNTZWriter(vector)
      case (MapType(_, _, _), vector: MapVector) =>
        val structVector = vector.getDataVector.asInstanceOf[StructVector]
        val keyWriter = createFieldWriter(
          structVector.getChild(MapVector.KEY_NAME),
          sparkType.asInstanceOf[MapType].keyType,
          null)
        val valueWriter = createFieldWriter(
          structVector.getChild(MapVector.VALUE_NAME),
          sparkType.asInstanceOf[MapType].valueType,
          null)
        new MapWriter(vector, structVector, keyWriter, valueWriter)
      case (StructType(fields), vector: StructVector) =>
        val children = fields.zipWithIndex.map { case (field, ordinal) =>
          createFieldWriter(vector.getChildByOrdinal(ordinal), field.dataType, field.metadata)
        }
        new StructWriter(vector, children.toArray)
      case (NullType, vector: NullVector) => new NullWriter(vector)
      case (_: YearMonthIntervalType, vector: IntervalYearVector) => new IntervalYearWriter(vector)
      case (_: DayTimeIntervalType, vector: DurationVector) => new DurationWriter(vector)
      case (CalendarIntervalType, vector: IntervalMonthDayNanoVector) =>
        new IntervalMonthDayNanoWriter(vector)
      case (dt, _) =>
        throw new UnsupportedOperationException(s"Unsupported data type: $dt")
    }
  }

  private def shouldBeFixedSizeList(
      metadata: org.apache.spark.sql.types.Metadata,
      elementType: DataType): Boolean = {
    metadata != null &&
    metadata.contains(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY) &&
    metadata.getLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY) > 0 &&
    (elementType == FloatType || elementType == DoubleType)
  }
}

/**
 * Writer that converts Spark InternalRow data to Arrow format.
 * Copied from Spark's ArrowWriter to support custom field writers for FixedSizeList.
 */
class LanceArrowWriter(root: VectorSchemaRoot, fields: Array[LanceArrowFieldWriter]) {
  def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fields.length) {
      fields(i).write(row, i)
      i += 1
    }
  }

  def finish(): Unit = {
    fields.foreach(_.finish())
    root.setRowCount(fields(0).count)
  }

  def reset(): Unit = {
    fields.foreach(_.reset())
    root.setRowCount(0)
  }

  def field(index: Int): LanceArrowFieldWriter = fields(index)
}

/**
 * Custom writer for FixedSizeList vectors (used for ML embeddings/vectors).
 * This is a new class not present in Spark's ArrowWriter.
 * It handles writing Spark ArrayType data to Arrow FixedSizeListVector for vector columns.
 */
private[arrow] class FixedSizeListWriter(
    val valueVector: FixedSizeListVector,
    val elementWriter: LanceArrowFieldWriter) extends LanceArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val array = input.getArray(ordinal)
    val listSize = valueVector.getListSize()

    if (array.numElements() != listSize) {
      throw new IllegalArgumentException(
        s"Array size ${array.numElements()} does not match FixedSizeList size $listSize")
    }

    valueVector.setNotNull(count)
    var i = 0
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
  }

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

// ================================================================================
// The following writer classes are copied from Spark's ArrowWriter with no modifications.
// They handle conversion from Spark's InternalRow format to Arrow vectors for basic types.
// ================================================================================

private[arrow] class BooleanWriter(val valueVector: BitVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }
}

private[arrow] class ByteWriter(val valueVector: TinyIntVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getByte(ordinal))
  }
}

private[arrow] class ShortWriter(val valueVector: SmallIntVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getShort(ordinal))
  }
}

private[arrow] class IntegerWriter(val valueVector: IntVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class LongWriter(val valueVector: BigIntVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class UnsignedLongWriter(val valueVector: UInt8Vector)
  extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class FloatWriter(val valueVector: Float4Vector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getFloat(ordinal))
  }
}

private[arrow] class DoubleWriter(val valueVector: Float8Vector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getDouble(ordinal))
  }
}

private[arrow] class DecimalWriter(
    val valueVector: DecimalVector,
    precision: Int,
    scale: Int) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val decimal = input.getDecimal(ordinal, precision, scale)
    if (decimal == null) {
      valueVector.setNull(count)
    } else {
      valueVector.setSafe(count, decimal.toJavaBigDecimal)
    }
  }
}

private[arrow] class StringWriter(val valueVector: VarCharVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    valueVector.setSafe(count, utf8.getBytes)
  }
}

private[arrow] class LargeStringWriter(val valueVector: LargeVarCharVector)
  extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    valueVector.setSafe(count, utf8.getBytes)
  }
}

private[arrow] class BinaryWriter(val valueVector: VarBinaryVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes)
  }
}

private[arrow] class LargeBinaryWriter(val valueVector: LargeVarBinaryVector)
  extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes)
  }
}

private[arrow] class DateWriter(val valueVector: DateDayVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class TimestampWriter(val valueVector: TimeStampMicroTZVector)
  extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class TimestampNTZWriter(val valueVector: TimeStampMicroVector)
  extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class ArrayWriter(
    val valueVector: ListVector,
    val elementWriter: LanceArrowFieldWriter) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val array = input.getArray(ordinal)
    var i = 0
    valueVector.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueVector.endValue(count, array.numElements())
  }
  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }
  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

private[arrow] class MapWriter(
    val valueVector: MapVector,
    val structVector: StructVector,
    val keyWriter: LanceArrowFieldWriter,
    val valueWriter: LanceArrowFieldWriter) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val map = input.getMap(ordinal)
    val keys = map.keyArray()
    val values = map.valueArray()
    var i = 0
    valueVector.startNewValue(count)
    while (i < map.numElements()) {
      keyWriter.write(keys, i)
      valueWriter.write(values, i)
      i += 1
    }
    valueVector.endValue(count, map.numElements())
  }
  override def finish(): Unit = {
    super.finish()
    keyWriter.finish()
    valueWriter.finish()
  }
  override def reset(): Unit = {
    super.reset()
    keyWriter.reset()
    valueWriter.reset()
  }
}

private[arrow] class StructWriter(
    val valueVector: StructVector,
    val children: Array[LanceArrowFieldWriter]) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val struct = input.getStruct(ordinal, children.length)
    var i = 0
    while (i < children.length) {
      children(i).write(struct, i)
      i += 1
    }
    valueVector.setIndexDefined(count)
  }
  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }
  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }
}

private[arrow] class NullWriter(val valueVector: NullVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {}
}

private[arrow] class IntervalYearWriter(val valueVector: IntervalYearVector)
  extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class DurationWriter(val valueVector: DurationVector) extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class IntervalMonthDayNanoWriter(val valueVector: IntervalMonthDayNanoVector)
  extends LanceArrowFieldWriter {
  override def setNull(): Unit = {}
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val interval = input.getInterval(ordinal)
    valueVector.setSafe(count, interval.months, interval.days, interval.microseconds * 1000L)
  }
}
