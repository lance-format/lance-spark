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
package org.apache.spark.sql.util

/*
 * The following code is originally from https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
 * and is licensed under the Apache license:
 *
 * License: Apache License 2.0, Copyright 2014 and onwards The Apache Software Foundation.
 * https://github.com/apache/spark/blob/master/LICENSE
 *
 * It has been modified by the Lance developers to fit the needs of the Lance project.
 */

import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.types._
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST.{JObject, JString}
import org.lance.spark.LanceConstant
import org.lance.spark.utils.{BlobUtils, VectorUtils}

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

object LanceArrowUtils {
  val ARROW_FIXED_SIZE_LIST_SIZE_KEY = VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY
  val ENCODING_BLOB = BlobUtils.LANCE_ENCODING_BLOB_KEY

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      // Handle unsigned integers by mapping to larger signed types
      // UInt8 -> ShortType (signed 16-bit can hold all UInt8 values 0-255)
      case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 => ShortType
      // UInt16 -> IntegerType (signed 32-bit can hold all UInt16 values 0-65535)
      case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 2 => IntegerType
      // UInt32 -> LongType (signed 64-bit can hold all UInt32 values 0-4294967295)
      case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 4 => LongType
      // UInt64 -> LongType (may overflow for values > Long.MAX_VALUE, but no better option)
      case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
      case fixedSizeList: ArrowType.FixedSizeList =>
        // Convert FixedSizeList back to ArrayType for Spark
        // The FixedSizeList has a single child field that describes the element type
        val children = field.getChildren
        if (children.isEmpty) {
          throw new SparkException(s"FixedSizeList field ${field.getName} has no children")
        }
        val elementField = children.get(0)
        val elementType = fromArrowField(elementField)
        val containsNull = elementField.isNullable
        ArrayType(elementType, containsNull)
      case struct: ArrowType.Struct =>
        if (isBlobField(field)) {
          // Lance returns blob columns as structs with position and size fields
          // Keep it as a struct but handle unsigned Int64 fields
          val fields = field.getChildren.asScala.map { childField =>
            val childType = childField.getType match {
              case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
              case _ => fromArrowField(childField)
            }
            StructField(childField.getName, childType, childField.isNullable)
          }.toArray
          StructType(fields)
        } else {
          // Regular struct, use standard conversion
          ArrowUtils.fromArrowField(field)
        }
      case largeBinary: ArrowType.LargeBinary if isBlobField(field) =>
        // Lance returns LargeBinary in schema but Struct in data for blob columns
        // We need to handle this as binary to match the schema
        BinaryType
      case l: ArrowType.List =>
        val children = field.getChildren
        if (children.isEmpty) {
          throw new SparkException(s"List field ${field.getName} has no children")
        }
        val elementField = children.get(0)
        val elementType = fromArrowField(elementField)
        val containsNull = elementField.isNullable
        ArrayType(elementType, containsNull)
      case _ => ArrowUtils.fromArrowField(field)
    }
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      // If the Arrow field was a FixedSizeList, add metadata to preserve the size information
      val metadata = field.getType match {
        case fixedSizeList: ArrowType.FixedSizeList =>
          new MetadataBuilder()
            .putLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY, fixedSizeList.getListSize)
            .build()
        case _ => Metadata.fromJObject(
            JObject(field.getMetadata.asScala.map { case (k, v) => (k, JString(v)) }.toList))
      }
      StructField(field.getName, dt, field.isNullable, metadata)
    }.toArray)
  }

  def toArrowSchema(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean = false): Schema = {
    new Schema(schema.map { field =>
      toArrowField(
        field.name,
        deduplicateFieldNames(field.dataType, errorOnDuplicatedFieldNames),
        field.nullable,
        timeZoneId,
        largeVarTypes,
        field.metadata)
    }.asJava)
  }

  def toArrowField(
      name: String,
      dt: DataType,
      nullable: Boolean,
      timeZoneId: String,
      largeVarTypes: Boolean = false,
      metadata: org.apache.spark.sql.types.Metadata = null): Field = {
    var large: Boolean = largeVarTypes
    var meta: Map[String, String] = Map.empty

    if (metadata != null) {
      if (metadata.contains(ENCODING_BLOB)
        && metadata.getString(ENCODING_BLOB).equalsIgnoreCase("true")) {
        large = true
      }

      implicit val formats: Formats = DefaultFormats
      meta = metadata.jsonValue.extract[Map[String, Object]].map { case (k, v) =>
        (k, String.valueOf(v))
      }
    }

    dt match {
      case ArrayType(elementType, containsNull) =>
        if (shouldBeFixedSizeList(metadata, elementType)) {
          val listSize = metadata.getLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY).toInt
          val fieldType =
            new FieldType(nullable, new ArrowType.FixedSizeList(listSize), null, meta.asJava)
          new Field(
            name,
            fieldType,
            Seq(
              toArrowField("element", elementType, containsNull, timeZoneId, large)).asJava)
        } else {
          val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null, meta.asJava)
          new Field(
            name,
            fieldType,
            Seq(
              toArrowField("element", elementType, containsNull, timeZoneId, large)).asJava)
        }
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null, meta.asJava)
        new Field(
          name,
          fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId, large)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null, meta.asJava)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(
          name,
          mapType,
          Seq(toArrowField(
            MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId,
            largeVarTypes)).asJava)
      case udt: UserDefinedType[_] =>
        toArrowField(name, udt.sqlType, nullable, timeZoneId, largeVarTypes)
      case dataType =>
        val fieldType =
          new FieldType(nullable, toArrowType(dataType, timeZoneId, large, name), null, meta.asJava)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  /**
   * Contains copy of org.apache.spark.sql.util.ArrowUtils#toArrowType for Spark version compatibility
   * Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes
   */
  private def toArrowType(
      dt: DataType,
      timeZoneId: String,
      largeVarTypes: Boolean = false,
      name: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType if name.equals(LanceConstant.ROW_ID) || name.equals(LanceConstant.ROW_ADDRESS) =>
      new ArrowType.Int(8 * 8, false)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case _: StringType if !largeVarTypes => ArrowType.Utf8.INSTANCE
    case BinaryType if !largeVarTypes => ArrowType.Binary.INSTANCE
    case _: StringType if largeVarTypes => ArrowType.LargeUtf8.INSTANCE
    case BinaryType if largeVarTypes => ArrowType.LargeBinary.INSTANCE
    case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale, 8 * 16)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType if timeZoneId == null =>
      throw SparkException.internalError("Missing timezoneId where it is mandatory.")
    case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
    case TimestampNTZType =>
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    case NullType => ArrowType.Null.INSTANCE
    case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
    case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
    case CalendarIntervalType => new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)
    case _ =>
      throw unsupportedDataTypeError(dt)
  }

  private def deduplicateFieldNames(
      dt: DataType,
      errorOnDuplicatedFieldNames: Boolean): DataType = dt match {
    case udt: UserDefinedType[_] => deduplicateFieldNames(udt.sqlType, errorOnDuplicatedFieldNames)
    case st @ StructType(fields) =>
      val newNames = if (st.names.toSet.size == st.names.length) {
        st.names
      } else {
        if (errorOnDuplicatedFieldNames) {
          throw duplicatedFieldNameInArrowStructError(st.names)
        }
        val genNawName = st.names.groupBy(identity).map {
          case (name, names) if names.length > 1 =>
            val i = new AtomicInteger()
            name -> { () => s"${name}_${i.getAndIncrement()}" }
          case (name, _) => name -> { () => name }
        }
        st.names.map(genNawName(_)())
      }
      val newFields =
        fields.zip(newNames).map { case (StructField(_, dataType, nullable, metadata), name) =>
          StructField(
            name,
            deduplicateFieldNames(dataType, errorOnDuplicatedFieldNames),
            nullable,
            metadata)
        }
      StructType(newFields)
    case ArrayType(elementType, containsNull) =>
      ArrayType(deduplicateFieldNames(elementType, errorOnDuplicatedFieldNames), containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(
        deduplicateFieldNames(keyType, errorOnDuplicatedFieldNames),
        deduplicateFieldNames(valueType, errorOnDuplicatedFieldNames),
        valueContainsNull)
    case _ => dt
  }

  private def shouldBeFixedSizeList(
      metadata: org.apache.spark.sql.types.Metadata,
      elementType: DataType): Boolean = {
    // Create a temporary ArrayType to use VectorUtils.shouldBeFixedSizeList
    VectorUtils.shouldBeFixedSizeList(ArrayType(elementType, true), metadata)
  }

  /* Copy from copy of org.apache.spark.sql.errors.ExecutionErrors for Spark version compatibility */
  private def unsupportedDataTypeError(typeName: DataType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> ("\"" + typeName.sql.toUpperCase(Locale.ROOT) + "\"")))
  }

  /* Copy from copy of org.apache.spark.sql.errors.ExecutionErrors for Spark version compatibility */
  private def duplicatedFieldNameInArrowStructError(fieldNames: Seq[String])
      : SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
      messageParameters = Map("fieldNames" -> fieldNames.mkString("[", ", ", "]")))
  }

  private def isBlobField(field: Field): Boolean = {
    val metadata = field.getMetadata
    metadata != null && metadata.containsKey(ENCODING_BLOB) &&
    "true".equalsIgnoreCase(metadata.get(ENCODING_BLOB))
  }
}
