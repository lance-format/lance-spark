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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.types._
import org.lance.spark.LanceConstant
import org.lance.spark.utils.{BlobUtils, DateMilliUtils, FixedSizeBinaryUtils, Float16Utils, LargeVarCharUtils, VectorUtils}

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

object LanceArrowUtils {

  private val mapper = new ObjectMapper()

  val ARROW_FIXED_SIZE_LIST_SIZE_KEY = VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY
  val ARROW_FLOAT16_KEY = Float16Utils.ARROW_FLOAT16_KEY
  val ENCODING_BLOB = BlobUtils.LANCE_ENCODING_BLOB_KEY
  val ARROW_EXT_NAME_KEY = BlobUtils.ARROW_EXTENSION_NAME_KEY
  val BLOB_V2_EXT_NAME = BlobUtils.ARROW_EXTENSION_BLOB_V2
  val ARROW_LARGE_VAR_CHAR_KEY = LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_KEY
  val ARROW_DATE_MILLISECOND_KEY = DateMilliUtils.ARROW_DATE_MILLISECOND_KEY
  val ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY =
    FixedSizeBinaryUtils.ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY

  // Namespaced keys used to embed child Spark Metadata on a parent StructField when the
  // child sits inside an ArrayType/MapType — Spark has no per-element metadata slot of its
  // own. The value is the child Metadata serialized as JSON (the same shape produced by
  // `Metadata#json`), so deeply nested cases work transparently.
  val LANCE_ELEMENT_METADATA_KEY = "_lance.element"
  val LANCE_MAP_KEY_METADATA_KEY = "_lance.key"
  val LANCE_MAP_VALUE_METADATA_KEY = "_lance.value"

  // Lance-Spark convention: UDT class FQN on the Arrow field's user metadata. Write stamps it
  // from udt.getClass.getName when udt.sqlType is a StructType; read consumes it to rewrap the
  // field's dataType. UDTs with non-struct sqlType are written via their underlying type without
  // the marker (the UDT wrapper degrades to sqlType on read-back). Spark's own sources don't
  // use this key — Parquet round-trips UDT info via a file-level serialized schema
  // (ParquetReadSupport.SPARK_METADATA_KEY) instead.
  val LANCE_UDT_CLASS_KEY = "__udt"

  private val LANCE_INTERNAL_METADATA_KEYS = Set(
    LANCE_ELEMENT_METADATA_KEY,
    LANCE_MAP_KEY_METADATA_KEY,
    LANCE_MAP_VALUE_METADATA_KEY)

  def fromArrowField(field: Field): DataType = {
    val baseType = convertArrowFieldType(field)
    val udtFqn = field.getMetadata.get(LANCE_UDT_CLASS_KEY)
    if (udtFqn == null) {
      baseType
    } else {
      val udt = loadUdt(udtFqn, field.getName)
      if (!udt.sqlType.sameType(baseType)) {
        throw new IllegalStateException(
          s"UDT '$udtFqn' for field '${field.getName}' has sqlType ${udt.sqlType} " +
            s"which does not match the persisted type $baseType")
      }
      udt
    }
  }

  private def convertArrowFieldType(field: Field): DataType = {
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
      case _: ArrowType.Struct if isBlobField(field) =>
        BinaryType
      case _: ArrowType.Struct =>
        // Always recurse through LanceArrowUtils for struct children so special cases
        // like Date(MILLISECOND), FixedSizeBinary, etc. are applied in nested schemas too.
        val blobField = isBlobField(field)
        val fields = field.getChildren.asScala.map { childField =>
          val childType = childField.getType match {
            // Lance returns blob columns as structs with unsigned Int64 position/size fields.
            case int: ArrowType.Int
                if blobField && !int.getIsSigned && int.getBitWidth == 8 * 8 =>
              LongType
            case _ => fromArrowField(childField)
          }
          StructField(
            childField.getName,
            childType,
            childField.isNullable,
            buildFieldMetadata(childField))
        }.toArray
        StructType(fields)
      case largeBinary: ArrowType.LargeBinary if isBlobField(field) =>
        // Lance returns LargeBinary in schema but Struct in data for blob columns
        // We need to handle this as binary to match the schema
        BinaryType
      case _: ArrowType.LargeUtf8 =>
        // LargeUtf8 maps back to StringType in Spark
        StringType
      case _: ArrowType.LargeBinary =>
        // LargeBinary maps back to BinaryType in Spark
        BinaryType
      case _: ArrowType.FixedSizeBinary =>
        // Spark ArrowUtils doesn't support FixedSizeBinary. Read as BinaryType.
        BinaryType
      case date: ArrowType.Date =>
        // Spark ArrowUtils doesn't support Date(MILLISECOND). Normalize both Arrow date units
        // to Spark DateType.
        date.getUnit match {
          case DateUnit.DAY => DateType
          case DateUnit.MILLISECOND => DateType
        }
      case ts: ArrowType.Timestamp =>
        if (ts.getTimezone != null && ts.getTimezone.nonEmpty) TimestampType
        else TimestampNTZType
      case l: ArrowType.List =>
        val children = field.getChildren
        if (children.isEmpty) {
          throw new SparkException(s"List field ${field.getName} has no children")
        }
        val elementField = children.get(0)
        val elementType = fromArrowField(elementField)
        val containsNull = elementField.isNullable
        ArrayType(elementType, containsNull)
      case _: ArrowType.Map =>
        // Keep map conversion recursive to avoid delegating nested unsupported Arrow types
        // back to Spark ArrowUtils.
        val children = field.getChildren
        if (children.isEmpty) {
          throw new SparkException(s"Map field ${field.getName} has no children")
        }
        val entriesField = children.get(0)
        val entryChildren = entriesField.getChildren
        if (entryChildren.size() < 2) {
          throw new SparkException(
            s"Map field ${field.getName} has invalid entries struct: expected key/value children")
        }
        val keyField = entryChildren.get(0)
        val valueField = entryChildren.get(1)
        MapType(
          fromArrowField(keyField),
          fromArrowField(valueField),
          valueField.isNullable)
      case fp: ArrowType.FloatingPoint
          if fp.getPrecision == FloatingPointPrecision.HALF =>
        // Widen float16 to float32 for Spark (Spark has no native float16 type)
        FloatType
      case _: ArrowType.Time =>
        // Spark's own ArrowUtils accepts only Time(NANOSECOND, 64); other units throw.
        // We accept all four Arrow Time variants (Sec/Milli/Micro/Nano) and rescale to nanos
        // in TimeUnitAccessor so Lance datasets written by non-Spark producers (Arrow,
        // DataFusion, Polars) round-trip cleanly.
        // TimeType is Spark 4.1+ only; fall back to LongType (raw nanos) on older versions.
        TimeUtils.resolveSparkTimeType()
      case d: ArrowType.Decimal if d.getBitWidth == 256 =>
        // Spark only supports 128-bit decimal (precision <= 38).
        // Throw at schema time rather than a cryptic UnsupportedOperationException
        // when LanceArrowColumnVector tries to read the data.
        throw unsupportedDataTypeError(
          s"DECIMAL256(${d.getPrecision}, ${d.getScale}) in column '${field.getName}'." +
            " Lance-Spark only supports 128-bit decimal (precision <= 38)." +
            " Consider converting the column to a compatible DECIMAL type before reading.")
      case _ => ArrowUtils.fromArrowField(field)
    }
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      StructField(
        field.getName,
        fromArrowField(field),
        field.isNullable,
        buildFieldMetadata(field))
    }.toArray)
  }

  /**
   * Build Spark Metadata for the given Arrow field, capturing type information that Spark's
   * DataType cannot represent on its own (Date(MILLISECOND), LargeUtf8, Float16, FixedSizeList
   * size). For Arrow types that nest a Spark DataType lacking a metadata slot for its element
   * (List, FixedSizeList, Map), the child's Spark Metadata is serialized as JSON under
   * `_lance.element`, `_lance.key`, and `_lance.value` so writeback can reconstruct it.
   */
  private[util] def buildFieldMetadata(field: Field): Metadata = {
    // Strip __udt — fromArrowField has already consumed it to rewrap the dataType, so surfacing
    // it here would be noise. Re-write is safe: toArrowField's UDT branch re-derives the marker
    // from udt.getClass.getName.
    val filteredArrowMeta = field.getMetadata.asScala.toMap - LANCE_UDT_CLASS_KEY
    val arrowMeta = Metadata.fromJson(mapper.writeValueAsString(filteredArrowMeta.asJava))
    val builder = new MetadataBuilder().withMetadata(arrowMeta)
    augmentTypeMarkers(builder, field)
    augmentChildMetadata(builder, field)
    builder.build()
  }

  private def augmentTypeMarkers(builder: MetadataBuilder, field: Field): Unit = {
    field.getType match {
      case fixedSizeList: ArrowType.FixedSizeList =>
        builder.putLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY, fixedSizeList.getListSize.toLong)
        if (Float16Utils.isFloat16ArrowField(field)) {
          builder.putString(ARROW_FLOAT16_KEY, Float16Utils.ARROW_FLOAT16_VALUE)
        }
      case _: ArrowType.LargeUtf8 =>
        builder.putString(ARROW_LARGE_VAR_CHAR_KEY, LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_VALUE)
      case date: ArrowType.Date if date.getUnit == DateUnit.MILLISECOND =>
        builder.putString(ARROW_DATE_MILLISECOND_KEY, DateMilliUtils.ARROW_DATE_MILLISECOND_VALUE)
      case fsb: ArrowType.FixedSizeBinary =>
        // Preserve FixedSizeBinary byte width so a subsequent write reproduces
        // FixedSizeBinary(n) instead of falling back to variable-length Binary.
        builder.putLong(ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY, fsb.getByteWidth.toLong)
      case _ =>
    }
  }

  private def augmentChildMetadata(builder: MetadataBuilder, field: Field): Unit = {
    field.getType match {
      case _: ArrowType.FixedSizeList | _: ArrowType.List =>
        val children = field.getChildren
        if (!children.isEmpty && !Float16Utils.isFloat16ArrowField(field)) {
          // For Float16 vectors, the child's HALF type is encoded in `arrow.float16` on the
          // parent, so there is no extra child-side metadata worth embedding.
          val childMeta = buildFieldMetadata(children.get(0))
          if (hasContent(childMeta)) {
            builder.putString(LANCE_ELEMENT_METADATA_KEY, childMeta.json)
          }
        }
      case _: ArrowType.Map =>
        val children = field.getChildren
        if (!children.isEmpty) {
          val entryChildren = children.get(0).getChildren
          if (entryChildren.size() >= 2) {
            val keyMeta = buildFieldMetadata(entryChildren.get(0))
            val valueMeta = buildFieldMetadata(entryChildren.get(1))
            if (hasContent(keyMeta)) {
              builder.putString(LANCE_MAP_KEY_METADATA_KEY, keyMeta.json)
            }
            if (hasContent(valueMeta)) {
              builder.putString(LANCE_MAP_VALUE_METADATA_KEY, valueMeta.json)
            }
          }
        }
      case _ =>
    }
  }

  private def hasContent(metadata: Metadata): Boolean =
    metadata != null && metadata.json != "{}"

  private def parseEmbeddedMetadata(parent: Metadata, key: String): Metadata = {
    if (parent == null || !parent.contains(key)) {
      Metadata.empty
    } else {
      Metadata.fromJson(parent.getString(key))
    }
  }

  def toArrowSchema(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean): Schema = {
    toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes = false)
  }

  def toArrowSchema(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean): Schema = {
    new Schema(schema.map { field =>
      toArrowField(
        field.name,
        deduplicateFieldNames(field.dataType, errorOnDuplicatedFieldNames),
        field.nullable,
        timeZoneId,
        field.metadata,
        largeVarTypes)
    }.asJava)
  }

  def toArrowField(
      name: String,
      dt: DataType,
      nullable: Boolean,
      timeZoneId: String,
      metadata: org.apache.spark.sql.types.Metadata = null,
      largeVarTypes: Boolean = false): Field = {
    var large: Boolean = largeVarTypes
    var meta: Map[String, String] = Map.empty

    if (metadata != null) {
      if (metadata.contains(ENCODING_BLOB)
        && metadata.getString(ENCODING_BLOB).equalsIgnoreCase("true")) {
        large = true
      }
      if (metadata.contains(ARROW_LARGE_VAR_CHAR_KEY)
        && metadata.getString(ARROW_LARGE_VAR_CHAR_KEY).equalsIgnoreCase("true")) {
        large = true
      }

      meta = mapper
        .readValue(metadata.json, classOf[java.util.LinkedHashMap[_, _]])
        .asScala
        .collect {
          case (k, v) if !LANCE_INTERNAL_METADATA_KEYS.contains(k.toString) =>
            (k.toString, String.valueOf(v))
        }
        .toMap
    }

    dt match {
      case ArrayType(elementType, containsNull) =>
        val elementMetadata = parseEmbeddedMetadata(metadata, LANCE_ELEMENT_METADATA_KEY)
        if (shouldBeFixedSizeList(metadata, elementType)) {
          val listSize = metadata.getLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY).toInt
          val fieldType =
            new FieldType(nullable, new ArrowType.FixedSizeList(listSize), null, meta.asJava)
          // Check if float16 metadata is set
          val isFloat16 = Float16Utils.hasFloat16Metadata(metadata)
          val elementField = if (isFloat16) {
            if (!Float16Utils.isFloat2VectorAvailable) {
              throw new UnsupportedOperationException(
                "Float16 vectors require Arrow 18+ (Spark 4.0+). " +
                  "Current Arrow version does not support Float2Vector.")
            }
            new Field(
              "element",
              new FieldType(
                containsNull,
                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF),
                null),
              Seq.empty[Field].asJava)
          } else {
            toArrowField(
              "element",
              elementType,
              containsNull,
              timeZoneId,
              elementMetadata,
              largeVarTypes)
          }
          new Field(
            name,
            fieldType,
            Seq(elementField).asJava)
        } else {
          val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null, meta.asJava)
          new Field(
            name,
            fieldType,
            Seq(
              toArrowField(
                "element",
                elementType,
                containsNull,
                timeZoneId,
                elementMetadata,
                largeVarTypes)).asJava)
        }
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null, meta.asJava)
        new Field(
          name,
          fieldType,
          fields.map { field =>
            toArrowField(
              field.name,
              field.dataType,
              // Relax child nullability when the parent is nullable. A null parent row forces
              // StructWriter.setNull() to advance every child's count (to keep child vectors
              // aligned with the parent StructVector), and Lance's Rust writer rejects those
              // null placeholders if any child field is non-nullable. Scoping the relaxation
              // to nullable parents keeps user-declared non-nullability on top-level fields,
              // and propagates naturally into nested structs. Most common unblocked case: UDT
              // columns like VectorUDT / MatrixUDT whose sqlType marks children as non-nullable.
              nullable || field.nullable,
              timeZoneId,
              field.metadata,
              largeVarTypes)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val keyMetadata = parseEmbeddedMetadata(metadata, LANCE_MAP_KEY_METADATA_KEY)
        val valueMetadata = parseEmbeddedMetadata(metadata, LANCE_MAP_VALUE_METADATA_KEY)
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null, meta.asJava)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(
          name,
          mapType,
          Seq(toArrowField(
            MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false, keyMetadata)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull, valueMetadata),
            nullable = false,
            timeZoneId,
            largeVarTypes = largeVarTypes)).asJava)
      case udt: UserDefinedType[_] =>
        // Unwrap to sqlType for storage (Arrow has no UDT concept). When sqlType is a
        // StructType, stamp the UDT FQN as a field-level metadata marker so the read side
        // can rewrap automatically. For non-struct sqlTypes the marker is skipped — the data
        // still round-trips via the underlying type, but the UDT wrapper degrades. All real
        // MLlib UDTs (VectorUDT, MatrixUDT) use struct sqlType, so this restriction matches
        // every supported case while keeping `__udt` off arrow fields where it has no use.
        udt.sqlType match {
          case _: StructType =>
            val baseBuilder = new MetadataBuilder()
            if (metadata != null) {
              baseBuilder.withMetadata(metadata)
            }
            val udtMeta = baseBuilder.putString(LANCE_UDT_CLASS_KEY, udt.getClass.getName).build()
            toArrowField(name, udt.sqlType, nullable, timeZoneId, udtMeta, largeVarTypes)
          case _ =>
            toArrowField(name, udt.sqlType, nullable, timeZoneId, metadata, largeVarTypes)
        }
      case DateType if DateMilliUtils.hasDateMilliMetadata(metadata) =>
        val fieldType = new FieldType(
          nullable,
          new ArrowType.Date(DateUnit.MILLISECOND),
          null,
          meta.asJava)
        new Field(name, fieldType, Seq.empty[Field].asJava)
      case BinaryType if FixedSizeBinaryUtils.hasFixedSizeBinaryMetadata(metadata) =>
        val byteWidth = metadata.getLong(ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY).toInt
        val fieldType = new FieldType(
          nullable,
          new ArrowType.FixedSizeBinary(byteWidth),
          null,
          meta.asJava)
        new Field(name, fieldType, Seq.empty[Field].asJava)
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
    // TimeType is Spark 4.1+ only. Use class name check for cross-version compatibility.
    case dt if dt.getClass.getName == "org.apache.spark.sql.types.TimeType" =>
      new ArrowType.Time(TimeUnit.NANOSECOND, 64)
    // In Spark 3.4/3.5, CharType and VarcharType extend AtomicType (not StringType),
    // so `case _: StringType` above does not match them. On Spark 4.0+ they extend
    // StringType and are already caught above, making these branches harmlessly unreachable.
    // Kept for Spark 3.x compatibility. Length constraints are intentionally discarded —
    // Arrow has no varchar concept.
    case _: CharType | _: VarcharType if largeVarTypes => ArrowType.LargeUtf8.INSTANCE
    case _: CharType | _: VarcharType => ArrowType.Utf8.INSTANCE
    case _ =>
      throw unsupportedDataTypeError(dt)
  }

  private def deduplicateFieldNames(
      dt: DataType,
      errorOnDuplicatedFieldNames: Boolean): DataType = dt match {
    // UDTs have a fixed sqlType defined by the UDT class; recursing here would either be a no-op
    // or break the UDT contract. Preserving the UDT lets toArrowField's UDT branch stamp __udt
    // and unwrap at every nesting depth (top-level, inside StructType, ArrayType element, etc.).
    case udt: UserDefinedType[_] => udt
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

  /**
   * Reflective UDT instantiation for the read-side rewrap. Two non-obvious choices:
   *   - Thread context classloader (with this class's loader as fallback) so user-provided UDTs
   *     shipped via --jars / spark.jars resolve.
   *   - setAccessible(true) is required for `private[spark]` UDTs like VectorUDT (constructor is
   *     JVM package-private). On JDK 9+ this can throw InaccessibleObjectException unless the
   *     spark module is opened — Spark's own --add-opens flags cover that.
   */
  private def loadUdt(fqn: String, fieldName: String): UserDefinedType[_] = {
    val loader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
    // initialize=false: defer the class's static initializer until we've verified the class is
    // actually a UserDefinedType. The FQN is read from Lance manifest data; without this guard a
    // crafted manifest could trigger arbitrary static-init side effects on any class that happens
    // to be on the reader's classpath. newInstance() below triggers initialization at use time.
    val cls =
      try {
        Class.forName(fqn, false, loader)
      } catch {
        case e: ClassNotFoundException =>
          throw new IllegalStateException(
            s"UDT class '$fqn' for field '$fieldName' could not be loaded",
            e)
      }
    if (!classOf[UserDefinedType[_]].isAssignableFrom(cls)) {
      throw new IllegalStateException(
        s"Class '$fqn' named in '__udt' metadata of field '$fieldName' is not a UserDefinedType")
    }
    val ctor =
      try {
        cls.getDeclaredConstructor()
      } catch {
        case e: NoSuchMethodException =>
          throw new IllegalStateException(
            s"UDT class '$fqn' for field '$fieldName' has no no-arg constructor",
            e)
      }
    try {
      ctor.setAccessible(true)
    } catch {
      case e: RuntimeException =>
        throw new IllegalStateException(
          s"UDT class '$fqn' for field '$fieldName' could not be made accessible " +
            "(reflective access denied; check JDK module opens / Java security manager " +
            "configuration)",
          e)
    }
    try {
      ctor.newInstance().asInstanceOf[UserDefinedType[_]]
    } catch {
      case e: ReflectiveOperationException =>
        throw new IllegalStateException(
          s"UDT class '$fqn' for field '$fieldName' could not be instantiated",
          e)
    }
  }

  /* Copy from copy of org.apache.spark.sql.errors.ExecutionErrors for Spark version compatibility */
  private def unsupportedDataTypeError(typeName: DataType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> ("\"" + typeName.sql.toUpperCase(Locale.ROOT) + "\"")))
  }

  /* For unsupported Arrow types where a DataType cannot be constructed (e.g. DECIMAL256) */
  private def unsupportedDataTypeError(typeName: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> ("\"" + typeName + "\"")))
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
    if (metadata == null) return false
    (metadata.containsKey(ENCODING_BLOB) &&
      "true".equalsIgnoreCase(metadata.get(ENCODING_BLOB))) ||
    BLOB_V2_EXT_NAME.equals(metadata.get(ARROW_EXT_NAME_KEY))
  }
}

/**
 * Resolves Spark TimeType via reflection for cross-version compatibility.
 * TimeType is only available in Spark 4.1+.
 */
private[util] object TimeUtils {
  @volatile private var timeTypeResult: Option[DataType] = null

  /**
   * Returns Spark TimeType (default precision) if available (Spark 4.1+), otherwise LongType.
   */
  def resolveSparkTimeType(): DataType = {
    if (timeTypeResult == null) {
      synchronized {
        if (timeTypeResult == null) {
          timeTypeResult =
            try {
              // TimeType$ is the Scala companion object; its apply() returns TimeType
              // with default precision (MICROS_PRECISION = 6).
              val companionClass = Class.forName("org.apache.spark.sql.types.TimeType$")
              val module = companionClass.getField("MODULE$").get(null)
              val applyMethod = companionClass.getMethod("apply")
              Some(applyMethod.invoke(module).asInstanceOf[DataType])
            } catch {
              case _: ClassNotFoundException | _: NoSuchFieldException |
                  _: NoSuchMethodException => None
            }
        }
      }
    }
    timeTypeResult.getOrElse(LongType)
  }

  /**
   * Returns true if Spark TimeType is available (Spark 4.1+).
   */
  def isTimeTypeAvailable: Boolean = {
    resolveSparkTimeType() ne LongType
  }
}
