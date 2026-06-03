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
 * The following code is originally from https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/util/ArrowUtilsSuite.scala
 * and is licensed under the Apache license:
 *
 * License: Apache License 2.0, Copyright 2014 and onwards The Apache Software Foundation.
 * https://github.com/apache/spark/blob/master/LICENSE
 *
 * It has been modified by the Lance developers to fit the needs of the Lance project.
 */

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision}
import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.types._
import org.lance.spark.LanceConstant
import org.lance.spark.utils.{FixedSizeBinaryUtils, Float16Utils, LargeVarCharUtils, VectorUtils}
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId

class LanceArrowUtilsSuite extends AnyFunSuite {
  def roundtrip(dt: DataType, fieldName: String = "value"): Unit = {
    dt match {
      case schema: StructType =>
        assert(LanceArrowUtils.fromArrowSchema(
          LanceArrowUtils.toArrowSchema(schema, null, true)) === schema)
      case _ =>
        roundtrip(new StructType().add(fieldName, dt))
    }
  }

  test("unsigned") {
    roundtrip(BooleanType, LanceConstant.ROW_ID)
    val arrowType = LanceArrowUtils.toArrowField(LanceConstant.ROW_ID, LongType, true, "Beijing")
    assert(arrowType.getType.asInstanceOf[ArrowType.Int].getBitWidth === 64)
    assert(!arrowType.getType.asInstanceOf[ArrowType.Int].getIsSigned)
    // also verify unsigned smaller integers mapping (uint8/uint16/uint32)
    val u8Field = new Field(
      "u8",
      new FieldType(true, new ArrowType.Int(8, /* signed = */ false), null, null),
      java.util.Collections.emptyList())
    val u16Field = new Field(
      "u16",
      new FieldType(true, new ArrowType.Int(16, /* signed = */ false), null, null),
      java.util.Collections.emptyList())
    val u32Field = new Field(
      "u32",
      new FieldType(true, new ArrowType.Int(32, /* signed = */ false), null, null),
      java.util.Collections.emptyList())
    assert(LanceArrowUtils.fromArrowField(u8Field) === ShortType)
    assert(LanceArrowUtils.fromArrowField(u16Field) === IntegerType)
    assert(LanceArrowUtils.fromArrowField(u32Field) === LongType)
  }

  test("simple") {
    roundtrip(BooleanType)
    roundtrip(ByteType)
    roundtrip(ShortType)
    roundtrip(IntegerType)
    roundtrip(LongType)
    roundtrip(FloatType)
    roundtrip(DoubleType)
    roundtrip(StringType)
    roundtrip(BinaryType)
    roundtrip(DecimalType.SYSTEM_DEFAULT)
    roundtrip(DateType)
    roundtrip(YearMonthIntervalType())
    roundtrip(DayTimeIntervalType())
  }

  test("decimal256 field throws SparkUnsupportedOperationException") {
    val field = new Field(
      "amount",
      new FieldType(true, new ArrowType.Decimal(38, 5, 256), null, null),
      java.util.Collections.emptyList())
    val ex = intercept[SparkUnsupportedOperationException] {
      LanceArrowUtils.fromArrowField(field)
    }
    assert(ex.getMessage.contains("amount"), s"field name missing: ${ex.getMessage}")
    assert(ex.getMessage.contains("256"), s"bit-width missing: ${ex.getMessage}")
    assert(ex.getMessage.contains("128"), s"128-bit hint missing: ${ex.getMessage}")
    assert(ex.getMessage.contains("38"), s"precision limit hint missing: ${ex.getMessage}")
  }

  test("timestamp") {

    def roundtripWithTz(timeZoneId: String): Unit = {
      val schema = new StructType().add("value", TimestampType)
      val arrowSchema = LanceArrowUtils.toArrowSchema(schema, timeZoneId, true)
      val fieldType = arrowSchema.findField("value").getType.asInstanceOf[ArrowType.Timestamp]
      assert(fieldType.getTimezone() === timeZoneId)
      assert(LanceArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    roundtripWithTz(ZoneId.systemDefault().getId)
    roundtripWithTz("Asia/Tokyo")
    roundtripWithTz("UTC")
  }

  test("array") {
    roundtrip(ArrayType(IntegerType, containsNull = true))
    roundtrip(ArrayType(IntegerType, containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = false))
  }

  test("struct") {
    roundtrip(new StructType())
    roundtrip(new StructType().add("i", IntegerType))
    roundtrip(new StructType().add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add(
      "struct",
      new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType))))
  }

  test("nested date millisecond types") {
    val dateMilliField = new Field(
      "d",
      new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null, null),
      java.util.Collections.emptyList())
    val nestedStructField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(dateMilliField))

    val nestedStructType =
      LanceArrowUtils.fromArrowField(nestedStructField).asInstanceOf[StructType]
    assert(nestedStructType("d").dataType === DateType)

    val keyField = new Field(
      "key",
      new FieldType(false, ArrowType.Utf8.INSTANCE, null, null),
      java.util.Collections.emptyList())
    val valueField = new Field(
      "value",
      new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null, null),
      java.util.Collections.emptyList())
    val entriesField = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(keyField, valueField))
    val mapField = new Field(
      "m",
      new FieldType(true, new ArrowType.Map(false), null, null),
      java.util.Arrays.asList(entriesField))

    val mapType = LanceArrowUtils.fromArrowField(mapField).asInstanceOf[MapType]
    assert(mapType.keyType === StringType)
    assert(mapType.valueType === DateType)
    assert(mapType.valueContainsNull)
  }

  test("non-microsecond timestamp types") {
    import org.apache.arrow.vector.types.TimeUnit

    // Timestamp with timezone → TimestampType
    for (unit <- Seq(TimeUnit.SECOND, TimeUnit.MILLISECOND, TimeUnit.NANOSECOND)) {
      val field = new Field(
        "ts",
        new FieldType(true, new ArrowType.Timestamp(unit, "UTC"), null, null),
        java.util.Collections.emptyList())
      assert(
        LanceArrowUtils.fromArrowField(field) === TimestampType,
        s"Timestamp($unit, UTC) should map to TimestampType")
    }

    // Timestamp without timezone → TimestampNTZType
    for (unit <- Seq(TimeUnit.SECOND, TimeUnit.MILLISECOND, TimeUnit.NANOSECOND)) {
      val field = new Field(
        "ts",
        new FieldType(true, new ArrowType.Timestamp(unit, null), null, null),
        java.util.Collections.emptyList())
      assert(
        LanceArrowUtils.fromArrowField(field) === TimestampNTZType,
        s"Timestamp($unit, null) should map to TimestampNTZType")
    }
  }

  test("nested non-microsecond timestamp types") {
    import org.apache.arrow.vector.types.TimeUnit

    // Timestamp(SECOND, UTC) inside a struct
    val tsField = new Field(
      "ts",
      new FieldType(true, new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), null, null),
      java.util.Collections.emptyList())
    val structField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(tsField))

    val structType =
      LanceArrowUtils.fromArrowField(structField).asInstanceOf[StructType]
    assert(structType("ts").dataType === TimestampType)

    // Timestamp(NANOSECOND, null) as map value
    val keyField = new Field(
      "key",
      new FieldType(false, ArrowType.Utf8.INSTANCE, null, null),
      java.util.Collections.emptyList())
    val valueField = new Field(
      "value",
      new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null, null),
      java.util.Collections.emptyList())
    val entriesField = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(keyField, valueField))
    val mapField = new Field(
      "m",
      new FieldType(true, new ArrowType.Map(false), null, null),
      java.util.Arrays.asList(entriesField))

    val mapType = LanceArrowUtils.fromArrowField(mapField).asInstanceOf[MapType]
    assert(mapType.keyType === StringType)
    assert(mapType.valueType === TimestampNTZType)
  }

  test("struct with duplicated field names") {

    def check(dt: DataType, expected: DataType): Unit = {
      val schema = new StructType().add("value", dt)
      intercept[SparkUnsupportedOperationException] {
        LanceArrowUtils.toArrowSchema(schema, null, true)
      }
      assert(LanceArrowUtils.fromArrowSchema(LanceArrowUtils.toArrowSchema(schema, null, false))
        === new StructType().add("value", expected))
    }

    roundtrip(new StructType().add("i", IntegerType).add("i", StringType))

    check(
      new StructType().add("i", IntegerType).add("i", StringType),
      new StructType().add("i_0", IntegerType).add("i_1", StringType))
    check(
      ArrayType(new StructType().add("i", IntegerType).add("i", StringType)),
      ArrayType(new StructType().add("i_0", IntegerType).add("i_1", StringType)))
    check(
      MapType(StringType, new StructType().add("i", IntegerType).add("i", StringType)),
      MapType(StringType, new StructType().add("i_0", IntegerType).add("i_1", StringType)))
  }

  // CharType/VarcharType reach the connector as distinct types when tables are created via
  // SQL DDL through a V2 catalog (Spark 3.1+ default: charVarcharAsString=false). These unit
  // tests exercise the conversion functions directly, without requiring SparkSession.
  test("CharType maps to Arrow Utf8") {
    val field = LanceArrowUtils.toArrowField("v", CharType(10), nullable = true, "UTC")
    assert(field.getType === ArrowType.Utf8.INSTANCE)
  }

  test("VarcharType maps to Arrow Utf8") {
    val field = LanceArrowUtils.toArrowField("v", VarcharType(50), nullable = true, "UTC")
    assert(field.getType === ArrowType.Utf8.INSTANCE)
  }

  test("CharType maps to Arrow LargeUtf8 when largeVarTypes is true") {
    val field = LanceArrowUtils.toArrowField(
      "v",
      CharType(10),
      nullable = true,
      "UTC",
      largeVarTypes = true)
    assert(field.getType === ArrowType.LargeUtf8.INSTANCE)
  }

  test("VarcharType maps to Arrow LargeUtf8 when largeVarTypes is true") {
    val field = LanceArrowUtils.toArrowField(
      "v",
      VarcharType(50),
      nullable = true,
      "UTC",
      largeVarTypes = true)
    assert(field.getType === ArrowType.LargeUtf8.INSTANCE)
  }

  test("large varchar metadata produces LargeUtf8 arrow type") {
    import org.lance.spark.utils.LargeVarCharUtils

    val largeVarCharMetadata = new MetadataBuilder()
      .putString(
        LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_KEY,
        LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_VALUE)
      .build()

    val schema = new StructType()
      .add("regular_string", StringType, nullable = true)
      .add("large_string", StringType, nullable = true, largeVarCharMetadata)

    val arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", false)

    // Regular string should use Utf8
    val regularField = arrowSchema.findField("regular_string")
    assert(regularField.getType === ArrowType.Utf8.INSTANCE)

    // Large string with metadata should use LargeUtf8
    val largeField = arrowSchema.findField("large_string")
    assert(largeField.getType === ArrowType.LargeUtf8.INSTANCE)
  }

  test("date millisecond metadata preserved in fromArrowSchema") {
    val dateMilliField = new Field(
      "dt",
      new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null, null),
      java.util.Collections.emptyList())
    val schema = new Schema(java.util.Arrays.asList(dateMilliField))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(schema)
    assert(sparkSchema("dt").dataType === DateType)
    assert(sparkSchema("dt").metadata.contains(LanceArrowUtils.ARROW_DATE_MILLISECOND_KEY))
    assert(
      sparkSchema("dt").metadata.getString(
        LanceArrowUtils.ARROW_DATE_MILLISECOND_KEY) === "true")
  }

  test("date millisecond metadata produces Date(MILLISECOND) arrow type") {
    val dayCol = StructField("day_col", DateType, nullable = true)
    val milliMeta = new MetadataBuilder()
      .putString(LanceArrowUtils.ARROW_DATE_MILLISECOND_KEY, "true")
      .build()
    val milliCol = StructField("milli_col", DateType, nullable = true, milliMeta)
    val sparkSchema = StructType(Seq(dayCol, milliCol))
    val arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    assert(arrowSchema.findField("day_col").getType === new ArrowType.Date(DateUnit.DAY))
    assert(
      arrowSchema.findField("milli_col").getType === new ArrowType.Date(DateUnit.MILLISECOND))
  }

  test("date millisecond roundtrip Arrow -> Spark -> Arrow") {
    val dateMilliField = new Field(
      "dt",
      new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null, null),
      java.util.Collections.emptyList())
    val arrowSchema = new Schema(java.util.Arrays.asList(dateMilliField))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrowSchema)
    val arrowSchemaBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    assert(
      arrowSchemaBack.findField("dt").getType === new ArrowType.Date(DateUnit.MILLISECOND))
  }

  test("date millisecond metadata preserved in nested struct") {
    val dateMilliChild = new Field(
      "nested_dt",
      new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null, null),
      java.util.Collections.emptyList())
    val structField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(dateMilliChild))
    val schema = new Schema(java.util.Arrays.asList(structField))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(schema)
    val structType = sparkSchema("s").dataType.asInstanceOf[StructType]
    assert(structType("nested_dt").dataType === DateType)
    assert(
      structType("nested_dt").metadata.contains(LanceArrowUtils.ARROW_DATE_MILLISECOND_KEY))
    assert(
      structType("nested_dt").metadata.getString(
        LanceArrowUtils.ARROW_DATE_MILLISECOND_KEY) === "true")
  }

  // Helpers for building Arrow fields used by the nested-metadata tests below.
  private def listField(name: String, child: Field): Field = {
    new Field(
      name,
      new FieldType(true, ArrowType.List.INSTANCE, null, null),
      java.util.Arrays.asList(child))
  }

  private def fixedSizeListField(name: String, size: Int, child: Field): Field = {
    new Field(
      name,
      new FieldType(true, new ArrowType.FixedSizeList(size), null, null),
      java.util.Arrays.asList(child))
  }

  private def mapField(name: String, keyField: Field, valueField: Field): Field = {
    val entriesField = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(keyField, valueField))
    new Field(
      name,
      new FieldType(true, new ArrowType.Map(false), null, null),
      java.util.Arrays.asList(entriesField))
  }

  private def primitiveField(
      name: String,
      arrowType: ArrowType,
      nullable: Boolean = true): Field = {
    new Field(
      name,
      new FieldType(nullable, arrowType, null, null),
      java.util.Collections.emptyList())
  }

  test("Date(MILLISECOND) inside Array roundtrips to Date(MILLISECOND)") {
    val arrow = new Schema(java.util.Arrays.asList(listField(
      "arr",
      primitiveField("element", new ArrowType.Date(DateUnit.MILLISECOND)))))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    assert(sparkSchema("arr").dataType === ArrayType(DateType, containsNull = true))

    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val elementType = arrowBack.findField("arr").getChildren.get(0).getType
    assert(
      elementType === new ArrowType.Date(DateUnit.MILLISECOND),
      s"Array element should remain Date(MILLISECOND), got $elementType")
  }

  test("LargeUtf8 inside Array roundtrips to LargeUtf8") {
    val arrow = new Schema(java.util.Arrays.asList(listField(
      "arr",
      primitiveField("element", ArrowType.LargeUtf8.INSTANCE))))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    assert(sparkSchema("arr").dataType === ArrayType(StringType, containsNull = true))

    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val elementType = arrowBack.findField("arr").getChildren.get(0).getType
    assert(
      elementType === ArrowType.LargeUtf8.INSTANCE,
      s"Array element should remain LargeUtf8, got $elementType")
  }

  test("LargeUtf8 inside FixedSizeList roundtrips to LargeUtf8") {
    val arrow = new Schema(java.util.Arrays.asList(fixedSizeListField(
      "fsl",
      3,
      primitiveField("element", ArrowType.LargeUtf8.INSTANCE))))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    // FixedSizeList<LargeUtf8> is not a Lance vector (element is not numeric), so it
    // collapses to a regular List on writeback. Element type fidelity is still required.
    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val elementType = arrowBack.findField("fsl").getChildren.get(0).getType
    assert(
      elementType === ArrowType.LargeUtf8.INSTANCE,
      s"FixedSizeList element should remain LargeUtf8, got $elementType")
  }

  test("Date(MILLISECOND) as Map value roundtrips to Date(MILLISECOND)") {
    val arrow = new Schema(java.util.Arrays.asList(mapField(
      "m",
      primitiveField("key", ArrowType.Utf8.INSTANCE, nullable = false),
      primitiveField("value", new ArrowType.Date(DateUnit.MILLISECOND)))))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    assert(sparkSchema("m").dataType === MapType(StringType, DateType, valueContainsNull = true))

    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val valueType = arrowBack
      .findField("m").getChildren.get(0)
      .getChildren.get(1).getType
    assert(
      valueType === new ArrowType.Date(DateUnit.MILLISECOND),
      s"Map value should remain Date(MILLISECOND), got $valueType")
  }

  test("LargeUtf8 as Map key and Date(MILLISECOND) as Map value both preserved") {
    val arrow = new Schema(java.util.Arrays.asList(mapField(
      "m",
      primitiveField("key", ArrowType.LargeUtf8.INSTANCE, nullable = false),
      primitiveField("value", new ArrowType.Date(DateUnit.MILLISECOND)))))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val entries = arrowBack.findField("m").getChildren.get(0).getChildren
    assert(entries.get(0).getType === ArrowType.LargeUtf8.INSTANCE)
    assert(entries.get(1).getType === new ArrowType.Date(DateUnit.MILLISECOND))
  }

  test("nested Array<Array<Date(MILLISECOND)>> roundtrips") {
    val arrow = new Schema(java.util.Arrays.asList(listField(
      "outer",
      listField("element", primitiveField("element", new ArrowType.Date(DateUnit.MILLISECOND))))))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val inner = arrowBack.findField("outer").getChildren.get(0)
    val innermost = inner.getChildren.get(0)
    assert(inner.getType === ArrowType.List.INSTANCE)
    assert(
      innermost.getType === new ArrowType.Date(DateUnit.MILLISECOND),
      s"Innermost element should remain Date(MILLISECOND), got ${innermost.getType}")
  }

  test("FixedSizeList(Float16) nested inside an Array preserves size + float16 markers") {
    val float16Element = primitiveField(
      "element",
      new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))
    val arrow = new Schema(java.util.Arrays.asList(listField(
      "vectors",
      fixedSizeListField("element", 4, float16Element))))

    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    // Read side: outer ArrayType must carry the inner FixedSizeList's metadata
    // (size + float16 marker) under the namespaced _lance.element key, so writeback
    // can restore it on Spark 4.0+ runtimes that support Float2Vector.
    val outerMeta = sparkSchema("vectors").metadata
    assert(outerMeta.contains(LanceArrowUtils.LANCE_ELEMENT_METADATA_KEY))
    val elementMeta = org.apache.spark.sql.types.Metadata.fromJson(
      outerMeta.getString(LanceArrowUtils.LANCE_ELEMENT_METADATA_KEY))
    assert(elementMeta.contains(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY))
    assert(elementMeta.getLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY) === 4L)
    assert(elementMeta.contains(Float16Utils.ARROW_FLOAT16_KEY))
    assert(elementMeta.getString(Float16Utils.ARROW_FLOAT16_KEY) === "true")

    // Write side only runs on Arrow 18+ (Spark 4.0+). On older Arrow versions
    // toArrowSchema rightly refuses to materialize a Float16 vector.
    if (Float16Utils.isFloat2VectorAvailable) {
      val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
      val inner = arrowBack.findField("vectors").getChildren.get(0)
      assert(inner.getType.isInstanceOf[ArrowType.FixedSizeList])
      assert(inner.getType.asInstanceOf[ArrowType.FixedSizeList].getListSize === 4)
      assert(Float16Utils.isFloat16ArrowField(inner))
    }
  }

  test("Array<Date(MILLISECOND)> nested inside a Struct roundtrips") {
    val dateInArray = listField(
      "arr",
      primitiveField("element", new ArrowType.Date(DateUnit.MILLISECOND)))
    val structField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(dateInArray))
    val arrow = new Schema(java.util.Arrays.asList(structField))

    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val innerArr = arrowBack.findField("s").getChildren.get(0)
    val element = innerArr.getChildren.get(0)
    assert(
      element.getType === new ArrowType.Date(DateUnit.MILLISECOND),
      s"Struct->Array element should remain Date(MILLISECOND), got ${element.getType}")
  }

  test("LargeUtf8 inside a Struct field preserves LargeUtf8 marker on writeback") {
    val struct = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(primitiveField("name", ArrowType.LargeUtf8.INSTANCE)))
    val arrow = new Schema(java.util.Arrays.asList(struct))
    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrow)
    val arrowBack = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val nameType = arrowBack.findField("s").getChildren.get(0).getType
    assert(
      nameType === ArrowType.LargeUtf8.INSTANCE,
      s"Struct child should remain LargeUtf8, got $nameType")
  }

  test("Arrow Time types map to TimeType or LongType") {
    import org.apache.arrow.vector.types.TimeUnit

    // All four Arrow Time types should be handled by fromArrowField
    val timeFields = Seq(
      ("time_nano", new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
      ("time_micro", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
      ("time_milli", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
      ("time_sec", new ArrowType.Time(TimeUnit.SECOND, 32)))

    for ((name, arrowTimeType) <- timeFields) {
      val field = new Field(
        name,
        new FieldType(true, arrowTimeType, null, null),
        java.util.Collections.emptyList())
      val sparkType = LanceArrowUtils.fromArrowField(field)
      // On Spark 4.1+ this should be TimeType, on older Spark it should be LongType
      val expectedType = TimeUtils.resolveSparkTimeType()
      assert(
        sparkType === expectedType,
        s"Arrow $arrowTimeType should map to $expectedType, got $sparkType")
    }
  }

  test("nested Arrow Time types") {
    import org.apache.arrow.vector.types.TimeUnit

    // Time field inside a struct
    val timeField = new Field(
      "t",
      new FieldType(true, new ArrowType.Time(TimeUnit.NANOSECOND, 64), null, null),
      java.util.Collections.emptyList())
    val structField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(timeField))

    val structType =
      LanceArrowUtils.fromArrowField(structField).asInstanceOf[StructType]
    val expectedType = TimeUtils.resolveSparkTimeType()
    assert(structType("t").dataType === expectedType)
  }

  test("nested Arrow Time types in list and map") {
    import org.apache.arrow.vector.types.TimeUnit

    val expectedType = TimeUtils.resolveSparkTimeType()

    // Time as list element
    val timeElemField = new Field(
      "element",
      new FieldType(true, new ArrowType.Time(TimeUnit.MICROSECOND, 64), null, null),
      java.util.Collections.emptyList())
    val listField = new Field(
      "l",
      new FieldType(true, ArrowType.List.INSTANCE, null, null),
      java.util.Arrays.asList(timeElemField))

    val arrayType = LanceArrowUtils.fromArrowField(listField).asInstanceOf[ArrayType]
    assert(arrayType.elementType === expectedType)

    // Time as map value
    val keyField = new Field(
      "key",
      new FieldType(false, ArrowType.Utf8.INSTANCE, null, null),
      java.util.Collections.emptyList())
    val valueField = new Field(
      "value",
      new FieldType(true, new ArrowType.Time(TimeUnit.SECOND, 32), null, null),
      java.util.Collections.emptyList())
    val entriesField = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(keyField, valueField))
    val mapField = new Field(
      "m",
      new FieldType(true, new ArrowType.Map(false), null, null),
      java.util.Arrays.asList(entriesField))

    val mapType = LanceArrowUtils.fromArrowField(mapField).asInstanceOf[MapType]
    assert(mapType.keyType === StringType)
    assert(mapType.valueType === expectedType)
  }

  test("toArrowType for TimeType produces Time(NANOSECOND, 64)") {
    import org.apache.arrow.vector.types.TimeUnit

    if (!TimeUtils.isTimeTypeAvailable) {
      cancel("TimeType not available on this Spark version")
    }
    val timeType = TimeUtils.resolveSparkTimeType()
    val schema = new StructType().add("t", timeType)
    val arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", true)
    val arrowField = arrowSchema.findField("t")
    val arrowTimeType = arrowField.getType.asInstanceOf[ArrowType.Time]
    assert(arrowTimeType.getUnit === TimeUnit.NANOSECOND)
    assert(arrowTimeType.getBitWidth === 64)
  }

  test("FixedSizeBinary metadata produces FixedSizeBinary arrow type") {
    val fixedSizeBinaryMetadata = new MetadataBuilder()
      .putLong(
        FixedSizeBinaryUtils.ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY,
        16)
      .build()

    val schema = new StructType()
      .add("regular_binary", BinaryType, nullable = true)
      .add("fixed_binary", BinaryType, nullable = true, fixedSizeBinaryMetadata)

    val arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", false)

    // Regular binary should use Binary
    val regularField = arrowSchema.findField("regular_binary")
    assert(regularField.getType === ArrowType.Binary.INSTANCE)

    // Fixed binary with metadata should use FixedSizeBinary(16)
    val fixedField = arrowSchema.findField("fixed_binary")
    assert(fixedField.getType.isInstanceOf[ArrowType.FixedSizeBinary])
    assert(fixedField.getType.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth === 16)
  }

  test("FixedSizeBinary roundtrip preserves byte width") {
    // Simulate reading: create an Arrow FixedSizeBinary field -> convert to Spark schema
    val arrowField = new Field(
      "hash",
      new FieldType(true, new ArrowType.FixedSizeBinary(32), null, null),
      java.util.Collections.emptyList())
    val arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(
      java.util.Arrays.asList(arrowField))

    val sparkSchema = LanceArrowUtils.fromArrowSchema(arrowSchema)
    val hashField = sparkSchema("hash")

    // Data type is BinaryType (Spark has no FixedSizeBinary)
    assert(hashField.dataType === BinaryType)

    // But metadata preserves the byte width
    assert(hashField.metadata.contains(FixedSizeBinaryUtils.ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY))
    assert(hashField.metadata.getLong(
      FixedSizeBinaryUtils.ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY) === 32)

    // Simulate writing: convert Spark schema back to Arrow
    val roundtripArrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false)
    val roundtripField = roundtripArrowSchema.findField("hash")

    assert(roundtripField.getType.isInstanceOf[ArrowType.FixedSizeBinary])
    assert(roundtripField.getType.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth === 32)
  }
}
