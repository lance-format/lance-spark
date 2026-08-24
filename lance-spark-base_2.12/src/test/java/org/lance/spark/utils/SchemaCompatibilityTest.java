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
package org.lance.spark.utils;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link SchemaCompatibility}. Pure Arrow types, no Spark/Lance runtime needed. */
class SchemaCompatibilityTest {

  @Test
  void promotesNestedVariableWidthTypesWithoutChangingContainerTypes() {
    Field struct =
        new Field(
            "payload",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                field("text", ArrowType.Utf8.INSTANCE),
                listField("items", new ArrowType.List(), ArrowType.Binary.INSTANCE),
                field("fixed", new ArrowType.FixedSizeBinary(16))));

    Schema original = schema(struct);
    assertTrue(SchemaCompatibility.hasSmallVarTypes(original));

    Schema promoted = SchemaCompatibility.withLargeVarTypes(original);
    assertFalse(SchemaCompatibility.hasSmallVarTypes(promoted));
    Field payload = promoted.findField("payload");
    assertEquals(ArrowType.Struct.INSTANCE, payload.getType());
    assertEquals(ArrowType.LargeUtf8.INSTANCE, payload.getChildren().get(0).getType());
    assertEquals(ArrowType.List.INSTANCE, payload.getChildren().get(1).getType());
    assertEquals(
        ArrowType.LargeBinary.INSTANCE,
        payload.getChildren().get(1).getChildren().get(0).getType());
    assertEquals(new ArrowType.FixedSizeBinary(16), payload.getChildren().get(2).getType());
  }

  // ==================== Exact match ====================

  @Test
  void exactSameSchema() {
    Schema s = schema(field("id", new ArrowType.Int(32, true)));
    assertTrue(SchemaCompatibility.isCompatible(s, s));
  }

  // ==================== Integer family ====================

  @Test
  void unsignedToSignedWidening() {
    // uint8 -> int16
    Schema orig = schema(field("x", new ArrowType.Int(8, false)));
    Schema spark = schema(field("x", new ArrowType.Int(16, true)));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void unsignedToSignedWidening32() {
    // uint16 -> int32
    Schema orig = schema(field("x", new ArrowType.Int(16, false)));
    Schema spark = schema(field("x", new ArrowType.Int(32, true)));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void unsignedToSignedWidening64() {
    // uint32 -> int64
    Schema orig = schema(field("x", new ArrowType.Int(32, false)));
    Schema spark = schema(field("x", new ArrowType.Int(64, true)));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void uint32AcceptsSignedInt32WithRuntimeRangeCheck() {
    Schema orig = schema(field("x", new ArrowType.Int(32, false)));
    Schema spark = schema(field("x", new ArrowType.Int(32, true)));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void rejectsUnsupportedSameWidthUnsignedMapping() {
    // LanceArrowWriter has no ByteType -> UInt1Vector binding.
    Schema orig = schema(field("x", new ArrowType.Int(8, false)));
    Schema spark = schema(field("x", new ArrowType.Int(8, true)));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void incompatibleIntWidening() {
    // int8 signed -> int64 signed is NOT a valid unsigned widening path
    Schema orig = schema(field("x", new ArrowType.Int(8, true)));
    Schema spark = schema(field("x", new ArrowType.Int(64, true)));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  // ==================== Timestamp family ====================

  @Test
  void timestampDifferentTimezone() {
    // Non-UTC vs UTC — same unit, compatible (timezone is metadata only)
    Schema orig =
        schema(field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "America/New_York")));
    Schema spark = schema(field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")));
    assertTrue(
        SchemaCompatibility.isCompatible(orig, spark),
        "Same unit timestamp with different timezone should be compatible");
  }

  @Test
  void timestampNullTimezoneVsUtc() {
    Schema orig = schema(field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    Schema spark = schema(field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")));
    assertFalse(
        SchemaCompatibility.isCompatible(orig, spark),
        "TimestampNTZ and zoned Timestamp require different writers");
  }

  @Test
  void timestampDifferentUnit() {
    Schema orig =
        schema(field("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "Europe/Paris")));
    Schema spark = schema(field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")));
    assertFalse(
        SchemaCompatibility.isCompatible(orig, spark),
        "Timestamp unit conversion is not supported by LanceArrowWriter");
  }

  // ==================== List family ====================

  @Test
  void listToFixedSizeList() {
    Schema orig = schema(listField("arr", new ArrowType.List(), new ArrowType.Int(32, true)));
    Schema spark =
        schema(listField("arr", new ArrowType.FixedSizeList(10), new ArrowType.Int(32, true)));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void largeListToList() {
    Schema orig = schema(listField("arr", new ArrowType.LargeList(), new ArrowType.Int(32, true)));
    Schema spark = schema(listField("arr", new ArrowType.List(), new ArrowType.Int(32, true)));
    assertTrue(
        SchemaCompatibility.isCompatible(orig, spark), "LargeList <-> List should be compatible");
  }

  @Test
  void fixedSizeListSizeMismatch() {
    ArrowType.FloatingPoint float32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    Schema orig = schema(listField("arr", new ArrowType.FixedSizeList(4), float32));
    Schema spark = schema(listField("arr", new ArrowType.FixedSizeList(8), float32));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void fixedSizeListWithFloatingPointElement() {
    ArrowType.FloatingPoint float32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    Schema orig = schema(listField("arr", new ArrowType.FixedSizeList(4), float32));
    Schema spark = schema(listField("arr", ArrowType.List.INSTANCE, float32));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void fixedSizeListWithLargeUtf8Element() {
    Schema orig =
        schema(listField("arr", new ArrowType.FixedSizeList(3), ArrowType.LargeUtf8.INSTANCE));
    Schema spark = schema(listField("arr", ArrowType.List.INSTANCE, ArrowType.LargeUtf8.INSTANCE));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  // ==================== String family ====================

  @Test
  void utf8ToLargeUtf8() {
    Schema orig = schema(field("s", ArrowType.Utf8.INSTANCE));
    Schema spark = schema(field("s", ArrowType.LargeUtf8.INSTANCE));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void largeUtf8ToUtf8() {
    Schema orig = schema(field("s", ArrowType.LargeUtf8.INSTANCE));
    Schema spark = schema(field("s", ArrowType.Utf8.INSTANCE));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  // ==================== Binary family ====================

  @Test
  void binaryToLargeBinary() {
    Schema orig = schema(field("b", ArrowType.Binary.INSTANCE));
    Schema spark = schema(field("b", ArrowType.LargeBinary.INSTANCE));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void fixedSizeBinaryToBinary() {
    Schema orig = schema(field("b", new ArrowType.FixedSizeBinary(16)));
    Schema spark = schema(field("b", ArrowType.Binary.INSTANCE));
    assertTrue(SchemaCompatibility.isCompatible(orig, spark));
  }

  // ==================== FloatingPoint family ====================

  @Test
  void float16ToFloat32() {
    // Float16 child in FixedSizeList -> Float32 child (Spark widens)
    Schema orig =
        schema(
            listField(
                "emb",
                new ArrowType.FixedSizeList(4),
                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)));
    Schema spark =
        schema(
            listField(
                "emb",
                new ArrowType.FixedSizeList(4),
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    assertTrue(
        SchemaCompatibility.isCompatible(orig, spark),
        "Float16 -> Float32 should be compatible within FloatingPoint family");
  }

  @Test
  void float32ToFloat64IsNotCompatible() {
    Schema orig = schema(field("x", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    Schema spark = schema(field("x", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    assertFalse(
        SchemaCompatibility.isCompatible(orig, spark),
        "Arbitrary floating-point conversion has no matching writer");
  }

  // ==================== Structural incompatibility (should reject) ====================

  @Test
  void fieldCountMismatch() {
    Schema orig =
        schema(field("id", new ArrowType.Int(32, true)), field("name", ArrowType.Utf8.INSTANCE));
    Schema spark = schema(field("id", new ArrowType.Int(32, true)));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void fieldNameMismatch() {
    Schema orig = schema(field("old_name", new ArrowType.Int(32, true)));
    Schema spark = schema(field("new_name", new ArrowType.Int(32, true)));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void crossFamilyIncompatible() {
    // Int vs Utf8 — totally different families
    Schema orig = schema(field("x", new ArrowType.Int(32, true)));
    Schema spark = schema(field("x", ArrowType.Utf8.INSTANCE));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  @Test
  void nestedChildIncompatible() {
    // List<Int> vs List<Utf8>
    Schema orig = schema(listField("arr", new ArrowType.List(), new ArrowType.Int(32, true)));
    Schema spark = schema(listField("arr", new ArrowType.List(), ArrowType.Utf8.INSTANCE));
    assertFalse(SchemaCompatibility.isCompatible(orig, spark));
  }

  // ==================== Helpers ====================

  private static Schema schema(Field... fields) {
    return new Schema(Arrays.asList(fields));
  }

  private static Field field(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  private static Field listField(String name, ArrowType listType, ArrowType childType) {
    Field child = new Field("item", FieldType.nullable(childType), null);
    return new Field(name, FieldType.nullable(listType), Collections.singletonList(child));
  }
}
