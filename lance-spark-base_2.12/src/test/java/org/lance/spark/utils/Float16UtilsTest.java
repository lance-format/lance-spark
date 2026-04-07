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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for Float16Utils IEEE 754 half-precision conversion. */
public class Float16UtilsTest {

  @Test
  public void testExactRoundTrips() {
    // Values exactly representable in float16
    assertRoundTrip(0.0f);
    assertRoundTrip(1.0f);
    assertRoundTrip(-1.0f);
    assertRoundTrip(0.5f);
    assertRoundTrip(-0.5f);
    assertRoundTrip(2.0f);
    assertRoundTrip(0.25f);
    assertRoundTrip(0.125f);
    // Float16 max normal value: 65504
    assertRoundTrip(65504.0f);
  }

  @Test
  public void testNegativeZero() {
    short halfBits = Float16Utils.floatToHalf(-0.0f);
    float result = Float16Utils.halfToFloat(halfBits);
    assertEquals(
        Float.floatToRawIntBits(-0.0f),
        Float.floatToRawIntBits(result),
        "Negative zero should be preserved");
  }

  @Test
  public void testPositiveZero() {
    short halfBits = Float16Utils.floatToHalf(0.0f);
    float result = Float16Utils.halfToFloat(halfBits);
    assertEquals(
        Float.floatToRawIntBits(0.0f),
        Float.floatToRawIntBits(result),
        "Positive zero should be preserved");
  }

  @Test
  public void testInfinity() {
    assertRoundTrip(Float.POSITIVE_INFINITY);
    assertRoundTrip(Float.NEGATIVE_INFINITY);
  }

  @Test
  public void testNaN() {
    short halfBits = Float16Utils.floatToHalf(Float.NaN);
    float result = Float16Utils.halfToFloat(halfBits);
    assertTrue(Float.isNaN(result), "NaN should be preserved");
  }

  @Test
  public void testOverflowToInfinity() {
    // Values larger than float16 max (65504) should clamp to infinity
    short halfBits = Float16Utils.floatToHalf(100000.0f);
    float result = Float16Utils.halfToFloat(halfBits);
    assertEquals(Float.POSITIVE_INFINITY, result, "Overflow should produce +Inf");

    halfBits = Float16Utils.floatToHalf(-100000.0f);
    result = Float16Utils.halfToFloat(halfBits);
    assertEquals(Float.NEGATIVE_INFINITY, result, "Negative overflow should produce -Inf");

    // Float.MAX_VALUE
    halfBits = Float16Utils.floatToHalf(Float.MAX_VALUE);
    result = Float16Utils.halfToFloat(halfBits);
    assertEquals(Float.POSITIVE_INFINITY, result, "Float.MAX_VALUE overflows");
  }

  @Test
  public void testUnderflowToZero() {
    // Values too small for float16 subnormal should flush to zero
    // Smallest float16 subnormal: 2^-24 ~= 5.96e-8
    short halfBits = Float16Utils.floatToHalf(1.0e-10f);
    float result = Float16Utils.halfToFloat(halfBits);
    assertEquals(0.0f, result, "Very small values should flush to zero");
  }

  @Test
  public void testSubnormals() {
    // Smallest positive float16 subnormal: 2^-24 ~= 5.96e-8
    float smallestSubnormal = Float16Utils.halfToFloat((short) 0x0001);
    assertTrue(smallestSubnormal > 0, "Smallest subnormal should be positive");
    assertTrue(smallestSubnormal < 1.0e-6f, "Smallest subnormal should be very small");

    // Round-trip the smallest subnormal
    short rt = Float16Utils.floatToHalf(smallestSubnormal);
    assertEquals(0x0001, rt & 0xFFFF, "Smallest subnormal should round-trip");

    // Largest subnormal: 0x03FF
    float largestSubnormal = Float16Utils.halfToFloat((short) 0x03FF);
    assertTrue(largestSubnormal > 0);
    short rt2 = Float16Utils.floatToHalf(largestSubnormal);
    assertEquals(0x03FF, rt2 & 0xFFFF, "Largest subnormal should round-trip");
  }

  @Test
  public void testPrecisionLoss() {
    // Pi: 3.14159... should be close but not exact in float16
    float pi = 3.14159f;
    short halfBits = Float16Utils.floatToHalf(pi);
    float result = Float16Utils.halfToFloat(halfBits);
    // Float16 has ~3 decimal digits of precision
    assertEquals(pi, result, 0.005f, "Pi should be close in float16");
    // But not exactly equal
    assertNotEquals(pi, result, "Pi should lose some precision in float16");
  }

  @Test
  public void testSmallIntegers() {
    // Small integers (0-2048) are exactly representable in float16
    for (int i = 0; i <= 2048; i++) {
      assertRoundTrip((float) i);
    }
  }

  @Test
  public void testHalfToFloatKnownBits() {
    // 0x3C00 = 1.0 in float16
    assertEquals(1.0f, Float16Utils.halfToFloat((short) 0x3C00));
    // 0xC000 = -2.0 in float16
    assertEquals(-2.0f, Float16Utils.halfToFloat((short) 0xC000));
    // 0x7BFF = 65504.0 (max normal value)
    assertEquals(65504.0f, Float16Utils.halfToFloat((short) 0x7BFF));
    // 0x0400 = smallest positive normal: 2^-14
    float smallestNormal = Float16Utils.halfToFloat((short) 0x0400);
    assertEquals(Math.pow(2, -14), smallestNormal, 1e-10f, "Smallest normal");
  }

  @Test
  public void testIsFloat2VectorAvailable() {
    // Should return a consistent boolean (true on Arrow 18+, false otherwise)
    boolean first = Float16Utils.isFloat2VectorAvailable();
    boolean second = Float16Utils.isFloat2VectorAvailable();
    assertEquals(first, second, "isFloat2VectorAvailable should be stable across calls");
  }

  @Test
  public void testCreatePropertyKey() {
    assertEquals("embeddings.arrow.float16", Float16Utils.createPropertyKey("embeddings"));
  }

  @Test
  public void testFloatToHalfNaNPayload() {
    // NaN with a specific payload should still produce NaN after round-trip
    int nanBits = Float.floatToRawIntBits(Float.NaN) | 0x0001;
    float nanWithPayload = Float.intBitsToFloat(nanBits);
    assertTrue(Float.isNaN(nanWithPayload));

    short halfBits = Float16Utils.floatToHalf(nanWithPayload);
    float result = Float16Utils.halfToFloat(halfBits);
    assertTrue(Float.isNaN(result), "NaN with payload should remain NaN after round-trip");
  }

  @Test
  public void testSignalingNaNDoesNotBecomeInfinity() {
    // Signaling NaN with payload only in lowest mantissa bit (0x7F800001).
    // Before the fix, mantissa >>> 13 == 0, producing Infinity instead of NaN.
    float signalingNaN = Float.intBitsToFloat(0x7F800001);
    assertTrue(Float.isNaN(signalingNaN), "Input should be NaN");

    short halfBits = Float16Utils.floatToHalf(signalingNaN);
    float result = Float16Utils.halfToFloat(halfBits);

    assertTrue(Float.isNaN(result), "Signaling NaN must remain NaN, not become Infinity");
    assertFalse(Float.isInfinite(result), "Signaling NaN must not become Infinity");

    // Verify the raw half bits have non-zero mantissa (NaN, not Inf)
    int h = halfBits & 0xFFFF;
    int halfExponent = (h >>> 10) & 0x1F;
    int halfMantissaBits = h & 0x3FF;
    assertEquals(0x1F, halfExponent, "Exponent should be all 1s");
    assertTrue(halfMantissaBits != 0, "Half mantissa must be non-zero for NaN");
  }

  @Test
  public void testNegativeSignalingNaN() {
    // Negative signaling NaN: 0xFF800001
    float negSignalingNaN = Float.intBitsToFloat(0xFF800001);
    assertTrue(Float.isNaN(negSignalingNaN));

    short halfBits = Float16Utils.floatToHalf(negSignalingNaN);
    float result = Float16Utils.halfToFloat(halfBits);

    assertTrue(Float.isNaN(result), "Negative signaling NaN must remain NaN");
    assertFalse(Float.isInfinite(result), "Negative signaling NaN must not become Infinity");
  }

  @Test
  public void testAllLowBitNaNPayloadsPreserved() {
    // Test several NaN payloads that fit entirely in the lower 13 bits
    for (int payload = 1; payload <= 0x1FFF; payload <<= 1) {
      int nanBits = 0x7F800000 | payload;
      float nan = Float.intBitsToFloat(nanBits);
      assertTrue(Float.isNaN(nan));

      short halfBits = Float16Utils.floatToHalf(nan);
      float result = Float16Utils.halfToFloat(halfBits);
      assertTrue(
          Float.isNaN(result),
          "NaN with low-bit payload 0x" + Integer.toHexString(payload) + " must stay NaN");
    }
  }

  @Test
  public void testHasFloat16Metadata() {
    Metadata withFlag = new MetadataBuilder().putString("arrow.float16", "true").build();
    assertTrue(Float16Utils.hasFloat16Metadata(withFlag));

    Metadata withFalse = new MetadataBuilder().putString("arrow.float16", "false").build();
    assertFalse(Float16Utils.hasFloat16Metadata(withFalse));

    assertFalse(Float16Utils.hasFloat16Metadata(null));
    assertFalse(Float16Utils.hasFloat16Metadata(Metadata.empty()));
  }

  @Test
  public void testIsFloat16SparkField() {
    Metadata float16Meta = new MetadataBuilder().putString("arrow.float16", "true").build();

    // Valid float16 field: ArrayType(FloatType) + metadata
    StructField valid =
        new StructField(
            "vec", DataTypes.createArrayType(DataTypes.FloatType, false), false, float16Meta);
    assertTrue(Float16Utils.isFloat16SparkField(valid));

    // Wrong element type (Double)
    StructField doubleField =
        new StructField(
            "vec", DataTypes.createArrayType(DataTypes.DoubleType, false), false, float16Meta);
    assertFalse(Float16Utils.isFloat16SparkField(doubleField));

    // Not array type
    StructField intField = new StructField("vec", DataTypes.IntegerType, false, float16Meta);
    assertFalse(Float16Utils.isFloat16SparkField(intField));

    // No metadata
    StructField noMeta =
        new StructField(
            "vec", DataTypes.createArrayType(DataTypes.FloatType, false), false, Metadata.empty());
    assertFalse(Float16Utils.isFloat16SparkField(noMeta));

    assertFalse(Float16Utils.isFloat16SparkField(null));
  }

  @Test
  public void testIsFloat16ArrowField() {
    // Valid: FixedSizeList with FloatingPoint(HALF) child
    Field validChild =
        new Field(
            "element",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
            Collections.emptyList());
    Field validField =
        new Field(
            "vec",
            FieldType.nullable(new ArrowType.FixedSizeList(4)),
            Collections.singletonList(validChild));
    assertTrue(Float16Utils.isFloat16ArrowField(validField));

    // FixedSizeList with Float32 child — not float16
    Field float32Child =
        new Field(
            "element",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            Collections.emptyList());
    Field float32Field =
        new Field(
            "vec",
            FieldType.nullable(new ArrowType.FixedSizeList(4)),
            Collections.singletonList(float32Child));
    assertFalse(Float16Utils.isFloat16ArrowField(float32Field));

    // Not a FixedSizeList
    Field listField =
        new Field(
            "vec",
            FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(validChild));
    assertFalse(Float16Utils.isFloat16ArrowField(listField));

    assertFalse(Float16Utils.isFloat16ArrowField(null));
  }

  @Test
  public void testBoundaryValues() {
    // Just below float16 max normal (65504) — should round-trip
    assertRoundTrip(65504.0f);

    // Just above: should overflow to infinity
    short halfBits = Float16Utils.floatToHalf(65536.0f);
    float result = Float16Utils.halfToFloat(halfBits);
    assertEquals(Float.POSITIVE_INFINITY, result, "65536 should overflow to +Inf");

    // Negative boundary
    halfBits = Float16Utils.floatToHalf(-65536.0f);
    result = Float16Utils.halfToFloat(halfBits);
    assertEquals(Float.NEGATIVE_INFINITY, result, "-65536 should overflow to -Inf");
  }

  private void assertRoundTrip(float value) {
    short halfBits = Float16Utils.floatToHalf(value);
    float result = Float16Utils.halfToFloat(halfBits);
    if (Float.isNaN(value)) {
      assertTrue(Float.isNaN(result), "NaN should round-trip: " + value);
    } else {
      assertEquals(value, result, "Value should round-trip exactly: " + value);
    }
  }
}
