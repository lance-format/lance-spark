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
package org.lance.spark.substrait;

import io.substrait.proto.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiteralEncoderTest {

  /** Tiny Literal<?> impl that lets us hand the encoder a (value, dataType) pair directly. */
  private static <T> Literal<T> lit(T value, DataType type) {
    return new Literal<T>() {
      @Override
      public T value() {
        return value;
      }

      @Override
      public DataType dataType() {
        return type;
      }
    };
  }

  // ---------- primitives ----------

  @Test
  void encodesBoolean() {
    Expression.Literal a = LiteralEncoder.encode(lit(Boolean.TRUE, DataTypes.BooleanType));
    assertEquals(Expression.Literal.LiteralTypeCase.BOOLEAN, a.getLiteralTypeCase());
    assertTrue(a.getBoolean());
    assertFalse(a.getNullable());
  }

  @Test
  void encodesIntegralWidths() {
    assertEquals((byte) 7, (byte) LiteralEncoder.encode(lit((byte) 7, DataTypes.ByteType)).getI8());
    assertEquals(
        (short) 1234,
        (short) LiteralEncoder.encode(lit((short) 1234, DataTypes.ShortType)).getI16());
    assertEquals(
        Integer.MAX_VALUE,
        LiteralEncoder.encode(lit(Integer.MAX_VALUE, DataTypes.IntegerType)).getI32());
    assertEquals(
        Long.MIN_VALUE, LiteralEncoder.encode(lit(Long.MIN_VALUE, DataTypes.LongType)).getI64());
  }

  @Test
  void encodesFloatingPoint() {
    Expression.Literal f = LiteralEncoder.encode(lit(3.14f, DataTypes.FloatType));
    assertEquals(3.14f, f.getFp32(), 0.0f);

    Expression.Literal d = LiteralEncoder.encode(lit(2.718281828d, DataTypes.DoubleType));
    assertEquals(2.718281828d, d.getFp64(), 0.0d);
  }

  /**
   * Regression for the legacy SQL-string filter path: {@code Double.toString → reparse} lost
   * precision on values like {@code 0.1 + 0.2}. Substrait writes IEEE-754 bits directly, so the
   * round-trip must be bit-exact.
   */
  @Test
  void literalRoundTripsExactBitsForDouble() {
    double original = 0.1 + 0.2; // 0.30000000000000004
    Expression.Literal encoded = LiteralEncoder.encode(lit(original, DataTypes.DoubleType));
    double roundTripped = encoded.getFp64();
    assertEquals(
        Double.doubleToRawLongBits(original),
        Double.doubleToRawLongBits(roundTripped),
        "double literal must round-trip exact bits");
  }

  // ---------- string / binary ----------

  @Test
  void encodesUtf8StringValueAndPlainStringValue() {
    Expression.Literal viaUtf8 =
        LiteralEncoder.encode(lit(UTF8String.fromString("héllo"), DataTypes.StringType));
    assertEquals("héllo", viaUtf8.getString());

    // Tolerate older Spark versions that occasionally pass a plain String.
    Expression.Literal viaString = LiteralEncoder.encode(lit("héllo", DataTypes.StringType));
    assertEquals("héllo", viaString.getString());
  }

  @Test
  void encodesBinary() {
    byte[] bytes = {1, 2, 3, 4};
    Expression.Literal lit = LiteralEncoder.encode(lit(bytes, DataTypes.BinaryType));
    assertArrayEquals(bytes, lit.getBinary().toByteArray());
  }

  // ---------- date / timestamp ----------

  @Test
  void encodesDateAsDaysSinceEpoch() {
    // 2021-01-01 = day 18628
    Expression.Literal l = LiteralEncoder.encode(lit(18628, DataTypes.DateType));
    assertEquals(18628, l.getDate());
  }

  @Test
  void encodesTimestampNtzAsMicros() {
    long micros = 1_700_000_000_000_000L;
    Expression.Literal l = LiteralEncoder.encode(lit(micros, DataTypes.TimestampNTZType));
    assertEquals(micros, l.getTimestamp());
  }

  @Test
  void encodesTimestampWithTimezoneAsMicros() {
    long micros = 1_700_000_000_000_000L;
    Expression.Literal l = LiteralEncoder.encode(lit(micros, DataTypes.TimestampType));
    assertEquals(micros, l.getTimestampTz());
  }

  // ---------- decimal ----------

  @Test
  void encodesDecimalCarriesPrecisionScaleAndValue() {
    DecimalType dt = DataTypes.createDecimalType(10, 2);
    Decimal d = Decimal.fromDecimal(new BigDecimal("123.45"));
    Expression.Literal l = LiteralEncoder.encode(lit(d, dt));
    assertEquals(10, l.getDecimal().getPrecision());
    assertEquals(2, l.getDecimal().getScale());
    // 123.45 * 10^2 = 12345 → little-endian: 39, 48, 0, 0, ..., 0
    byte[] expected = new byte[16];
    expected[0] = 0x39;
    expected[1] = 0x30;
    assertArrayEquals(expected, l.getDecimal().getValue().toByteArray());
  }

  @Test
  void encodesNegativeDecimalWithSignExtension() {
    DecimalType dt = DataTypes.createDecimalType(10, 2);
    Decimal d = Decimal.fromDecimal(new BigDecimal("-1.00"));
    Expression.Literal l = LiteralEncoder.encode(lit(d, dt));
    // -1.00 * 100 = -100 → big-endian two's complement = [0x9C], reverse to LE = [0x9C],
    // pad with 0xFF (sign extend) for the remaining 15 bytes.
    byte[] bytes = l.getDecimal().getValue().toByteArray();
    assertEquals(16, bytes.length);
    assertEquals((byte) 0x9C, bytes[0]);
    for (int i = 1; i < 16; i++) {
      assertEquals((byte) 0xFF, bytes[i], "byte " + i + " should be sign-extended");
    }
  }

  @Test
  void rejectsDecimalThatOverflows16Bytes() {
    // 39 nines is 130-bit, which doesn't fit in 16 bytes (128-bit signed = max precision 38).
    BigDecimal huge = new BigDecimal("999999999999999999999999999999999999999"); // 39 nines
    DecimalType dt = DataTypes.createDecimalType(38, 0);
    assertThrows(
        IllegalArgumentException.class,
        () -> LiteralEncoder.encode(lit(Decimal.fromDecimal(huge), dt)));
  }

  // ---------- nulls ----------

  @Test
  void encodesNullLiteralWithTypeProto() {
    Expression.Literal l = LiteralEncoder.encode(lit(null, DataTypes.IntegerType));
    assertEquals(Expression.Literal.LiteralTypeCase.NULL, l.getLiteralTypeCase());
    assertTrue(l.getNullable());
    assertEquals(io.substrait.proto.Type.KindCase.I32, l.getNull().getKindCase());
  }

  @Test
  void encodesNullStringLiteral() {
    Expression.Literal l = LiteralEncoder.encode(lit(null, DataTypes.StringType));
    assertEquals(Expression.Literal.LiteralTypeCase.NULL, l.getLiteralTypeCase());
    assertEquals(io.substrait.proto.Type.KindCase.STRING, l.getNull().getKindCase());
  }

  @Test
  void rejectsNullLiteralForUnsupportedType() {
    // ArrayType cannot be encoded → encoding a null array literal must fail loudly.
    org.apache.spark.sql.types.DataType arr =
        new org.apache.spark.sql.types.ArrayType(DataTypes.IntegerType, true);
    assertThrows(IllegalArgumentException.class, () -> LiteralEncoder.encode(lit(null, arr)));
  }

  // ---------- internal helper ----------

  @Test
  void encodeDecimalLittleEndianHasFixedWidth() {
    byte[] zero = LiteralEncoder.encodeDecimalLittleEndian(BigDecimal.ZERO, 0);
    assertEquals(16, zero.length);
    for (byte b : zero) {
      assertEquals(0, b);
    }
  }
}
