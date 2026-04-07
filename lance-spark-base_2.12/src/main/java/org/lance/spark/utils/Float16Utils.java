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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

/**
 * Utility methods for float16 (half-precision) vector support. Float16 vectors are stored as Arrow
 * FixedSizeList with FloatingPoint(HALF) elements. Since Spark has no native float16 type, they are
 * represented as ArrayType(FloatType) with metadata flag "arrow.float16" = "true".
 */
public class Float16Utils {

  public static final String ARROW_FLOAT16_KEY = "arrow.float16";
  public static final String ARROW_FLOAT16_VALUE = "true";

  private static volatile Boolean float2VectorAvailable = null;

  /**
   * Check if Spark metadata contains the float16 flag.
   *
   * @param metadata the Spark field metadata
   * @return true if metadata has "arrow.float16" = "true"
   */
  public static boolean hasFloat16Metadata(Metadata metadata) {
    if (metadata == null || !metadata.contains(ARROW_FLOAT16_KEY)) {
      return false;
    }
    return ARROW_FLOAT16_VALUE.equalsIgnoreCase(metadata.getString(ARROW_FLOAT16_KEY));
  }

  /**
   * Check if a Spark field is a float16 vector field based on its type and metadata.
   *
   * @param field the Spark struct field to check
   * @return true if the field is ArrayType(FloatType) with float16 metadata
   */
  public static boolean isFloat16SparkField(StructField field) {
    if (field == null || field.metadata() == null) {
      return false;
    }
    if (!(field.dataType() instanceof ArrayType)) {
      return false;
    }
    ArrayType arrayType = (ArrayType) field.dataType();
    if (!(arrayType.elementType() instanceof FloatType)) {
      return false;
    }
    return hasFloat16Metadata(field.metadata());
  }

  /**
   * Check if an Arrow FixedSizeList field has float16 (half-precision) elements.
   *
   * @param field the Arrow field to check
   * @return true if the field is a FixedSizeList with FloatingPoint(HALF) child
   */
  public static boolean isFloat16ArrowField(Field field) {
    if (field == null) {
      return false;
    }
    if (!(field.getType() instanceof ArrowType.FixedSizeList)) {
      return false;
    }
    if (field.getChildren().isEmpty()) {
      return false;
    }
    ArrowType childType = field.getChildren().get(0).getType();
    if (!(childType instanceof ArrowType.FloatingPoint)) {
      return false;
    }
    return ((ArrowType.FloatingPoint) childType).getPrecision() == FloatingPointPrecision.HALF;
  }

  /**
   * Create the property key for configuring float16 on a vector column.
   *
   * @param fieldName the name of the field
   * @return the property key (e.g., "embeddings.arrow.float16")
   */
  public static String createPropertyKey(String fieldName) {
    return fieldName + "." + ARROW_FLOAT16_KEY;
  }

  /**
   * Check if Arrow Float2Vector class is available at runtime. Float2Vector was introduced in Arrow
   * 18 (used by Spark 4.0+). Older Arrow versions (14, 15) do not have it.
   *
   * @return true if Float2Vector is available
   */
  public static boolean isFloat2VectorAvailable() {
    if (float2VectorAvailable == null) {
      synchronized (Float16Utils.class) {
        if (float2VectorAvailable == null) {
          try {
            Class.forName("org.apache.arrow.vector.Float2Vector");
            float2VectorAvailable = true;
          } catch (ClassNotFoundException e) {
            float2VectorAvailable = false;
          }
        }
      }
    }
    return float2VectorAvailable;
  }

  /**
   * Convert an IEEE 754 half-precision (16-bit) float to single-precision (32-bit) float.
   *
   * <p>Half-precision format: 1 sign bit, 5 exponent bits, 10 mantissa bits.
   *
   * @param halfBits the 16-bit half-precision float as a short
   * @return the equivalent single-precision float
   */
  public static float halfToFloat(short halfBits) {
    int h = halfBits & 0xFFFF;
    int sign = (h >>> 15) & 0x1;
    int exponent = (h >>> 10) & 0x1F;
    int mantissa = h & 0x3FF;

    if (exponent == 0) {
      if (mantissa == 0) {
        // Zero (positive or negative)
        return Float.intBitsToFloat(sign << 31);
      }
      // Subnormal: normalize by shifting mantissa until leading 1 is in bit 10
      exponent = 1;
      while ((mantissa & 0x400) == 0) {
        mantissa <<= 1;
        exponent--;
      }
      mantissa &= 0x3FF; // Remove the leading 1 bit
      // Convert to float32 exponent: half bias=15, float bias=127
      int floatExponent = exponent + (127 - 15);
      int floatBits = (sign << 31) | (floatExponent << 23) | (mantissa << 13);
      return Float.intBitsToFloat(floatBits);
    }

    if (exponent == 0x1F) {
      if (mantissa == 0) {
        // Infinity
        return Float.intBitsToFloat((sign << 31) | 0x7F800000);
      }
      // NaN - preserve payload bits
      return Float.intBitsToFloat((sign << 31) | 0x7F800000 | (mantissa << 13));
    }

    // Normal number: re-bias exponent from half (bias=15) to float (bias=127)
    int floatExponent = exponent + (127 - 15);
    int floatBits = (sign << 31) | (floatExponent << 23) | (mantissa << 13);
    return Float.intBitsToFloat(floatBits);
  }

  /**
   * Convert a single-precision (32-bit) float to IEEE 754 half-precision (16-bit) float.
   *
   * <p>Handles overflow (clamps to +/-Infinity), underflow (flushes to zero), NaN propagation, and
   * round-to-nearest-even.
   *
   * @param value the single-precision float
   * @return the 16-bit half-precision float as a short
   */
  public static short floatToHalf(float value) {
    int floatBits = Float.floatToRawIntBits(value);
    int sign = (floatBits >>> 31) & 0x1;
    int exponent = (floatBits >>> 23) & 0xFF;
    int mantissa = floatBits & 0x7FFFFF;

    int halfSign = sign << 15;

    if (exponent == 0xFF) {
      if (mantissa == 0) {
        // Infinity
        return (short) (halfSign | 0x7C00);
      }
      // NaN - preserve some payload bits.
      // When the payload is entirely in the lower 13 bits (e.g., signaling NaN 0x7F800001),
      // mantissa >>> 13 yields 0, which would produce Infinity instead of NaN.
      // Ensure at least one mantissa bit is set to keep it a NaN.
      int halfMantissa = mantissa >>> 13;
      if (halfMantissa == 0) {
        halfMantissa = 1;
      }
      return (short) (halfSign | 0x7C00 | halfMantissa);
    }

    // Re-bias exponent from float (bias=127) to half (bias=15)
    int unbiasedExponent = exponent - 127;

    if (unbiasedExponent > 15) {
      // Overflow: clamp to infinity
      return (short) (halfSign | 0x7C00);
    }

    if (unbiasedExponent < -24) {
      // Too small for even subnormal representation: flush to zero
      return (short) halfSign;
    }

    if (unbiasedExponent < -14) {
      // Subnormal in half-precision
      // Add the implicit leading 1 bit to mantissa
      mantissa |= 0x800000;
      // Shift amount: number of bits to shift right
      int shift = -14 - unbiasedExponent + 13;
      // Round-to-nearest-even
      int roundBit = 1 << (shift - 1);
      int stickyBits = mantissa & (roundBit - 1);
      int halfMantissa = mantissa >>> shift;
      if ((mantissa & roundBit) != 0 && (stickyBits != 0 || (halfMantissa & 1) != 0)) {
        halfMantissa++;
      }
      return (short) (halfSign | halfMantissa);
    }

    // Normal number in half-precision
    int halfExponent = (unbiasedExponent + 15) << 10;
    // Round-to-nearest-even on the 13 truncated mantissa bits
    int roundBit = 1 << 12;
    int stickyBits = mantissa & (roundBit - 1);
    int halfMantissa = mantissa >>> 13;
    if ((mantissa & roundBit) != 0 && (stickyBits != 0 || (halfMantissa & 1) != 0)) {
      halfMantissa++;
      if (halfMantissa > 0x3FF) {
        // Mantissa overflow: increment exponent
        halfMantissa = 0;
        halfExponent += (1 << 10);
        if (halfExponent >= 0x7C00) {
          // Exponent overflow: clamp to infinity
          return (short) (halfSign | 0x7C00);
        }
      }
    }
    return (short) (halfSign | halfExponent | halfMantissa);
  }
}
