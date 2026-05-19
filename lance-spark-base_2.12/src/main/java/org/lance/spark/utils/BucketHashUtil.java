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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Computes bucket IDs using Iceberg-compatible Murmur3 hashing (seed 0, int-to-long widening). This
 * ensures Lance bucketed tables can participate in cross-catalog SPJ with Iceberg.
 *
 * <p>This is the single source of truth for bucket hashing — both the write path ({@link
 * #computeBucketId}) and the read path ({@link #computeBucketIdFromValue}) must use this class.
 */
public final class BucketHashUtil {

  static final int MURMUR3_SEED = 0;

  private BucketHashUtil() {}

  /**
   * Computes the bucket ID for a row. Used on the write path.
   *
   * @param row the internal row
   * @param columnIndices indices of bucket columns in the row
   * @param columnTypes data types of bucket columns
   * @param numBuckets number of buckets (must be &gt; 0)
   * @return bucket ID in [0, numBuckets)
   */
  public static int computeBucketId(
      InternalRow row, int[] columnIndices, DataType[] columnTypes, int numBuckets) {
    int hash = MURMUR3_SEED;
    for (int i = 0; i < columnIndices.length; i++) {
      int idx = columnIndices[i];
      if (row.isNullAt(idx)) {
        hash = Murmur3_x86_32.hashLong(0L, hash);
      } else {
        hash = hashRowValue(row, idx, columnTypes[i], hash);
      }
    }
    return (hash & Integer.MAX_VALUE) % numBuckets;
  }

  /**
   * Computes the bucket ID from a boxed value (e.g. from zonemap stats). Used on the read path to
   * match fragments to buckets.
   *
   * @param value the partition value (from zonemap min/max)
   * @param numBuckets number of buckets (must be &gt; 0)
   * @return bucket ID in [0, numBuckets)
   */
  public static int computeBucketIdFromValue(Object value, int numBuckets) {
    int hash = MURMUR3_SEED;
    if (value == null) {
      hash = Murmur3_x86_32.hashLong(0L, hash);
    } else {
      hash = hashBoxedValue(value, hash);
    }
    return (hash & Integer.MAX_VALUE) % numBuckets;
  }

  private static int hashRowValue(InternalRow row, int idx, DataType type, int seed) {
    if (type == DataTypes.IntegerType || type == DataTypes.DateType) {
      return Murmur3_x86_32.hashLong(row.getInt(idx), seed);
    } else if (type == DataTypes.LongType || type == DataTypes.TimestampType) {
      return Murmur3_x86_32.hashLong(row.getLong(idx), seed);
    } else if (type == DataTypes.StringType) {
      UTF8String str = row.getUTF8String(idx);
      return Murmur3_x86_32.hashUnsafeBytes(
          str.getBaseObject(), str.getBaseOffset(), str.numBytes(), seed);
    } else if (type == DataTypes.ShortType) {
      return Murmur3_x86_32.hashLong(row.getShort(idx), seed);
    } else if (type == DataTypes.ByteType) {
      return Murmur3_x86_32.hashLong(row.getByte(idx), seed);
    } else if (type == DataTypes.FloatType) {
      return Murmur3_x86_32.hashLong(Float.floatToIntBits(row.getFloat(idx)), seed);
    } else if (type == DataTypes.DoubleType) {
      return Murmur3_x86_32.hashLong(Double.doubleToLongBits(row.getDouble(idx)), seed);
    } else if (type == DataTypes.BooleanType) {
      return Murmur3_x86_32.hashLong(row.getBoolean(idx) ? 1L : 0L, seed);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported bucket column type: "
              + type
              + ". Supported: int, long, string, short,"
              + " byte, float, double, boolean, date,"
              + " timestamp.");
    }
  }

  private static int hashBoxedValue(Object value, int seed) {
    if (value instanceof Integer) {
      return Murmur3_x86_32.hashLong((Integer) value, seed);
    } else if (value instanceof Long) {
      return Murmur3_x86_32.hashLong((Long) value, seed);
    } else if (value instanceof String) {
      UTF8String str = UTF8String.fromString((String) value);
      return Murmur3_x86_32.hashUnsafeBytes(
          str.getBaseObject(), str.getBaseOffset(), str.numBytes(), seed);
    } else if (value instanceof Short) {
      return Murmur3_x86_32.hashLong((Short) value, seed);
    } else if (value instanceof Byte) {
      return Murmur3_x86_32.hashLong((Byte) value, seed);
    } else if (value instanceof Float) {
      return Murmur3_x86_32.hashLong(Float.floatToIntBits((Float) value), seed);
    } else if (value instanceof Double) {
      return Murmur3_x86_32.hashLong(Double.doubleToLongBits((Double) value), seed);
    } else if (value instanceof Boolean) {
      return Murmur3_x86_32.hashLong(((Boolean) value) ? 1L : 0L, seed);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported bucket value type: "
              + value.getClass().getName()
              + ". Supported: Integer, Long, String,"
              + " Short, Byte, Float, Double, Boolean.");
    }
  }
}
