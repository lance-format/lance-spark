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
package org.lance.spark.write;

import org.lance.Dataset;
import org.lance.schema.LanceField;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * The target table's unenforced primary key columns, mapped onto the Spark write schema so an
 * inserted row's key can be hashed into a {@code KeyExistenceFilter}. Serializable so it travels to
 * the write tasks with the writer factory.
 *
 * <p>Key bytes follow lance-core's {@code KeyValue::to_bytes} encoding where the type overlaps
 * (strings as UTF-8, integers widened to 64-bit little-endian, binary raw; a multi-column key
 * appends a zero byte after every column), and the hash is xxHash64 with seed 0. Combined with
 * {@link SplitBlockBloomFilter} matching lance-core's bloom configuration, a filter built here
 * compares meaningfully against one built by a concurrent native {@code merge_insert}.
 */
final class PrimaryKeyColumns implements Serializable {
  private static final long serialVersionUID = 1L;

  private final int[] fieldIds;
  private final int[] ordinals;
  private final DataType[] types;

  /**
   * When non-null, a primary key is declared but this write cannot hash it (a key column is missing
   * from the write schema or has an unsupported type). Inserting rows must then fail rather than
   * silently skip conflict detection; deletes and updates are unaffected.
   */
  private final String unusableReason;

  private PrimaryKeyColumns(
      int[] fieldIds, int[] ordinals, DataType[] types, String unusableReason) {
    this.fieldIds = fieldIds;
    this.ordinals = ordinals;
    this.types = types;
    this.unusableReason = unusableReason;
  }

  /**
   * Resolves the table's unenforced primary key against the write schema. Returns null when no
   * primary key is declared; otherwise always returns an instance, which is unusable (fails on the
   * first hashed row) when a key column cannot be mapped or hashed.
   */
  static PrimaryKeyColumns resolve(Dataset dataset, StructType sparkSchema) {
    List<LanceField> keyFields = new ArrayList<>();
    for (LanceField field : dataset.getLanceSchema().fields()) {
      if (field.isUnenforcedPrimaryKey()) {
        keyFields.add(field);
      }
    }
    if (keyFields.isEmpty()) {
      return null;
    }
    keyFields.sort(
        Comparator.comparingInt(
            field -> field.getUnenforcedPrimaryKeyPosition().orElse(Integer.MAX_VALUE)));

    int n = keyFields.size();
    int[] fieldIds = new int[n];
    int[] ordinals = new int[n];
    DataType[] types = new DataType[n];
    for (int i = 0; i < n; i++) {
      LanceField field = keyFields.get(i);
      int ordinal;
      try {
        ordinal = sparkSchema.fieldIndex(field.getName());
      } catch (IllegalArgumentException e) {
        return unusable(
            "unenforced primary key column '" + field.getName() + "' is not in the write schema");
      }
      DataType type = sparkSchema.fields()[ordinal].dataType();
      if (!isSupported(type)) {
        return unusable(
            "unenforced primary key column '"
                + field.getName()
                + "' has type "
                + type.simpleString()
                + ", which the inserted-rows conflict filter cannot hash");
      }
      fieldIds[i] = field.getId();
      ordinals[i] = ordinal;
      types[i] = type;
    }
    return new PrimaryKeyColumns(fieldIds, ordinals, types, null);
  }

  int[] fieldIds() {
    return fieldIds;
  }

  /** Deterministic 64-bit hash of a row's primary key: xxHash64 (seed 0) of the key encoding. */
  long hashKey(InternalRow row) {
    if (unusableReason != null) {
      throw new UnsupportedOperationException(
          "Cannot insert rows: "
              + unusableReason
              + ". Concurrent inserts of the same key would not be detected.");
    }
    return XxHash64.hash(encodeKey(row));
  }

  private byte[] encodeKey(InternalRow row) {
    if (ordinals.length == 1) {
      return encodeColumn(row, 0);
    }
    // Multi-column keys mirror lance-core's composite encoding: every column's bytes followed by
    // a zero byte.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < ordinals.length; i++) {
      byte[] bytes = encodeColumn(row, i);
      out.write(bytes, 0, bytes.length);
      out.write(0);
    }
    return out.toByteArray();
  }

  private byte[] encodeColumn(InternalRow row, int i) {
    DataType type = types[i];
    int ordinal = ordinals[i];
    if (type instanceof StringType) {
      return row.getUTF8String(ordinal).getBytes();
    }
    if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return longToBytesLE(row.getLong(ordinal));
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return longToBytesLE(row.getInt(ordinal));
    }
    if (type instanceof ShortType) {
      return longToBytesLE(row.getShort(ordinal));
    }
    if (type instanceof ByteType) {
      return longToBytesLE(row.getByte(ordinal));
    }
    if (type instanceof BooleanType) {
      return longToBytesLE(row.getBoolean(ordinal) ? 1L : 0L);
    }
    if (type instanceof BinaryType) {
      return row.getBinary(ordinal);
    }
    if (type instanceof FloatType) {
      return doubleToBytesLE(row.getFloat(ordinal));
    }
    if (type instanceof DoubleType) {
      return doubleToBytesLE(row.getDouble(ordinal));
    }
    if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) type;
      // The column's scale is fixed, so the unscaled value is a canonical key encoding.
      return row.getDecimal(ordinal, decimalType.precision(), decimalType.scale())
          .toJavaBigDecimal()
          .unscaledValue()
          .toByteArray();
    }
    throw new IllegalStateException("Unsupported primary key type: " + type.simpleString());
  }

  private static boolean isSupported(DataType type) {
    return type instanceof StringType
        || type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType
        || type instanceof IntegerType
        || type instanceof DateType
        || type instanceof ShortType
        || type instanceof ByteType
        || type instanceof BooleanType
        || type instanceof BinaryType
        || type instanceof FloatType
        || type instanceof DoubleType
        || type instanceof DecimalType;
  }

  private static byte[] longToBytesLE(long v) {
    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) v;
      v >>>= 8;
    }
    return bytes;
  }

  private static byte[] doubleToBytesLE(double v) {
    // SQL equality treats -0.0 as 0.0 and all NaNs as one value; fold both so equal keys hash
    // equally. Adding 0.0 turns -0.0 into 0.0, and doubleToLongBits canonicalizes NaN.
    return longToBytesLE(Double.doubleToLongBits(v + 0.0d));
  }

  @Override
  public String toString() {
    if (unusableReason != null) {
      return "PrimaryKeyColumns{unusable: " + unusableReason + "}";
    }
    return "PrimaryKeyColumns{fieldIds=" + Arrays.toString(fieldIds) + "}";
  }

  private static PrimaryKeyColumns unusable(String reason) {
    return new PrimaryKeyColumns(null, null, null, reason);
  }
}
