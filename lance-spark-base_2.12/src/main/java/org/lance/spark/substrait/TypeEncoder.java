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

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import io.substrait.proto.Type.Nullability;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

/**
 * Translates Spark {@link DataType} into Substrait {@link Type} protobuf.
 *
 * <p>Two entry points:
 *
 * <ol>
 *   <li>{@link #encode} handles the 13 scalar types (bool, i8/i16/i32/i64, fp32/64, string, binary,
 *       date, timestamp, timestamp-with-tz, decimal) and returns {@code null} for everything else.
 *   <li>{@link #encodeDatasetSchema} wraps {@code encode} with placeholder emission so the result
 *       has exactly one top-level field per input column — Lance's {@code remove_extension_types}
 *       consumer drops the placeholders and remaps field references around them.
 * </ol>
 *
 * <p>Notable type mappings: {@code TimestampType} (session-TZ) → {@code TimestampTz} (int64 UTC
 * micros); {@code TimestampNTZType} → {@code Timestamp} (int64 local micros); {@code DateType} →
 * {@code Date} (int32 days since epoch); {@code DecimalType(p,s)} → {@code Decimal(p,s)}, with
 * negative scale rejected because DataFusion's consumer doesn't accept it.
 */
public final class TypeEncoder {

  /** Prefix Lance's {@code remove_extension_types} recognizes as a "drop me" marker. */
  static final String PLACEHOLDER_NAME_PREFIX = "__unlikely_name_placeholder";

  private TypeEncoder() {}

  /**
   * Encodes a single Spark {@link DataType} into a Substrait {@link Type}, or returns {@code null}
   * if the type cannot be encoded.
   *
   * @param sparkType the Spark data type
   * @param nullable whether the surrounding column / expression is nullable
   * @return the Substrait protobuf {@code Type}, or {@code null} if unsupported
   * @throws IllegalArgumentException if the type is malformed (e.g. negative-scale Decimal)
   */
  public static Type encode(DataType sparkType, boolean nullable) {
    Nullability n = nullable ? Nullability.NULLABILITY_NULLABLE : Nullability.NULLABILITY_REQUIRED;
    Type.Builder b = Type.newBuilder();
    if (sparkType instanceof BooleanType) {
      return b.setBool(Type.Boolean.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof ByteType) {
      return b.setI8(Type.I8.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof ShortType) {
      return b.setI16(Type.I16.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof IntegerType) {
      return b.setI32(Type.I32.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof LongType) {
      return b.setI64(Type.I64.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof FloatType) {
      return b.setFp32(Type.FP32.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof DoubleType) {
      return b.setFp64(Type.FP64.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof StringType) {
      return b.setString(Type.String.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof BinaryType) {
      return b.setBinary(Type.Binary.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof DateType) {
      return b.setDate(Type.Date.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof TimestampNTZType) {
      return b.setTimestamp(Type.Timestamp.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof TimestampType) {
      return b.setTimestampTz(Type.TimestampTZ.newBuilder().setNullability(n)).build();
    }
    if (sparkType instanceof DecimalType) {
      DecimalType dt = (DecimalType) sparkType;
      if (dt.scale() < 0) {
        throw new IllegalArgumentException(
            "Substrait Decimal does not accept negative scale: " + dt);
      }
      return b.setDecimal(
              Type.Decimal.newBuilder()
                  .setPrecision(dt.precision())
                  .setScale(dt.scale())
                  .setNullability(n))
          .build();
    }
    return null;
  }

  /**
   * Returns whether {@link #encode} produces a real Substrait type for {@code sparkType}, as
   * opposed to a placeholder.
   */
  public static boolean isEncodable(DataType sparkType) {
    return encode(sparkType, false) != null;
  }

  /**
   * Builds the dataset-level {@link NamedStruct} for an envelope's {@code base_schema} field.
   *
   * <p>The output has exactly one top-level entry per field in {@code datasetSchema}, in dataset
   * order. Columns whose Spark type cannot be encoded into a Substrait type are emitted with the
   * placeholder name {@code __unlikely_name_placeholder_<i>} and an {@code i32} stand-in type, so
   * Lance's consumer drops them via {@code remove_extension_types}.
   *
   * @param datasetSchema the full Lance dataset schema (NOT the post-pruning Spark scan schema)
   */
  public static NamedStruct encodeDatasetSchema(StructType datasetSchema) {
    NamedStruct.Builder ns = NamedStruct.newBuilder();
    Type.Struct.Builder structBuilder =
        Type.Struct.newBuilder().setNullability(Nullability.NULLABILITY_REQUIRED);
    StructField[] fields = datasetSchema.fields();
    for (int i = 0; i < fields.length; i++) {
      StructField f = fields[i];
      Type encoded = encode(f.dataType(), f.nullable());
      if (encoded != null) {
        ns.addNames(f.name());
        structBuilder.addTypes(encoded);
      } else {
        // Placeholder: Lance's remove_extension_types drops fields whose name starts with this
        // prefix. The type body doesn't matter; use i32 as a cheap stand-in.
        ns.addNames(PLACEHOLDER_NAME_PREFIX + "_" + i);
        structBuilder.addTypes(
            Type.newBuilder()
                .setI32(Type.I32.newBuilder().setNullability(Nullability.NULLABILITY_NULLABLE))
                .build());
      }
    }
    ns.setStruct(structBuilder);
    return ns.build();
  }
}
