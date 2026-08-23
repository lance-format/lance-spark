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

import java.util.ArrayList;
import java.util.List;

/**
 * Validates that a Spark-derived Arrow schema is structurally compatible with an existing Lance
 * dataset schema. "Compatible" means same field count, matching field names, and each field pair
 * belongs to the same type family (e.g. Int32 signed ↔ Int32 unsigned, List ↔ FixedSizeList, Utf8 ↔
 * LargeUtf8). This allows schema-preserving overwrite to use the original Lance schema for commit.
 */
public final class SchemaCompatibility {

  private SchemaCompatibility() {}

  /** Returns true if the schema contains Utf8 or Binary fields with 32-bit offsets. */
  public static boolean hasSmallVarTypes(Schema schema) {
    for (Field field : schema.getFields()) {
      if (hasSmallVarTypes(field)) {
        return true;
      }
    }
    return false;
  }

  /** Returns a copy with variable-width string and binary fields promoted to 64-bit offsets. */
  public static Schema withLargeVarTypes(Schema schema) {
    List<Field> fields = new ArrayList<>(schema.getFields().size());
    for (Field field : schema.getFields()) {
      fields.add(withLargeVarTypes(field));
    }
    return new Schema(fields, schema.getCustomMetadata());
  }

  /** Returns true if the original schema is type-compatible with the Spark-derived schema. */
  public static boolean isCompatible(Schema original, Schema spark) {
    if (original.getFields().size() != spark.getFields().size()) {
      return false;
    }
    for (int i = 0; i < original.getFields().size(); i++) {
      Field originalField = original.getFields().get(i);
      Field sparkField = spark.getFields().get(i);
      if (!originalField.getName().equals(sparkField.getName())
          || !isFieldCompatible(originalField, sparkField)) {
        return false;
      }
    }
    return true;
  }

  private static boolean hasSmallVarTypes(Field field) {
    if (field.getType() instanceof ArrowType.Utf8 || field.getType() instanceof ArrowType.Binary) {
      return true;
    }
    for (Field child : field.getChildren()) {
      if (hasSmallVarTypes(child)) {
        return true;
      }
    }
    return false;
  }

  private static Field withLargeVarTypes(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Utf8) {
      type = ArrowType.LargeUtf8.INSTANCE;
    } else if (type instanceof ArrowType.Binary) {
      type = ArrowType.LargeBinary.INSTANCE;
    }

    List<Field> children = new ArrayList<>(field.getChildren().size());
    for (Field child : field.getChildren()) {
      children.add(withLargeVarTypes(child));
    }

    FieldType fieldType =
        new FieldType(field.isNullable(), type, field.getDictionary(), field.getMetadata());
    return new Field(field.getName(), fieldType, children);
  }

  private static boolean isFieldCompatible(Field orig, Field spark) {
    ArrowType ot = orig.getType();
    ArrowType st = spark.getType();

    // Only allow integer pairs that LanceArrowWriter can bind safely. Unsigned values are widened
    // by LanceArrowUtils on read; uint32 also accepts IntegerType with runtime range validation.
    if (ot instanceof ArrowType.Int && st instanceof ArrowType.Int) {
      ArrowType.Int oi = (ArrowType.Int) ot;
      ArrowType.Int si = (ArrowType.Int) st;
      if (oi.equals(si)) {
        return true;
      }
      return !oi.getIsSigned()
          && si.getIsSigned()
          && ((oi.getBitWidth() == 8 && si.getBitWidth() == 16)
              || (oi.getBitWidth() == 16 && si.getBitWidth() == 32)
              || (oi.getBitWidth() == 32 && (si.getBitWidth() == 32 || si.getBitWidth() == 64))
              || (oi.getBitWidth() == 64 && si.getBitWidth() == 64));
    }

    // Spark represents all three Arrow list variants as ArrayType. The executor has a dedicated
    // writer for each original vector type. Two explicit FixedSizeLists must retain the same size.
    if (isListFamily(ot) && isListFamily(st)) {
      if (ot instanceof ArrowType.FixedSizeList && st instanceof ArrowType.FixedSizeList) {
        ArrowType.FixedSizeList of = (ArrowType.FixedSizeList) ot;
        ArrowType.FixedSizeList sf = (ArrowType.FixedSizeList) st;
        if (of.getListSize() != sf.getListSize()) {
          return false;
        }
      }
      return childrenCompatible(orig, spark, false);
    }

    // Spark rewrites every zoned timestamp to UTC. Preserve the original timezone, but never cross
    // the Timestamp/TimestampNTZ boundary. LanceArrowWriter currently supports microseconds only.
    if (ot instanceof ArrowType.Timestamp && st instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp oTs = (ArrowType.Timestamp) ot;
      ArrowType.Timestamp sTs = (ArrowType.Timestamp) st;
      return oTs.getUnit() == TimeUnit.MICROSECOND
          && sTs.getUnit() == TimeUnit.MICROSECOND
          && (hasTimezone(oTs) == hasTimezone(sTs));
    }

    // Spark has no Float16 type and widens it to Float32. Do not allow arbitrary floating-point
    // conversions: LanceArrowWriter cannot bind FloatType to Float8Vector or DoubleType to Float4.
    if (ot instanceof ArrowType.FloatingPoint && st instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint of = (ArrowType.FloatingPoint) ot;
      ArrowType.FloatingPoint sf = (ArrowType.FloatingPoint) st;
      return of.equals(sf)
          || (of.getPrecision() == FloatingPointPrecision.HALF
              && sf.getPrecision() == FloatingPointPrecision.SINGLE);
    }

    if (ot.equals(st)) {
      return childrenCompatible(orig, spark, true);
    }

    // String family: Utf8 <-> LargeUtf8
    if (isStringFamily(ot) && isStringFamily(st)) {
      return true;
    }

    // Binary family: Binary <-> LargeBinary <-> FixedSizeBinary
    if (isBinaryFamily(ot) && isBinaryFamily(st)) {
      return true;
    }

    return false;
  }

  private static boolean childrenCompatible(Field orig, Field spark, boolean compareNames) {
    if (orig.getChildren().size() != spark.getChildren().size()) {
      return false;
    }
    for (int i = 0; i < orig.getChildren().size(); i++) {
      Field originalChild = orig.getChildren().get(i);
      Field sparkChild = spark.getChildren().get(i);
      if ((compareNames && !originalChild.getName().equals(sparkChild.getName()))
          || !isFieldCompatible(originalChild, sparkChild)) {
        return false;
      }
    }
    return true;
  }

  private static boolean hasTimezone(ArrowType.Timestamp timestamp) {
    return timestamp.getTimezone() != null && !timestamp.getTimezone().isEmpty();
  }

  private static boolean isListFamily(ArrowType type) {
    return type instanceof ArrowType.List
        || type instanceof ArrowType.FixedSizeList
        || type instanceof ArrowType.LargeList;
  }

  private static boolean isStringFamily(ArrowType type) {
    return type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8;
  }

  private static boolean isBinaryFamily(ArrowType type) {
    return type instanceof ArrowType.Binary
        || type instanceof ArrowType.LargeBinary
        || type instanceof ArrowType.FixedSizeBinary;
  }
}
