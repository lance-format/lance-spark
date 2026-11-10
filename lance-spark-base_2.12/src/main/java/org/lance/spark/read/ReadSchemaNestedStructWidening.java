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
package org.lance.spark.read;

import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

/**
 * Aligns Spark's column-pruned read schema with full Arrow batches from Lance.
 *
 * <p>When {@link org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns#pruneColumns}
 * narrows a nested {@link StructType} to a subset of fields (e.g. only one nested column),
 * vectorized execution still uses child ordinals from that pruned struct. Lance always returns the
 * full struct column from the scanner, and {@link org.lance.spark.vectorized.LanceStructAccessor}
 * indexes Arrow children by full struct ordinal. Replacing pruned nested structs with the table's
 * full struct definition restores ordinal agreement between Catalyst and Arrow.
 */
final class ReadSchemaNestedStructWidening {

  private ReadSchemaNestedStructWidening() {}

  static void validateRequiredSchema(StructType required, StructType tableFull) {
    validateRequiredType(required, tableFull, "");
  }

  private static void validateRequiredType(DataType required, DataType tableFull, String path) {
    if (required instanceof StructType && tableFull instanceof StructType) {
      Map<String, StructField> tableFields = indexByName((StructType) tableFull);
      for (StructField field : ((StructType) required).fields()) {
        StructField tableField = tableFields.get(field.name());
        String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
        if (tableField == null) {
          if (path.isEmpty() && Utils.isMetadataColumn(field)) {
            continue;
          }
          throw new IllegalArgumentException(
              "Column '"
                  + fieldPath
                  + "' does not exist in the selected snapshot. "
                  + "Select compatible columns or use SQL VERSION AS OF to read its schema.");
        }
        validateRequiredType(field.dataType(), tableField.dataType(), fieldPath);
      }
    } else if (required instanceof ArrayType && tableFull instanceof ArrayType) {
      validateRequiredType(
          ((ArrayType) required).elementType(), ((ArrayType) tableFull).elementType(), path + "[]");
    } else if (required instanceof MapType && tableFull instanceof MapType) {
      validateRequiredType(
          ((MapType) required).keyType(), ((MapType) tableFull).keyType(), path + ".key");
      validateRequiredType(
          ((MapType) required).valueType(), ((MapType) tableFull).valueType(), path + ".value");
    } else if (!required.equals(tableFull)) {
      throw new IllegalArgumentException(
          "Column '"
              + path
              + "' requires "
              + required.simpleString()
              + " but the selected snapshot has "
              + tableFull.simpleString()
              + ". Select compatible columns or use SQL VERSION AS OF to read its schema.");
    }
  }

  static StructType widenRequiredSchema(StructType required, StructType tableFull) {
    if (required == null || tableFull == null) {
      return required;
    }
    Map<String, StructField> tableFieldsByName = indexByName(tableFull);
    StructField[] widenedFields = new StructField[required.length()];
    for (int fieldIndex = 0; fieldIndex < required.length(); fieldIndex++) {
      StructField requiredField = required.fields()[fieldIndex];
      StructField tableField = tableFieldsByName.get(requiredField.name());
      widenedFields[fieldIndex] =
          tableField != null ? widenField(requiredField, tableField) : requiredField;
    }
    return new StructType(widenedFields);
  }

  private static StructField widenField(StructField required, StructField tableFull) {
    DataType widened = widenDataType(required.dataType(), tableFull.dataType());
    // Column pruning rebuilds required fields from Catalyst attributes and drops custom metadata.
    // For blob v2 descriptor columns keep the lance.blob.v2 flag from the table field so the
    // scanner detects them by flag, not struct shape.
    Metadata metadata =
        BlobUtils.isBlobV2SparkField(tableFull) ? tableFull.metadata() : required.metadata();
    return new StructField(required.name(), widened, required.nullable(), metadata);
  }

  private static DataType widenDataType(DataType required, DataType tableFull) {
    if (required instanceof StructType && tableFull instanceof StructType) {
      return widenStructType((StructType) required, (StructType) tableFull);
    }
    if (required instanceof ArrayType && tableFull instanceof ArrayType) {
      return widenArrayType((ArrayType) required, (ArrayType) tableFull);
    }
    if (required instanceof MapType && tableFull instanceof MapType) {
      return widenMapType((MapType) required, (MapType) tableFull);
    }
    return required;
  }

  /**
   * Widens a pruned nested struct back to the full table struct field order and field set.
   *
   * <p>Only widens when {@code requiredStruct} is a name-subset of {@code tableStruct} — i.e. every
   * field in the required schema also exists in the table schema. Fields present in the table but
   * absent from the required schema are filled in from the table definition unchanged, preserving
   * the Arrow child ordinals that {@link org.lance.spark.vectorized.LanceStructAccessor} relies on.
   */
  private static DataType widenStructType(StructType requiredStruct, StructType tableStruct) {
    if (tableStruct.size() < requiredStruct.size()) {
      return requiredStruct;
    }
    Map<String, StructField> requiredFieldsByName = indexByName(requiredStruct);

    // Single pass over table fields: validate all required names are present, then widen.
    // Building only one map (required) avoids an extra O(n) allocation and pass over table fields.
    StructField[] tableFields = tableStruct.fields();
    StructField[] widenedFields = new StructField[tableFields.length];
    int matchedRequiredFields = 0;
    for (int fieldIndex = 0; fieldIndex < tableFields.length; fieldIndex++) {
      StructField tableField = tableFields[fieldIndex];
      StructField requiredField = requiredFieldsByName.get(tableField.name());
      if (requiredField != null) {
        widenedFields[fieldIndex] = widenField(requiredField, tableField);
        matchedRequiredFields++;
      } else {
        widenedFields[fieldIndex] = tableField;
      }
    }
    // If not all required fields were matched, the schemas diverge — do not widen.
    if (matchedRequiredFields != requiredFieldsByName.size()) {
      return requiredStruct;
    }
    return new StructType(widenedFields);
  }

  private static DataType widenArrayType(ArrayType requiredArray, ArrayType tableArray) {
    DataType widenedElement = widenDataType(requiredArray.elementType(), tableArray.elementType());
    return new ArrayType(widenedElement, requiredArray.containsNull());
  }

  private static DataType widenMapType(MapType requiredMap, MapType tableMap) {
    DataType widenedKey = widenDataType(requiredMap.keyType(), tableMap.keyType());
    DataType widenedValue = widenDataType(requiredMap.valueType(), tableMap.valueType());
    return new MapType(widenedKey, widenedValue, requiredMap.valueContainsNull());
  }

  private static Map<String, StructField> indexByName(StructType struct) {
    Map<String, StructField> index = new HashMap<>(struct.size() * 2);
    for (StructField field : struct.fields()) {
      index.put(field.name(), field);
    }
    return index;
  }
}
