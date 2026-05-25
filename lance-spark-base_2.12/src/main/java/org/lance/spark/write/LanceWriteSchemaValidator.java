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

import org.lance.spark.utils.BlobUtils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class LanceWriteSchemaValidator {

  private LanceWriteSchemaValidator() {}

  public static void validate(StructType tableSchema, StructType inputSchema) {
    StructField[] tableFields = tableSchema.fields();
    StructField[] inputFields = inputSchema.fields();

    if (tableFields.length != inputFields.length) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot write to Lance table because the column count is different. "
                  + "Table has %d columns, input has %d.",
              tableFields.length, inputFields.length));
    }

    StructField[] resolved = resolveFields(tableFields, inputFields);

    List<String> errors = new ArrayList<>();
    for (int i = 0; i < inputFields.length; i++) {
      checkField(resolved[i].name(), resolved[i], inputFields[i], errors);
    }

    if (errors.isEmpty()) {
      return;
    }

    throw new IllegalArgumentException(
        String.format(
            "Cannot write to Lance table because the schemas do not match: %s",
            String.join("; ", errors)));
  }

  private static void checkField(
      String path, StructField tableField, StructField inputField, List<String> errors) {
    if (!tableField.nullable() && inputField.nullable()) {
      errors.add(
          String.format(
              "column '%s': the table column does not allow nulls, but the input does", path));
    }

    DataType tableType = tableField.dataType();
    DataType inputType = inputField.dataType();

    if (BlobUtils.isBlobV2SparkField(tableField)) {
      if (!DataTypes.BinaryType.equals(inputType)) {
        errors.add(
            String.format(
                "column '%s': blob v2 columns accept binary data; got %s",
                path, inputType.simpleString()));
      }
      return;
    }

    if (tableType instanceof StructType && inputType instanceof StructType) {
      checkStruct(path, (StructType) tableType, (StructType) inputType, errors);
      return;
    }

    if (!tableType.equals(inputType)) {
      errors.add(
          String.format(
              "column '%s': expected %s, got %s",
              path, tableType.simpleString(), inputType.simpleString()));
    }
  }

  private static void checkStruct(
      String path, StructType tableStruct, StructType inputStruct, List<String> errors) {
    StructField[] tableFields = tableStruct.fields();
    StructField[] inputFields = inputStruct.fields();

    if (tableFields.length != inputFields.length) {
      errors.add(
          String.format(
              "column '%s': the struct has %d fields in the table but %d in the input",
              path, tableFields.length, inputFields.length));
      return;
    }

    for (int i = 0; i < tableFields.length; i++) {
      StructField tableField = tableFields[i];
      StructField inputField = inputFields[i];
      if (!tableField.name().equals(inputField.name())) {
        errors.add(
            String.format(
                "column '%s': struct fields are in the wrong order; expected '%s' at position %d,"
                    + " got '%s'",
                path, tableField.name(), i, inputField.name()));
        return;
      }
      checkField(path + "." + tableField.name(), tableField, inputField, errors);
    }
  }

  // LanceArrowWriter maps input column i to table column i. Match by name in table order,
  // or accept Spark's SQL VALUES column names {col1, col2, ...}.
  private static StructField[] resolveFields(StructField[] tableFields, StructField[] inputFields) {
    Map<String, StructField> tableByName = new HashMap<>(tableFields.length);
    for (StructField field : tableFields) {
      tableByName.put(field.name(), field);
    }

    StructField[] resolved = new StructField[inputFields.length];
    int matchedByName = 0;
    for (int i = 0; i < inputFields.length; i++) {
      resolved[i] = tableByName.get(inputFields[i].name());
      if (resolved[i] != null) {
        matchedByName++;
      }
    }

    if (matchedByName == inputFields.length) {
      for (int i = 0; i < inputFields.length; i++) {
        if (!tableFields[i].name().equals(inputFields[i].name())) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot write to Lance table because column names match, but the "
                      + "order is different. Expected column order: %s.",
                  quotedNames(tableFields)));
        }
      }
      return tableFields;
    }
    if (matchedByName == 0 && isSparkValuesColumns(inputFields)) {
      return tableFields;
    }

    List<String> unknownFields = new ArrayList<>();
    for (int i = 0; i < inputFields.length; i++) {
      if (resolved[i] == null) {
        unknownFields.add(String.format("'%s'", inputFields[i].name()));
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Cannot write to Lance table because input columns %s are not in the table. Table"
                + " columns: %s",
            String.join(", ", unknownFields), quotedNames(tableFields)));
  }

  private static boolean isSparkValuesColumns(StructField[] inputFields) {
    for (int i = 0; i < inputFields.length; i++) {
      if (!String.format("col%d", i + 1).equals(inputFields[i].name())) {
        return false;
      }
    }
    return true;
  }

  private static String quotedNames(StructField[] fields) {
    List<String> names = new ArrayList<>(fields.length);
    for (StructField field : fields) {
      names.add(String.format("\"%s\"", field.name()));
    }
    return String.join(", ", names);
  }
}
