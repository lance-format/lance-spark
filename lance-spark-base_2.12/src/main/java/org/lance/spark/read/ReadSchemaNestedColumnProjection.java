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

import org.lance.spark.LanceConstant;
import org.lance.spark.utils.FieldPathUtils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ReadSchemaNestedColumnProjection {
  private ReadSchemaNestedColumnProjection() {}

  public static List<String> buildProjectedColumns(
      StructType requiredSchema, StructType fullSchema) {
    List<String> projectedColumns = new ArrayList<>();
    if (requiredSchema == null) {
      return projectedColumns;
    }

    for (StructField requiredField : requiredSchema.fields()) {
      if (isScannerSpecialField(requiredField.name())) {
        continue;
      }
      appendProjectedColumns(
          requiredField,
          findField(fullSchema, requiredField.name()),
          newPath(requiredField.name()),
          projectedColumns);
    }
    return projectedColumns;
  }

  private static void appendProjectedColumns(
      StructField requiredField,
      StructField fullField,
      List<String> columnPath,
      List<String> projectedColumns) {
    if (fullField == null) {
      throw new IllegalArgumentException(
          "Required projection column '"
              + FieldPathUtils.canonicalPath(columnPath)
              + "' with type "
              + requiredField.dataType().catalogString()
              + " does not exist in the full schema");
    }

    if (shouldProjectNestedChildren(requiredField, fullField)) {
      StructType requiredStruct = (StructType) requiredField.dataType();
      StructType fullStruct = (StructType) fullField.dataType();
      for (StructField childField : requiredStruct.fields()) {
        appendProjectedColumns(
            childField,
            findField(fullStruct, childField.name()),
            appendPath(columnPath, childField.name()),
            projectedColumns);
      }
      return;
    }

    projectedColumns.add(FieldPathUtils.canonicalPath(columnPath));
  }

  private static List<String> newPath(String fieldName) {
    List<String> path = new ArrayList<>();
    path.add(fieldName);
    return path;
  }

  private static List<String> appendPath(List<String> path, String fieldName) {
    List<String> childPath = new ArrayList<>(path.size() + 1);
    childPath.addAll(path);
    childPath.add(fieldName);
    return childPath;
  }

  private static boolean shouldProjectNestedChildren(
      StructField requiredField, StructField fullField) {
    if (fullField == null) {
      return false;
    }

    DataType requiredType = requiredField.dataType();
    DataType fullType = fullField.dataType();
    return requiredType instanceof StructType
        && fullType instanceof StructType
        // Reconstructing a nullable parent struct from projected child vectors loses the
        // parent validity bitmap. Keep nullable structs as top-level projections so Spark
        // still observes exact parent null semantics.
        && !fullField.nullable()
        && !requiredType.equals(fullType);
  }

  private static StructField findField(StructType schema, String fieldName) {
    if (schema == null) {
      return null;
    }
    return Arrays.stream(schema.fields())
        .filter(field -> field.name().equals(fieldName))
        .findFirst()
        .orElse(null);
  }

  private static boolean isScannerSpecialField(String fieldName) {
    return fieldName.equals(LanceConstant.FRAGMENT_ID)
        || fieldName.equals(LanceConstant.ROW_ID)
        || fieldName.equals(LanceConstant.ROW_ADDRESS)
        || fieldName.equals(LanceConstant.ROW_CREATED_AT_VERSION)
        || fieldName.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
        || fieldName.equals(LanceConstant.SCORE)
        || fieldName.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
        || fieldName.endsWith(LanceConstant.BLOB_SIZE_SUFFIX);
  }
}
