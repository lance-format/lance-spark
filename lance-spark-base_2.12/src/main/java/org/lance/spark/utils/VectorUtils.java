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

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.StructField;

public class VectorUtils {

  public static final String ARROW_FIXED_SIZE_LIST_SIZE_KEY = "arrow.fixed-size-list.size";

  /**
   * Check if a Spark field is a vector field (FixedSizeList) based on its metadata.
   *
   * @param field the Spark struct field to check
   * @return true if the field is a vector field, false otherwise
   */
  public static boolean isVectorField(StructField field) {
    if (field == null) {
      return false;
    }

    // Check if it's an array type with numeric elements
    if (!(field.dataType() instanceof ArrayType)) {
      return false;
    }

    ArrayType arrayType = (ArrayType) field.dataType();
    DataType elementType = arrayType.elementType();

    // Vectors must have float or double elements
    if (!(elementType instanceof FloatType || elementType instanceof DoubleType)) {
      return false;
    }

    // Check for fixed-size-list metadata
    if (field.metadata() == null) {
      return false;
    }

    return field.metadata().contains(ARROW_FIXED_SIZE_LIST_SIZE_KEY);
  }

  /**
   * Get the vector dimension from a Spark field's metadata.
   *
   * @param field the Spark struct field
   * @return the vector dimension, or -1 if not a vector field
   */
  public static long getVectorDimension(StructField field) {
    if (!isVectorField(field)) {
      return -1;
    }

    try {
      return field.metadata().getLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY);
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Check if an Arrow field is a vector field (FixedSizeList).
   *
   * @param field the Arrow field to check
   * @return true if the field is a vector field, false otherwise
   */
  public static boolean isVectorArrowField(org.apache.arrow.vector.types.pojo.Field field) {
    if (field == null) {
      return false;
    }

    // Check if the Arrow type is FixedSizeList
    if (!(field.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList)) {
      return false;
    }

    // Optionally check if children have numeric types
    if (field.getChildren().isEmpty()) {
      return false;
    }

    org.apache.arrow.vector.types.pojo.Field childField = field.getChildren().get(0);
    org.apache.arrow.vector.types.pojo.ArrowType childType = childField.getType();

    // Check if child is a numeric type (Float or Double)
    return childType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
  }

  /**
   * Get the vector dimension from an Arrow FixedSizeList field.
   *
   * @param field the Arrow field
   * @return the vector dimension, or -1 if not a vector field
   */
  public static int getVectorArrowDimension(org.apache.arrow.vector.types.pojo.Field field) {
    if (!isVectorArrowField(field)) {
      return -1;
    }

    org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList fixedSizeList =
        (org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList) field.getType();
    return fixedSizeList.getListSize();
  }

  /**
   * Check if a Spark DataType should be treated as a FixedSizeList based on metadata.
   *
   * @param dataType the Spark data type
   * @param metadata the field metadata
   * @return true if it should be a FixedSizeList, false otherwise
   */
  public static boolean shouldBeFixedSizeList(
      DataType dataType, org.apache.spark.sql.types.Metadata metadata) {
    if (metadata == null || !metadata.contains(ARROW_FIXED_SIZE_LIST_SIZE_KEY)) {
      return false;
    }

    if (!(dataType instanceof ArrayType)) {
      return false;
    }

    ArrayType arrayType = (ArrayType) dataType;
    DataType elementType = arrayType.elementType();

    // Only numeric types are supported for vectors
    return elementType instanceof FloatType || elementType instanceof DoubleType;
  }

  /**
   * Create a property key for specifying vector dimensions in table properties. This is used in
   * CREATE TABLE statements.
   *
   * @param columnName the name of the column
   * @return the property key for specifying vector dimension
   */
  public static String createVectorSizePropertyKey(String columnName) {
    return columnName + "." + ARROW_FIXED_SIZE_LIST_SIZE_KEY;
  }
}
