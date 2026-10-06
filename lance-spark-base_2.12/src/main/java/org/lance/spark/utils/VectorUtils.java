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

public class VectorUtils {

  public static final String ARROW_FIXED_SIZE_LIST_SIZE_KEY = "arrow.fixed-size-list.size";

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
