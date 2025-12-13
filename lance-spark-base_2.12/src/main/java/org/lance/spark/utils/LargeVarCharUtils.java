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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;

import java.util.Map;

public class LargeVarCharUtils {

  public static final String ARROW_LARGE_VAR_CHAR_KEY = "arrow:large-var-char";
  public static final String ARROW_LARGE_VAR_CHAR_VALUE = "true";

  /**
   * Check if a Spark field is a large varchar field based on its metadata.
   *
   * @param field the Spark struct field to check
   * @return true if the field is a large varchar field, false otherwise
   */
  public static boolean isLargeVarCharSparkField(StructField field) {
    if (field == null) {
      return false;
    }

    if (field.metadata() == null) {
      return false;
    }

    if (!(field.dataType() instanceof StringType)) {
      return false;
    }

    if (!field.metadata().contains(ARROW_LARGE_VAR_CHAR_KEY)) {
      return false;
    }

    String value = field.metadata().getString(ARROW_LARGE_VAR_CHAR_KEY);
    return ARROW_LARGE_VAR_CHAR_VALUE.equalsIgnoreCase(value);
  }

  /**
   * Check if an Arrow field is a large varchar field based on its metadata.
   *
   * @param field the Arrow field to check
   * @return true if the field is a large varchar field, false otherwise
   */
  public static boolean isLargeVarCharArrowField(Field field) {
    if (field == null) {
      return false;
    }

    Map<String, String> metadata = field.getMetadata();
    if (metadata == null) {
      return false;
    }

    if (!metadata.containsKey(ARROW_LARGE_VAR_CHAR_KEY)) {
      return false;
    }

    String value = metadata.get(ARROW_LARGE_VAR_CHAR_KEY);
    return ARROW_LARGE_VAR_CHAR_VALUE.equalsIgnoreCase(value);
  }

  /**
   * Create the property key for configuring large varchar on a column.
   *
   * @param fieldName the name of the field
   * @return the property key (e.g., "my_column.arrow.large_var_char")
   */
  public static String createPropertyKey(String fieldName) {
    return fieldName + ".arrow.large_var_char";
  }
}
