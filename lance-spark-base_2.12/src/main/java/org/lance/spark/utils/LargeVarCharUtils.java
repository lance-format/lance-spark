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

public class LargeVarCharUtils {

  public static final String ARROW_LARGE_VAR_CHAR_KEY = "arrow:large-var-char";
  public static final String ARROW_LARGE_VAR_CHAR_VALUE = "true";

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
