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

/**
 * Utility class for LargeBinary Arrow type metadata handling.
 *
 * <p>Spark has a single {@code BinaryType} that covers both Arrow {@code Binary} (32-bit offsets)
 * and Arrow {@code LargeBinary} (64-bit offsets). When reading a Lance table containing a
 * LargeBinary column, the distinction is preserved in Spark field metadata so a subsequent write
 * reproduces LargeBinary instead of silently narrowing the column to Binary.
 *
 * <p>This mirrors {@link LargeVarCharUtils}, which does the same for LargeUtf8.
 */
public class LargeVarBinaryUtils {

  public static final String ARROW_LARGE_VAR_BINARY_KEY = "arrow:large-var-binary";
  public static final String ARROW_LARGE_VAR_BINARY_VALUE = "true";

  /**
   * Create the property key for configuring large binary on a column.
   *
   * @param fieldName the name of the field
   * @return the property key (e.g., "my_column.arrow.large_var_binary")
   */
  public static String createPropertyKey(String fieldName) {
    return fieldName + ".arrow.large_var_binary";
  }
}
