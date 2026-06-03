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

import org.apache.spark.sql.types.Metadata;

/**
 * Utility class for FixedSizeBinary Arrow type metadata handling. When reading a Lance file that
 * contains FixedSizeBinary(n) columns, the byte width is preserved in Spark field metadata so
 * subsequent writes can reproduce the original Arrow type instead of falling back to
 * variable-length Binary.
 */
public class FixedSizeBinaryUtils {

  public static final String ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY =
      "arrow.fixed-size-binary.byte-width";

  /**
   * Check if metadata contains the FixedSizeBinary byte-width key.
   *
   * @param metadata the Spark field metadata
   * @return true if the metadata contains the byte-width key
   */
  public static boolean hasFixedSizeBinaryMetadata(Metadata metadata) {
    return metadata != null && metadata.contains(ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY);
  }

  /**
   * Create a table property key for specifying FixedSizeBinary byte width. Used in CREATE TABLE
   * statements, e.g. {@code 'hash.arrow.fixed-size-binary.byte-width' = '16'}.
   *
   * @param columnName the name of the column
   * @return the property key
   */
  public static String createPropertyKey(String columnName) {
    return columnName + "." + ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY;
  }
}
