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

import org.apache.spark.sql.types.StructField;

public class BlobUtils {

  public static final String LANCE_ENCODING_BLOB_KEY = "lance-encoding:blob";
  public static final String LANCE_ENCODING_BLOB_VALUE = "true";

  /**
   * Check if a Spark field is a blob field based on its metadata.
   *
   * @param field the Spark struct field to check
   * @return true if the field is a blob field, false otherwise
   */
  public static boolean isBlobSparkField(StructField field) {
    if (field == null) {
      return false;
    }

    if (field.metadata() == null) {
      return false;
    }

    if (!field.metadata().contains(LANCE_ENCODING_BLOB_KEY)) {
      return false;
    }

    String value = field.metadata().getString(LANCE_ENCODING_BLOB_KEY);
    return LANCE_ENCODING_BLOB_VALUE.equalsIgnoreCase(value);
  }

  /**
   * Check if an Arrow field is a blob field based on its metadata.
   *
   * @param field the Arrow field to check
   * @return true if the field is a blob field, false otherwise
   */
  public static boolean isBlobArrowField(org.apache.arrow.vector.types.pojo.Field field) {
    if (field == null) {
      return false;
    }

    java.util.Map<String, String> metadata = field.getMetadata();
    if (metadata == null) {
      return false;
    }

    if (!metadata.containsKey(LANCE_ENCODING_BLOB_KEY)) {
      return false;
    }

    String value = metadata.get(LANCE_ENCODING_BLOB_KEY);
    return LANCE_ENCODING_BLOB_VALUE.equalsIgnoreCase(value);
  }
}
