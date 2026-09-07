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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;

import java.util.Map;

/**
 * Helpers for Lance JSON columns, which Lance models as an Arrow extension type over UTF-8 storage.
 *
 * <p>Spark has no JSON type, so a JSON column surfaces as {@link StringType} carrying the extension
 * name in the field metadata — the same approach {@link LargeVarCharUtils} uses for Arrow
 * LargeUtf8. The JSON text itself is what Spark reads and writes; Lance encodes it to its internal
 * JSONB form on write and decodes on read, so the connector never handles JSONB bytes.
 *
 * <p>Two spellings of the extension name are in play, and they are not interchangeable:
 *
 * <ul>
 *   <li>{@code arrow.json} — the canonical Arrow extension name. This is what a producer declares,
 *       and the only spelling lance-core recognizes when validating a write against an existing
 *       schema.
 *   <li>{@code lance.json} — the form lance-core reports back when a dataset is opened. It is an
 *       internal label; declaring it on a UTF-8 field is silently ignored, leaving an ordinary
 *       string column that merely looks like JSON.
 * </ul>
 *
 * <p>Both are recognized at the Arrow boundary. The connector normalizes either spelling to
 * {@link #ARROW_JSON_EXTENSION_NAME} when constructing Spark metadata, so Spark schemas and all
 * connector writes use the canonical Arrow name.
 */
public class JsonUtils {

  /** The canonical Arrow extension name, and the only one safe to write. */
  public static final String ARROW_JSON_EXTENSION_NAME = "arrow.json";

  /**
   * The internal spelling lance-core reports when a dataset is opened. Recognized, never written.
   */
  public static final String LANCE_JSON_EXTENSION_NAME = "lance.json";

  private JsonUtils() {}

  /**
   * Checks whether an extension name denotes a JSON column, in either spelling.
   *
   * @param extensionName the value of the {@code ARROW:extension:name} key, may be null
   * @return true if the name denotes a JSON column
   */
  public static boolean isJsonExtensionName(String extensionName) {
    return ARROW_JSON_EXTENSION_NAME.equals(extensionName)
        || LANCE_JSON_EXTENSION_NAME.equals(extensionName);
  }

  /**
   * Checks whether an Arrow field is a JSON column.
   *
   * <p>The storage type is deliberately not checked. Lance reports JSON columns as LargeBinary
   * because Arrow Java does not register the extension type, while other producers may present Utf8
   * or LargeUtf8; the extension name is the reliable signal in every case.
   *
   * @param field the Arrow field to check
   * @return true if the field is a JSON column
   */
  public static boolean hasJsonArrowExtension(Field field) {
    if (field == null) {
      return false;
    }

    Map<String, String> metadata = field.getMetadata();
    if (metadata == null) {
      return false;
    }

    return isJsonExtensionName(metadata.get(BlobUtils.ARROW_EXTENSION_NAME_KEY));
  }

  /**
   * Checks whether an Arrow field uses Lance's physical JSON representation.
   *
   * @param field the Arrow field to check
   * @return true if the field has the {@code lance.json} extension name
   */
  public static boolean isLanceJsonField(Field field) {
    if (field == null) {
      return false;
    }

    Map<String, String> metadata = field.getMetadata();
    return metadata != null
        && LANCE_JSON_EXTENSION_NAME.equals(metadata.get(BlobUtils.ARROW_EXTENSION_NAME_KEY));
  }

  /**
   * Checks whether Spark metadata carries a JSON extension marker.
   *
   * @param metadata the Spark field metadata, may be null
   * @return true if the metadata marks a JSON column
   */
  public static boolean hasJsonMetadata(Metadata metadata) {
    if (metadata == null || !metadata.contains(BlobUtils.ARROW_EXTENSION_NAME_KEY)) {
      return false;
    }

    return isJsonExtensionName(metadata.getString(BlobUtils.ARROW_EXTENSION_NAME_KEY));
  }

  /**
   * Checks whether a Spark field is a JSON column.
   *
   * @param field the Spark struct field to check
   * @return true if the field is a StringType column marked as JSON
   */
  public static boolean isJsonSparkField(StructField field) {
    if (field == null || !(field.dataType() instanceof StringType)) {
      return false;
    }

    return hasJsonMetadata(field.metadata());
  }
}
