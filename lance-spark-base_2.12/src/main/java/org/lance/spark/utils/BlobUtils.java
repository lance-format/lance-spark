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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class BlobUtils {

  public static final String LANCE_ENCODING_BLOB_KEY = "lance-encoding:blob";
  public static final String LANCE_ENCODING_BLOB_VALUE = "true";

  public static final String ARROW_EXTENSION_NAME_KEY = "ARROW:extension:name";
  public static final String ARROW_EXTENSION_BLOB_V2 = "lance.blob.v2";

  /**
   * Spark struct type for a Lance blob v2 descriptor: {@code kind, position, size, blob_id,
   * blob_uri}.
   */
  public static final StructType BLOB_DESCRIPTOR_STRUCT =
      new StructType()
          .add("kind", DataTypes.ShortType)
          .add("position", DataTypes.LongType)
          .add("size", DataTypes.LongType)
          .add("blob_id", DataTypes.LongType)
          .add("blob_uri", DataTypes.StringType);

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

  /** Returns true when a Spark field carries the lance-core blob v2 Arrow extension. */
  public static boolean isBlobV2SparkField(StructField field) {
    return field != null && isBlobV2SparkMetadata(field.metadata());
  }

  public static boolean isBlobV2SparkMetadata(Metadata metadata) {
    if (metadata == null) {
      return false;
    }

    return metadata.contains(ARROW_EXTENSION_NAME_KEY)
        && ARROW_EXTENSION_BLOB_V2.equals(metadata.getString(ARROW_EXTENSION_NAME_KEY));
  }

  /**
   * Arrow-side counterpart of {@link #isBlobV2SparkField} used inside the columnar batch scanner.
   */
  public static boolean isBlobV2ArrowField(Field field) {
    if (field == null) {
      return false;
    }

    Map<String, String> metadata = field.getMetadata();
    if (metadata != null
        && ARROW_EXTENSION_BLOB_V2.equals(metadata.get(ARROW_EXTENSION_NAME_KEY))) {
      return true;
    }

    // lance-core scan batches expose the unloaded descriptor struct (no extension metadata).
    return isBlobV2DescriptorArrowField(field);
  }

  private static boolean isBlobV2DescriptorArrowField(Field field) {
    if (!(field.getType() instanceof ArrowType.Struct)) {
      return false;
    }
    List<Field> children = field.getChildren();
    if (children == null || children.size() != BLOB_DESCRIPTOR_STRUCT.fields().length) {
      return false;
    }
    StructField[] expected = BLOB_DESCRIPTOR_STRUCT.fields();
    for (int i = 0; i < expected.length; i++) {
      if (!expected[i].name().equals(children.get(i).getName())) {
        return false;
      }
    }
    return true;
  }

  /** Returns true if any field in {@code schema} is a blob v2 column. */
  public static boolean hasBlobV2Fields(StructType schema) {
    for (StructField field : schema.fields()) {
      if (isBlobV2SparkField(field)) {
        return true;
      }
    }

    return false;
  }

  /** Rewrites blob v2 columns to the descriptor struct returned by Lance. */
  public static StructType applyBlobV2DescriptorSchema(StructType schema) {
    StructField[] fields = new StructField[schema.fields().length];
    boolean changed = false;
    for (int i = 0; i < schema.fields().length; i++) {
      StructField field = schema.fields()[i];
      if (!isBlobV2SparkField(field)) {
        fields[i] = field;
        continue;
      }

      fields[i] =
          new StructField(field.name(), BLOB_DESCRIPTOR_STRUCT, field.nullable(), field.metadata());
      changed = true;
    }

    return changed ? new StructType(fields) : schema;
  }
}
