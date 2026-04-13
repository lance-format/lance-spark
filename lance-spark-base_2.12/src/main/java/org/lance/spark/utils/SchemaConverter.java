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
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

import static org.lance.spark.utils.BlobUtils.LANCE_ENCODING_BLOB_KEY;
import static org.lance.spark.utils.BlobUtils.LANCE_ENCODING_BLOB_VALUE;
import static org.lance.spark.utils.Float16Utils.ARROW_FLOAT16_KEY;
import static org.lance.spark.utils.Float16Utils.ARROW_FLOAT16_VALUE;
import static org.lance.spark.utils.LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_KEY;
import static org.lance.spark.utils.LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_VALUE;
import static org.lance.spark.utils.VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY;

/**
 * Utility class that augments a Spark {@link StructType} with Lance-specific column metadata
 * (vector fixed-size-list, Float16, blob, large varchar, compression) derived from table
 * properties.
 */
public class SchemaConverter {

  private SchemaConverter() {
    // Utility class
  }

  /**
   * Processes a Spark schema with table properties to add metadata for vector, blob, large varchar,
   * and compression columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain column metadata
   * @return StructType with metadata added for matching columns
   */
  public static StructType processSchemaWithProperties(
      StructType sparkSchema, Map<String, String> properties) {
    StructType schemaWithVectors = addVectorMetadata(sparkSchema, properties);
    StructType schemaWithFloat16 = addFloat16Metadata(schemaWithVectors, properties);
    StructType schemaWithBlobs = addBlobMetadata(schemaWithFloat16, properties);
    StructType schemaWithLargeVarChar = addLargeVarCharMetadata(schemaWithBlobs, properties);
    return addCompressionMetadata(schemaWithLargeVarChar, properties);
  }

  /**
   * Adds metadata to ArrayType fields based on table properties for vector columns. Properties with
   * pattern "<column_name>.arrow.fixed-size-list.size" are applied to matching columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain vector column metadata
   * @return StructType with metadata added for vector columns
   */
  private static StructType addVectorMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String vectorSizeProperty = VectorUtils.createVectorSizePropertyKey(field.name());

      if (properties.containsKey(vectorSizeProperty)) {
        // This field should be a vector column
        if (field.dataType() instanceof ArrayType) {
          ArrayType arrayType = (ArrayType) field.dataType();
          DataType elementType = arrayType.elementType();

          // Validate element type is FloatType or DoubleType
          if (elementType instanceof FloatType || elementType instanceof DoubleType) {
            // Add metadata for FixedSizeList
            long vectorSize = Long.parseLong(properties.get(vectorSizeProperty));
            Metadata newMetadata =
                new MetadataBuilder()
                    .withMetadata(field.metadata())
                    .putLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY, vectorSize)
                    .build();
            newFields[i] =
                new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
          } else {
            throw new IllegalArgumentException(
                "Vector column '"
                    + field.name()
                    + "' must have element type FLOAT or DOUBLE, found: "
                    + elementType);
          }
        } else {
          throw new IllegalArgumentException(
              "Column '"
                  + field.name()
                  + "' has vector property but is not an ARRAY type: "
                  + field.dataType());
        }
      } else {
        // Keep field as-is
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Adds float16 metadata to vector fields based on table properties. Properties with pattern
   * "<column_name>.arrow.float16" = "true" are applied to matching columns. The field must already
   * have fixed-size-list metadata and be ArrayType(FloatType).
   *
   * @param sparkSchema the Spark StructType (already processed by addVectorMetadata)
   * @param properties table properties that may contain float16 column metadata
   * @return StructType with float16 metadata added
   */
  private static StructType addFloat16Metadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String float16Property = Float16Utils.createPropertyKey(field.name());

      if (properties.containsKey(float16Property)) {
        String value = properties.get(float16Property);
        if ("true".equalsIgnoreCase(value)) {
          // Validate: must be ArrayType(FloatType) with fixed-size-list metadata
          if (!(field.dataType() instanceof ArrayType)) {
            throw new IllegalArgumentException(
                "Float16 column '"
                    + field.name()
                    + "' must be an ARRAY type, found: "
                    + field.dataType());
          }
          ArrayType arrayType = (ArrayType) field.dataType();
          if (!(arrayType.elementType() instanceof FloatType)) {
            throw new IllegalArgumentException(
                "Float16 column '"
                    + field.name()
                    + "' must have element type FLOAT, found: "
                    + arrayType.elementType());
          }
          if (!field.metadata().contains(ARROW_FIXED_SIZE_LIST_SIZE_KEY)) {
            throw new IllegalArgumentException(
                "Float16 column '"
                    + field.name()
                    + "' must also have '"
                    + ARROW_FIXED_SIZE_LIST_SIZE_KEY
                    + "' property set");
          }
          Metadata newMetadata =
              new MetadataBuilder()
                  .withMetadata(field.metadata())
                  .putString(ARROW_FLOAT16_KEY, ARROW_FLOAT16_VALUE)
                  .build();
          newFields[i] =
              new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
        } else {
          newFields[i] = field;
        }
      } else {
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Adds metadata to BinaryType fields based on table properties for blob columns. Properties with
   * pattern "<column_name>.lance.encoding" = "blob" are applied to matching columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain blob column metadata
   * @return StructType with metadata added for blob columns
   */
  private static StructType addBlobMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String blobEncodingProperty = field.name() + ".lance.encoding";

      if (properties.containsKey(blobEncodingProperty)) {
        // This field should be a blob column
        String encodingValue = properties.get(blobEncodingProperty);
        if ("blob".equalsIgnoreCase(encodingValue)) {
          if (field.dataType() instanceof BinaryType) {
            // Add metadata for blob encoding
            Metadata newMetadata =
                new MetadataBuilder()
                    .withMetadata(field.metadata())
                    .putString(LANCE_ENCODING_BLOB_KEY, LANCE_ENCODING_BLOB_VALUE)
                    .build();
            newFields[i] =
                new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
          } else {
            throw new IllegalArgumentException(
                "Blob column '"
                    + field.name()
                    + "' must have BINARY type, found: "
                    + field.dataType());
          }
        } else {
          // Keep field as-is if encoding value is not blob
          newFields[i] = field;
        }
      } else {
        // Keep field as-is
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Adds metadata to StringType fields based on table properties for large varchar columns.
   * Properties with pattern "<column_name>.arrow.large_var_char" = "true" are applied to matching
   * columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain large varchar column metadata
   * @return StructType with metadata added for large varchar columns
   */
  private static StructType addLargeVarCharMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      String largeVarCharProperty = LargeVarCharUtils.createPropertyKey(field.name());

      if (properties.containsKey(largeVarCharProperty)) {
        // This field should be a large varchar column
        String encodingValue = properties.get(largeVarCharProperty);
        if ("true".equalsIgnoreCase(encodingValue)) {
          if (field.dataType() instanceof StringType) {
            // Add metadata for large varchar
            Metadata newMetadata =
                new MetadataBuilder()
                    .withMetadata(field.metadata())
                    .putString(ARROW_LARGE_VAR_CHAR_KEY, ARROW_LARGE_VAR_CHAR_VALUE)
                    .build();
            newFields[i] =
                new StructField(field.name(), field.dataType(), field.nullable(), newMetadata);
          } else {
            throw new IllegalArgumentException(
                "Large varchar column '"
                    + field.name()
                    + "' must have STRING type, found: "
                    + field.dataType());
          }
        } else {
          // Keep field as-is if value is not "true"
          newFields[i] = field;
        }
      } else {
        // Keep field as-is
        newFields[i] = field;
      }
    }

    return new StructType(newFields);
  }

  /**
   * Adds Lance compression metadata to top-level fields based on connector-supported TBLPROPERTIES.
   * Keys matching {@code <column>.lance.<key>} are validated and written as {@code
   * lance-encoding:<key>} Arrow field metadata. Invalid values throw {@link
   * IllegalArgumentException} at call time.
   *
   * <p>Silent-ignore cases (no exception, no metadata written):
   *
   * <ul>
   *   <li>TBLPROPERTIES keys whose {@code <column>} segment does not match any top-level field —
   *       consistent with the behavior of the other {@code addX} methods in this class.
   *   <li>Unrecognised {@code lance.*} key suffixes (e.g. deferred dict/minichunk keys).
   *   <li>Type-incompatible combinations (e.g. {@code fsst} on a numeric column) — semantic
   *       validation is left to the Lance Rust encoder.
   * </ul>
   *
   * <p>Only top-level fields are processed; nested column paths are not supported yet.
   *
   * @param sparkSchema the Spark StructType (already processed by earlier steps)
   * @param properties table properties that may contain compression metadata
   * @return StructType with compression metadata added for matching columns
   */
  private static StructType addCompressionMetadata(
      StructType sparkSchema, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return sparkSchema;
    }

    StructField[] newFields = new StructField[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      MetadataBuilder builder = new MetadataBuilder().withMetadata(field.metadata());
      boolean modified = false;

      for (LanceEncodingUtils.EncodingPropertyRule rule :
          LanceEncodingUtils.getSupportedEncodingPropertyRules()) {
        String propertyKey = rule.createPropertyKey(field.name());
        if (!properties.containsKey(propertyKey)) {
          continue;
        }
        String value = properties.get(propertyKey);
        rule.validate(field.name(), value);
        builder.putString(rule.getArrowMetadataKey(), value);
        modified = true;
      }

      newFields[i] =
          modified
              ? new StructField(field.name(), field.dataType(), field.nullable(), builder.build())
              : field;
    }

    return new StructType(newFields);
  }
}
