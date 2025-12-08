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

import org.lance.namespace.model.JsonArrowDataType;
import org.lance.namespace.model.JsonArrowField;
import org.lance.namespace.model.JsonArrowSchema;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.sql.types.YearMonthIntervalType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.lance.spark.utils.BlobUtils.LANCE_ENCODING_BLOB_KEY;
import static org.lance.spark.utils.BlobUtils.LANCE_ENCODING_BLOB_VALUE;
import static org.lance.spark.utils.VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY;

/**
 * Utility class for converting Spark schema types to JsonArrow schema types used by the Lance
 * Namespace API.
 */
public class SchemaConverter {

  private SchemaConverter() {
    // Utility class
  }

  /**
   * Converts a Spark StructType to JsonArrowSchema.
   *
   * @param sparkSchema the Spark StructType to convert
   * @return JsonArrowSchema representation
   */
  public static JsonArrowSchema toJsonArrowSchema(StructType sparkSchema) {
    JsonArrowSchema jsonSchema = new JsonArrowSchema();
    List<JsonArrowField> fields = new ArrayList<>();

    for (StructField sparkField : sparkSchema.fields()) {
      fields.add(toJsonArrowField(sparkField));
    }

    jsonSchema.setFields(fields);
    return jsonSchema;
  }

  /**
   * Processes a Spark schema with table properties to add metadata for vector and blob columns.
   *
   * @param sparkSchema the original Spark StructType
   * @param properties table properties that may contain vector column metadata or blob encoding
   * @return StructType with metadata added for vector and blob columns
   */
  public static StructType processSchemaWithProperties(
      StructType sparkSchema, Map<String, String> properties) {
    StructType schemaWithVectors = addVectorMetadata(sparkSchema, properties);
    return addBlobMetadata(schemaWithVectors, properties);
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
   * Converts a Spark StructField to JsonArrowField.
   *
   * @param sparkField the Spark StructField to convert
   * @return JsonArrowField representation
   */
  private static JsonArrowField toJsonArrowField(StructField sparkField) {
    JsonArrowField field = new JsonArrowField();
    field.setName(sparkField.name());
    field.setNullable(sparkField.nullable());
    field.setType(
        toJsonArrowDataType(sparkField.dataType(), sparkField.name(), sparkField.metadata()));
    return field;
  }

  /**
   * Converts a Spark DataType to JsonArrowDataType.
   *
   * @param sparkType the Spark DataType to convert
   * @param fieldName the name of the field (used for special cases like ROW_ID)
   * @return JsonArrowDataType representation
   */
  private static JsonArrowDataType toJsonArrowDataType(DataType sparkType, String fieldName) {
    return toJsonArrowDataType(sparkType, fieldName, null);
  }

  /**
   * Converts a Spark DataType to JsonArrowDataType.
   *
   * @param sparkType the Spark DataType to convert
   * @param fieldName the name of the field (used for special cases like ROW_ID)
   * @param metadata the field metadata (may contain vector column information)
   * @return JsonArrowDataType representation
   */
  private static JsonArrowDataType toJsonArrowDataType(
      DataType sparkType, String fieldName, Metadata metadata) {
    JsonArrowDataType dataType = new JsonArrowDataType();

    if (sparkType instanceof BooleanType) {
      dataType.setType("bool");
    } else if (sparkType instanceof ByteType) {
      dataType.setType("int8");
    } else if (sparkType instanceof ShortType) {
      dataType.setType("int16");
    } else if (sparkType instanceof IntegerType) {
      dataType.setType("int32");
    } else if (sparkType instanceof LongType) {
      dataType.setType("int64");
      // Special handling for ROW_ID field (unsigned 64-bit)
      // Note: JsonArrowDataType doesn't have signed/unsigned distinction in this simple mapping
    } else if (sparkType instanceof FloatType) {
      dataType.setType("float32");
    } else if (sparkType instanceof DoubleType) {
      dataType.setType("float64");
    } else if (sparkType instanceof StringType) {
      dataType.setType("string");
    } else if (sparkType instanceof BinaryType) {
      dataType.setType("binary");
    } else if (sparkType instanceof DateType) {
      dataType.setType("date");
    } else if (sparkType instanceof TimestampType) {
      dataType.setType("timestamp");
    } else if (sparkType instanceof TimestampNTZType) {
      dataType.setType("timestamp");
    } else if (sparkType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkType;
      dataType.setType("decimal");
      // Note: precision and scale would need additional fields if supported
    } else if (sparkType instanceof NullType) {
      dataType.setType("null");
    } else if (sparkType instanceof YearMonthIntervalType) {
      dataType.setType("interval");
    } else if (sparkType instanceof DayTimeIntervalType) {
      dataType.setType("duration");
    } else if (sparkType instanceof CalendarIntervalType) {
      dataType.setType("interval");
    } else if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;

      // Check if this should be a FixedSizeList based on metadata
      boolean isFixedSizeList = false;
      Long fixedSize = null;
      if (metadata != null && metadata.contains(ARROW_FIXED_SIZE_LIST_SIZE_KEY)) {
        try {
          fixedSize = metadata.getLong(ARROW_FIXED_SIZE_LIST_SIZE_KEY);
          isFixedSizeList = true;
        } catch (Exception e) {
          // Fall back to regular list if metadata is invalid
        }
      }

      if (isFixedSizeList && fixedSize != null) {
        dataType.setType("fixedsizelist");
        dataType.setLength(fixedSize);
      } else {
        dataType.setType("list");
      }

      // Create item field (Arrow convention for list child field)
      JsonArrowField itemField = new JsonArrowField();
      itemField.setName("item");
      itemField.setNullable(arrayType.containsNull());
      itemField.setType(toJsonArrowDataType(arrayType.elementType(), "item", null));
      List<JsonArrowField> fields = new ArrayList<>();
      fields.add(itemField);
      dataType.setFields(fields);

    } else if (sparkType instanceof StructType) {
      StructType structType = (StructType) sparkType;
      dataType.setType("struct");
      List<JsonArrowField> fields = new ArrayList<>();
      for (StructField field : structType.fields()) {
        fields.add(toJsonArrowField(field));
      }
      dataType.setFields(fields);
    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      dataType.setType("map");
      List<JsonArrowField> fields = new ArrayList<>();
      // Create struct field containing key and value
      JsonArrowField mapStructField = new JsonArrowField();
      mapStructField.setName("entries");
      mapStructField.setNullable(false);
      JsonArrowDataType mapStructType = new JsonArrowDataType();
      mapStructType.setType("struct");
      List<JsonArrowField> structFields = new ArrayList<>();
      // Key field
      JsonArrowField keyField = new JsonArrowField();
      keyField.setName("key");
      keyField.setNullable(false);
      keyField.setType(toJsonArrowDataType(mapType.keyType(), "key"));
      structFields.add(keyField);
      // Value field
      JsonArrowField valueField = new JsonArrowField();
      valueField.setName("value");
      valueField.setNullable(mapType.valueContainsNull());
      valueField.setType(toJsonArrowDataType(mapType.valueType(), "value"));
      structFields.add(valueField);
      mapStructType.setFields(structFields);
      mapStructField.setType(mapStructType);
      fields.add(mapStructField);
      dataType.setFields(fields);
    } else if (sparkType instanceof UserDefinedType) {
      UserDefinedType<?> udt = (UserDefinedType<?>) sparkType;
      return toJsonArrowDataType(udt.sqlType(), fieldName);
    } else {
      throw new IllegalArgumentException("Unsupported Spark data type: " + sparkType);
    }

    return dataType;
  }
}
