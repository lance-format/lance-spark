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

import org.lance.namespace.model.JsonArrowField;
import org.lance.namespace.model.JsonArrowSchema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaConverterTest {

  // --- toJsonArrowSchema ---

  @Test
  public void testPrimitiveTypes() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("bool_col", DataTypes.BooleanType, true),
              DataTypes.createStructField("byte_col", DataTypes.ByteType, false),
              DataTypes.createStructField("short_col", DataTypes.ShortType, true),
              DataTypes.createStructField("int_col", DataTypes.IntegerType, true),
              DataTypes.createStructField("long_col", DataTypes.LongType, true),
              DataTypes.createStructField("float_col", DataTypes.FloatType, true),
              DataTypes.createStructField("double_col", DataTypes.DoubleType, true),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
              DataTypes.createStructField("binary_col", DataTypes.BinaryType, true),
              DataTypes.createStructField("date_col", DataTypes.DateType, true),
              DataTypes.createStructField("timestamp_col", DataTypes.TimestampType, true),
            });
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    List<JsonArrowField> fields = result.getFields();
    assertEquals(11, fields.size());
    assertEquals("bool", fields.get(0).getType().getType());
    assertEquals("int8", fields.get(1).getType().getType());
    assertEquals("int16", fields.get(2).getType().getType());
    assertEquals("int32", fields.get(3).getType().getType());
    assertEquals("int64", fields.get(4).getType().getType());
    assertEquals("float32", fields.get(5).getType().getType());
    assertEquals("float64", fields.get(6).getType().getType());
    assertEquals("string", fields.get(7).getType().getType());
    assertEquals("binary", fields.get(8).getType().getType());
    assertEquals("date", fields.get(9).getType().getType());
    assertEquals("timestamp", fields.get(10).getType().getType());
  }

  @Test
  public void testNullability() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("nullable_col", DataTypes.IntegerType, true),
              DataTypes.createStructField("non_null_col", DataTypes.IntegerType, false),
            });
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    assertTrue(result.getFields().get(0).getNullable());
    assertFalse(result.getFields().get(1).getNullable());
  }

  @Test
  public void testArrayType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "list_col", DataTypes.createArrayType(DataTypes.IntegerType, true), true),
            });
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    JsonArrowField field = result.getFields().get(0);
    assertEquals("list", field.getType().getType());
    assertEquals(1, field.getType().getFields().size());
    assertEquals("item", field.getType().getFields().get(0).getName());
    assertEquals("int32", field.getType().getFields().get(0).getType().getType());
  }

  @Test
  public void testFixedSizeListWithMetadata() {
    Metadata vectorMetadata =
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", 128).build();
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField(
                  "embeddings",
                  DataTypes.createArrayType(DataTypes.FloatType, false),
                  false,
                  vectorMetadata),
            });
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    JsonArrowField field = result.getFields().get(0);
    assertEquals("fixedsizelist", field.getType().getType());
    assertEquals(128L, field.getType().getLength());
  }

  @Test
  public void testStructType() {
    StructType innerStruct =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("age", DataTypes.IntegerType, true),
            });
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("person", innerStruct, true),
            });
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    JsonArrowField field = result.getFields().get(0);
    assertEquals("struct", field.getType().getType());
    assertEquals(2, field.getType().getFields().size());
    assertEquals("name", field.getType().getFields().get(0).getName());
    assertEquals("string", field.getType().getFields().get(0).getType().getType());
  }

  @Test
  public void testMapType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "map_col",
                  DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
                  true),
            });
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    JsonArrowField field = result.getFields().get(0);
    assertEquals("map", field.getType().getType());
    // Map has an "entries" struct child with "key" and "value"
    assertEquals("entries", field.getType().getFields().get(0).getName());
    assertEquals("struct", field.getType().getFields().get(0).getType().getType());
    List<JsonArrowField> entryFields = field.getType().getFields().get(0).getType().getFields();
    assertEquals("key", entryFields.get(0).getName());
    assertEquals("string", entryFields.get(0).getType().getType());
    assertEquals("value", entryFields.get(1).getName());
    assertEquals("int32", entryFields.get(1).getType().getType());
  }

  @Test
  public void testDecimalType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("decimal_col", DataTypes.createDecimalType(10, 2), true),
            });
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    assertEquals("decimal", result.getFields().get(0).getType().getType());
  }

  @Test
  public void testEmptySchema() {
    StructType schema = new StructType();
    JsonArrowSchema result = SchemaConverter.toJsonArrowSchema(schema);
    assertTrue(result.getFields().isEmpty());
  }

  // --- processSchemaWithProperties ---

  @Test
  public void testProcessSchemaAddsVectorMetadata() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.FloatType, false), false),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("embeddings.arrow.fixed-size-list.size", "128");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField embeddingsField = result.apply("embeddings");
    assertTrue(embeddingsField.metadata().contains("arrow.fixed-size-list.size"));
    assertEquals(128L, embeddingsField.metadata().getLong("arrow.fixed-size-list.size"));
  }

  @Test
  public void testProcessSchemaAddsBlobMetadata() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("data.lance.encoding", "blob");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField dataField = result.apply("data");
    assertTrue(dataField.metadata().contains(BlobUtils.LANCE_ENCODING_BLOB_KEY));
    assertEquals(
        BlobUtils.LANCE_ENCODING_BLOB_VALUE,
        dataField.metadata().getString(BlobUtils.LANCE_ENCODING_BLOB_KEY));
  }

  @Test
  public void testProcessSchemaAddsLargeVarCharMetadata() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("text", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("text.arrow.large_var_char", "true");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField textField = result.apply("text");
    assertTrue(textField.metadata().contains(LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_KEY));
  }

  @Test
  public void testProcessSchemaWithNullProperties() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
            });
    StructType result = SchemaConverter.processSchemaWithProperties(schema, null);
    assertEquals(schema, result);
  }

  @Test
  public void testProcessSchemaWithEmptyProperties() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
            });
    StructType result = SchemaConverter.processSchemaWithProperties(schema, Collections.emptyMap());
    assertEquals(schema, result);
  }

  @Test
  public void testVectorMetadataRejectsNonArrayType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("id.arrow.fixed-size-list.size", "128");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testVectorMetadataRejectsNonFloatElementType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "ids", DataTypes.createArrayType(DataTypes.IntegerType, false), false),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("ids.arrow.fixed-size-list.size", "128");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testBlobMetadataRejectsNonBinaryType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("text", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("text.lance.encoding", "blob");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testLargeVarCharRejectsNonStringType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("num", DataTypes.IntegerType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("num.arrow.large_var_char", "true");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }
}
