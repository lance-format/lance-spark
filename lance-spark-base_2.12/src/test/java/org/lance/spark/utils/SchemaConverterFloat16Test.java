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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for SchemaConverter float16 metadata processing. */
public class SchemaConverterFloat16Test {

  @Test
  public void testProcessSchemaWithFloat16Properties() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.FloatType, false), false)
            });

    Map<String, String> properties = new HashMap<>();
    properties.put("embeddings.arrow.fixed-size-list.size", "32");
    properties.put("embeddings.arrow.float16", "true");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);

    StructField embField = result.apply("embeddings");
    assertTrue(embField.metadata().contains("arrow.fixed-size-list.size"));
    assertEquals(32L, embField.metadata().getLong("arrow.fixed-size-list.size"));
    assertTrue(embField.metadata().contains("arrow.float16"));
    assertEquals("true", embField.metadata().getString("arrow.float16"));
  }

  @Test
  public void testFloat16RequiresFixedSizeList() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.FloatType, false), false)
            });

    Map<String, String> properties = new HashMap<>();
    // Float16 without fixed-size-list property
    properties.put("embeddings.arrow.float16", "true");

    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties),
        "Should reject float16 without fixed-size-list metadata");
  }

  @Test
  public void testFloat16RequiresFloatElementType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.DoubleType, false), false)
            });

    Map<String, String> properties = new HashMap<>();
    properties.put("embeddings.arrow.fixed-size-list.size", "8");
    properties.put("embeddings.arrow.float16", "true");

    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties),
        "Should reject float16 on DOUBLE array");
  }

  @Test
  public void testFloat16RequiresArrayType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("embeddings", DataTypes.StringType, false)
            });

    Map<String, String> properties = new HashMap<>();
    properties.put("embeddings.arrow.float16", "true");

    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties),
        "Should reject float16 on non-array type");
  }

  @Test
  public void testFloat16FalseValueIgnored() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.FloatType, false), false)
            });

    Map<String, String> properties = new HashMap<>();
    properties.put("embeddings.arrow.fixed-size-list.size", "8");
    properties.put("embeddings.arrow.float16", "false");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField embField = result.apply("embeddings");

    // Should have fixed-size-list metadata but NOT float16
    assertTrue(embField.metadata().contains("arrow.fixed-size-list.size"));
    assertFalse(
        embField.metadata().contains("arrow.float16"),
        "Float16 metadata should not be added when value is 'false'");
  }

  @Test
  public void testToJsonArrowSchemaFloat16() {
    // Build a schema with float16 metadata already applied
    // (as processSchemaWithProperties would produce)
    Metadata float16Meta =
        new MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 128)
            .putString("arrow.float16", "true")
            .build();

    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              new StructField(
                  "embeddings",
                  DataTypes.createArrayType(DataTypes.FloatType, false),
                  false,
                  float16Meta)
            });

    JsonArrowSchema jsonSchema = SchemaConverter.toJsonArrowSchema(schema);

    // Find the embeddings field
    JsonArrowField embField = null;
    for (JsonArrowField f : jsonSchema.getFields()) {
      if ("embeddings".equals(f.getName())) {
        embField = f;
        break;
      }
    }
    assertNotNull(embField, "embeddings field should exist in schema");

    // Verify it's a fixedsizelist with length 128
    JsonArrowDataType embType = embField.getType();
    assertEquals("fixedsizelist", embType.getType());
    assertEquals(128L, embType.getLength());

    // Verify the child item field has float16 type
    assertNotNull(embType.getFields(), "FixedSizeList should have child fields");
    assertEquals(1, embType.getFields().size());
    JsonArrowField itemField = embType.getFields().get(0);
    assertEquals("item", itemField.getName());
    assertEquals(
        "float16",
        itemField.getType().getType(),
        "Child element type should be float16, not float32");
  }

  @Test
  public void testToJsonArrowSchemaFloat32VectorNotAffected() {
    // A regular float32 vector should NOT produce float16 type
    Metadata vectorMeta = new MetadataBuilder().putLong("arrow.fixed-size-list.size", 64).build();

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField(
                  "embeddings",
                  DataTypes.createArrayType(DataTypes.FloatType, false),
                  false,
                  vectorMeta)
            });

    JsonArrowSchema jsonSchema = SchemaConverter.toJsonArrowSchema(schema);
    JsonArrowField embField = jsonSchema.getFields().get(0);
    JsonArrowDataType embType = embField.getType();

    assertEquals("fixedsizelist", embType.getType());
    JsonArrowField itemField = embType.getFields().get(0);
    assertEquals(
        "float32",
        itemField.getType().getType(),
        "Regular vector should remain float32, not float16");
  }
}
