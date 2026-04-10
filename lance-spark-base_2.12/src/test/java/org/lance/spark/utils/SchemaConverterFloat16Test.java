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

import org.apache.spark.sql.types.DataTypes;
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
}
