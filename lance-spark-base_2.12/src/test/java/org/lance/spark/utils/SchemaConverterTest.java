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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaConverterTest {

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
