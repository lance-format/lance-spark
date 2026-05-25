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

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
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
    assertValidationFailure(
        "must have BINARY type",
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

  @Test
  public void testCompressionMetadataIsAdded() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("payload", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("payload.lance.compression", "zstd");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("payload");
    assertTrue(field.metadata().contains(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals("zstd", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testCompressionLevelMetadataIsAdded() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("payload", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("payload.lance.compression-level", "3");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("payload");
    assertTrue(field.metadata().contains(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION_LEVEL));
    assertEquals(
        "3", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION_LEVEL));
  }

  @Test
  public void testStructuralEncodingMetadataIsAdded() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("ts", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("ts.lance.structural-encoding", "miniblock");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("ts");
    assertTrue(field.metadata().contains(LanceEncodingUtils.LANCE_ENCODING_STRUCTURAL_ENCODING));
    assertEquals(
        "miniblock",
        field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_STRUCTURAL_ENCODING));
  }

  @Test
  public void testRleThresholdMetadataIsAdded() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("ts", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("ts.lance.rle-threshold", "0.5");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("ts");
    assertTrue(field.metadata().contains(LanceEncodingUtils.LANCE_ENCODING_RLE_THRESHOLD));
    assertEquals(
        "0.5", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_RLE_THRESHOLD));
  }

  @Test
  public void testBssMetadataIsAdded() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("ts", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("ts.lance.bss", "auto");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("ts");
    assertTrue(field.metadata().contains(LanceEncodingUtils.LANCE_ENCODING_BSS));
    assertEquals("auto", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_BSS));
  }

  @Test
  public void testAllFiveCompressionKeysOnOneField() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.compression", "lz4");
    properties.put("col.lance.compression-level", "1");
    properties.put("col.lance.structural-encoding", "fullzip");
    properties.put("col.lance.rle-threshold", "1.0");
    properties.put("col.lance.bss", "off");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("col");
    assertEquals("lz4", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals(
        "1", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION_LEVEL));
    assertEquals(
        "fullzip",
        field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_STRUCTURAL_ENCODING));
    assertEquals(
        "1.0", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_RLE_THRESHOLD));
    assertEquals("off", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_BSS));
  }

  @Test
  public void testMultiColumnCompressionConfig() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("payload", DataTypes.StringType, true),
              DataTypes.createStructField("ts", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("payload.lance.compression", "zstd");
    properties.put("ts.lance.compression", "none");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);

    assertFalse(
        result.apply("id").metadata().contains(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals(
        "zstd",
        result
            .apply("payload")
            .metadata()
            .getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
    assertEquals(
        "none",
        result.apply("ts").metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testUnknownLanceKeysAreIgnored() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    // dict-divisor is a deferred (unsupported) key — silently ignored, no exception thrown.
    // Unrecognised key suffixes are part of the connector's silent-ignore policy so that
    // future lance-core keys do not break existing Spark jobs.
    properties.put("col.lance.dict-divisor", "4");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    // no metadata set, no exception
    assertEquals(schema.apply("col").metadata(), result.apply("col").metadata());
  }

  @Test
  public void testCompressionPropertyForNonExistentColumnIsIgnored() {
    // Property whose <column> segment does not match any schema field is silently ignored.
    // This is intentional and consistent with the other addX methods in SchemaConverter —
    // no Spark-side error is thrown for unmatched column names.
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("nonexistent.lance.compression", "zstd");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    // schema unchanged, no exception
    assertEquals(schema, result);
  }

  @Test
  public void testTypeIncompatibleCompressionIsPassedThrough() {
    // Type-incompatible combinations (e.g. fsst on a numeric column) are not rejected
    // by the connector. Semantic validation is left to the Lance Rust encoder.
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("ts", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("ts.lance.compression", "fsst");

    // No exception — metadata is written and Rust decides what to do with it
    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    assertEquals(
        "fsst",
        result.apply("ts").metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testCompressionMetadataCoexistsWithBlobMetadata() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("data.lance.encoding", "blob");
    properties.put("data.lance.compression", "lz4");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("data");
    assertEquals(
        BlobUtils.LANCE_ENCODING_BLOB_VALUE,
        field.metadata().getString(BlobUtils.LANCE_ENCODING_BLOB_KEY));
    assertEquals("lz4", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testCompressionMetadataCoexistsWithLargeVarCharMetadata() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("content", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("content.arrow.large_var_char", "true");
    properties.put("content.lance.compression", "zstd");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField field = result.apply("content");
    assertEquals(
        LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_VALUE,
        field.metadata().getString(LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_KEY));
    assertEquals("zstd", field.metadata().getString(LanceEncodingUtils.LANCE_ENCODING_COMPRESSION));
  }

  @Test
  public void testInvalidCompressionSchemeThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.compression", "gzip");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testInvalidStructuralEncodingThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.structural-encoding", "rle");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testInvalidBssModeThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.bss", "yes");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testNonFloatRleThresholdThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.rle-threshold", "not-a-float");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testOutOfRangeRleThresholdThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    // 0.0 is excluded from (0.0, 1.0]
    properties.put("col.lance.rle-threshold", "0.0");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testRleThresholdAboveOneThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.LongType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.rle-threshold", "1.5");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testNonIntegerCompressionLevelThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.compression-level", "high");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testProcessSchemaAddsFixedSizeBinaryMetadata() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("hash", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("hash.arrow.fixed-size-binary.byte-width", "16");

    StructType result = SchemaConverter.processSchemaWithProperties(schema, properties);
    StructField hashField = result.apply("hash");
    // Spark type must remain BinaryType — Spark has no FixedSizeBinary — only the metadata changes.
    assertEquals(DataTypes.BinaryType, hashField.dataType());
    assertTrue(hashField.nullable());
    assertTrue(
        hashField.metadata().contains(FixedSizeBinaryUtils.ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY));
    assertEquals(
        16L,
        hashField.metadata().getLong(FixedSizeBinaryUtils.ARROW_FIXED_SIZE_BINARY_BYTE_WIDTH_KEY));
  }

  @Test
  public void testFixedSizeBinaryMetadataRejectsNonBinaryType() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("name.arrow.fixed-size-binary.byte-width", "16");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testFixedSizeBinaryMetadataRejectsNonNumericWidth() {
    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("hash", DataTypes.BinaryType, true)});
    Map<String, String> properties = new HashMap<>();
    properties.put("hash.arrow.fixed-size-binary.byte-width", "sixteen");
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SchemaConverter.processSchemaWithProperties(schema, properties));
    // Error must include the column name so users can locate the bad property.
    assertTrue(
        ex.getMessage().contains("hash"), "error must name the column; got: " + ex.getMessage());
  }

  @Test
  public void testFixedSizeBinaryMetadataRejectsZeroAndNegativeWidth() {
    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("hash", DataTypes.BinaryType, true)});
    for (String bad : new String[] {"0", "-1", "-16"}) {
      Map<String, String> properties = new HashMap<>();
      properties.put("hash.arrow.fixed-size-binary.byte-width", bad);
      assertThrows(
          IllegalArgumentException.class,
          () -> SchemaConverter.processSchemaWithProperties(schema, properties),
          "byte-width=" + bad + " must be rejected");
    }
  }

  @Test
  public void testFixedSizeBinaryMetadataRejectsOverflowWidth() {
    // Arrow's FixedSizeBinary stores byte width as int; reject values that would overflow int.
    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("hash", DataTypes.BinaryType, true)});
    Map<String, String> properties = new HashMap<>();
    properties.put(
        "hash.arrow.fixed-size-binary.byte-width", String.valueOf((long) Integer.MAX_VALUE + 1L));
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testNegativeCompressionLevelThrows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col", DataTypes.StringType, true),
            });
    Map<String, String> properties = new HashMap<>();
    properties.put("col.lance.compression-level", "-1");
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaConverter.processSchemaWithProperties(schema, properties));
  }

  @Test
  public void testBlobV2ArrowSchemaUsesWriteStruct() {
    StructType sparkSchema = blobSchemaWithVersion("2.2");

    assertBlobV2Field(sparkSchema.apply("data"));

    Schema arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", true);
    Field contentField = arrowSchema.findField("data");

    assertNotNull(contentField);
    assertInstanceOf(ArrowType.Struct.class, contentField.getType());
    assertEquals(
        BlobUtils.ARROW_EXTENSION_BLOB_V2,
        contentField.getMetadata().get(BlobUtils.ARROW_EXTENSION_NAME_KEY));
  }

  @Test
  public void testBlobV1WithUnsupportedVersion() {
    // v2 only supported on 2.2 or higher
    assertBlobV1Field(blobSchemaWithVersion("2.1").apply("data"));
  }

  @Test
  public void testBlobV1WhenVersionNull() {
    assertBlobV1Field(blobSchemaWithVersion(null).apply("data"));
  }

  @Test
  public void testBlobEncodingRequiresColumnProperty() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = ImmutableMap.of("file_format_version", "2.2");
    StructField field =
        SchemaConverter.processSchemaWithProperties(schema, properties, "2.2").apply("data");

    assertFalse(field.metadata().contains(BlobUtils.LANCE_ENCODING_BLOB_KEY));
    assertFalse(field.metadata().contains(BlobUtils.ARROW_EXTENSION_NAME_KEY));
  }

  @Test
  public void testBlobEncodingRejectsUnknownFileFormatVersion() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = ImmutableMap.of("data.lance.encoding", "blob");

    assertValidationFailure(
        "Blob columns require a numeric file_format_version like '2.2'. Got: 'stable'.",
        () -> SchemaConverter.processSchemaWithProperties(schema, properties, "stable"));
  }

  @Test
  public void testBlobEncodingRejectsMalformedFileFormatVersion() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = ImmutableMap.of("data.lance.encoding", "blob");

    assertValidationFailure(
        "numeric file_format_version",
        () -> SchemaConverter.processSchemaWithProperties(schema, properties, "2.x"));
  }

  private static StructType blobSchemaWithVersion(String fileFormatVersion) {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = ImmutableMap.of("data.lance.encoding", "blob");

    return SchemaConverter.processSchemaWithProperties(schema, properties, fileFormatVersion);
  }

  private static void assertBlobV1Field(StructField field) {
    assertEquals(DataTypes.BinaryType, field.dataType());
    assertTrue(field.metadata().contains(BlobUtils.LANCE_ENCODING_BLOB_KEY));
    assertEquals(
        BlobUtils.LANCE_ENCODING_BLOB_VALUE,
        field.metadata().getString(BlobUtils.LANCE_ENCODING_BLOB_KEY));
    assertFalse(field.metadata().contains(BlobUtils.ARROW_EXTENSION_NAME_KEY));
  }

  private static void assertBlobV2Field(StructField field) {
    assertEquals(DataTypes.BinaryType, field.dataType());
    assertEquals(
        BlobUtils.ARROW_EXTENSION_BLOB_V2,
        field.metadata().getString(BlobUtils.ARROW_EXTENSION_NAME_KEY));
    assertFalse(field.metadata().contains(BlobUtils.LANCE_ENCODING_BLOB_KEY));
  }

  private static void assertValidationFailure(String expectedFragment, Runnable action) {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, action::run);
    assertTrue(
        e.getMessage().contains(expectedFragment),
        () -> "expected message to contain '" + expectedFragment + "': " + e.getMessage());
  }
}
