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

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlobUtilsTest {

  @Test
  public void testBlobV2FieldWithArrowExtensionName() {
    assertTrue(BlobUtils.isBlobV2SparkField(blobV2Field()));
  }

  @Test
  public void testBlobV2FieldNullSafety() {
    assertFalse(BlobUtils.isBlobV2SparkField(null));
  }

  @Test
  public void testV1Field() {
    assertTrue(BlobUtils.isBlobSparkField(blobV1Field()));
  }

  @Test
  public void testHasBlobV2FieldsInSchema() {
    StructType schema =
        new StructType(
            new StructField[] {
              field("id", DataTypes.IntegerType), blobV2Field(),
            });
    assertTrue(BlobUtils.hasBlobV2Fields(schema));
  }

  @Test
  public void testBlobV2ColumnNamesExcludesNonBlobAndV1() {
    Metadata v1 =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    StructType schema =
        new StructType(
            new StructField[] {
              field("id", DataTypes.IntegerType),
              new StructField("attachment", DataTypes.BinaryType, true, v1),
              blobV2Field(),
            });
    assertEquals(Collections.singleton("payload"), BlobUtils.blobV2ColumnNames(schema));
  }

  @Test
  public void testIsBlobReadColumnCoversV1AndV2() {
    Metadata v1 =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    assertTrue(BlobUtils.isBlobReadColumn(new StructField("v1", DataTypes.BinaryType, true, v1)));
    assertTrue(BlobUtils.isBlobReadColumn(blobV2Field()));
    assertFalse(BlobUtils.isBlobReadColumn(field("id", DataTypes.IntegerType)));
  }

  @Test
  public void testNullBlobV2DescriptorSentinel() {
    // The sentinel is ALL five fields zero/empty; any single non-zero field means a real blob,
    // so each field must participate in the detection (size == 0 alone is not enough).
    Object[] sentinel = {(short) 0, 0L, 0L, 0L, UTF8String.fromString("")};
    assertTrue(BlobUtils.isNullBlobV2Descriptor(new GenericInternalRow(sentinel)));
    Object[][] realBlobs = {
      {(short) 1, 0L, 0L, 0L, UTF8String.fromString("")},
      {(short) 0, 8L, 0L, 0L, UTF8String.fromString("")},
      {(short) 0, 0L, 16L, 0L, UTF8String.fromString("")},
      {(short) 0, 0L, 0L, 7L, UTF8String.fromString("")},
      {(short) 0, 0L, 0L, 0L, UTF8String.fromString("file:///b.blob")},
    };
    for (Object[] fields : realBlobs) {
      assertFalse(
          BlobUtils.isNullBlobV2Descriptor(new GenericInternalRow(fields)),
          "must not classify a real descriptor as the null sentinel: "
              + java.util.Arrays.toString(fields));
    }
  }

  @Test
  public void testDescriptorStructShape() {
    StructType s = BlobUtils.BLOB_DESCRIPTOR_STRUCT;
    assertEquals(5, s.fields().length);
    assertEquals(DataTypes.ShortType, s.apply("kind").dataType());
    assertEquals(DataTypes.LongType, s.apply("position").dataType());
    assertEquals(DataTypes.LongType, s.apply("size").dataType());
    assertEquals(DataTypes.LongType, s.apply("blob_id").dataType());
    assertEquals(DataTypes.StringType, s.apply("blob_uri").dataType());
  }

  @Test
  public void testBlobV2DescriptorSchemaRewrite() {
    StructType schema =
        new StructType(
            new StructField[] {
              field("id", DataTypes.IntegerType), blobV2Field(),
            });
    StructType rewritten = BlobUtils.applyBlobV2DescriptorSchema(schema);
    assertEquals(DataTypes.IntegerType, rewritten.apply("id").dataType());
    assertEquals(BlobUtils.BLOB_DESCRIPTOR_STRUCT, rewritten.apply("payload").dataType());
  }

  @Test
  public void testV1FieldsPreservedInRewrite() {
    StructType schema =
        new StructType(
            new StructField[] {
              field("id", DataTypes.IntegerType), blobV1Field(),
            });
    StructType rewritten = BlobUtils.applyBlobV2DescriptorSchema(schema);
    assertEquals(DataTypes.BinaryType, rewritten.apply("payload").dataType());
  }

  @Test
  public void fileFormatSupportsBlobV2AcceptsTwoTwoAndNewer() {
    assertTrue(BlobUtils.fileFormatSupportsBlobV2("2.2"));
    assertTrue(BlobUtils.fileFormatSupportsBlobV2("2.10"));
    assertTrue(BlobUtils.fileFormatSupportsBlobV2("3.0"));
    assertTrue(BlobUtils.fileFormatSupportsBlobV2(" 2.2 "));
  }

  @Test
  public void fileFormatSupportsBlobV2RejectsOlderAndNull() {
    assertFalse(BlobUtils.fileFormatSupportsBlobV2(null));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("2.0"));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("2.1"));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("2"));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("2."));
  }

  @Test
  public void fileFormatSupportsBlobV2RejectsNamedAndMalformedVersions() {
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("stable"));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2(""));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("."));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("2.x"));
  }

  private static StructField field(String name, org.apache.spark.sql.types.DataType dt) {
    return new StructField(name, dt, true, Metadata.empty());
  }

  private static StructField blobV2Field() {
    Metadata md =
        new MetadataBuilder()
            .putString(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2)
            .build();
    return new StructField("payload", DataTypes.BinaryType, true, md);
  }

  private static StructField blobV1Field() {
    Metadata md =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    return new StructField("payload", DataTypes.BinaryType, true, md);
  }
}
