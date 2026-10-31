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

import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.fragment.DataFile;
import org.lance.operation.Merge;
import org.lance.operation.Overwrite;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  public void testBlobV1ColumnNamesIncludesNestedFields() {
    Metadata v1 =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    StructType inner = new StructType().add("data", DataTypes.BinaryType, true, v1);
    Metadata blobElements =
        new MetadataBuilder()
            .putString(LanceArrowUtils.LANCE_ELEMENT_METADATA_KEY(), v1.json())
            .build();
    Metadata blobValues =
        new MetadataBuilder()
            .putString(LanceArrowUtils.LANCE_MAP_VALUE_METADATA_KEY(), v1.json())
            .build();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("payload", inner)
            .add("items", DataTypes.createArrayType(inner))
            .add("byName", DataTypes.createMapType(DataTypes.StringType, inner))
            .add("chunks", DataTypes.createArrayType(DataTypes.BinaryType), true, blobElements)
            .add(
                "blobByName",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType),
                true,
                blobValues);
    assertEquals(
        new HashSet<>(
            Arrays.asList(
                "payload.data", "items.data", "byName.value.data", "chunks", "blobByName.value")),
        BlobUtils.blobV1ColumnNames(schema));
    assertFalse(
        BlobUtils.hasBlobV1Fields(
            new StructType().add("payload", new StructType().add("data", DataTypes.BinaryType))));
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
    // Lance's release selectors resolve to 2.2 or newer at write time.
    assertTrue(BlobUtils.fileFormatSupportsBlobV2("stable"));
    assertTrue(BlobUtils.fileFormatSupportsBlobV2("next"));
    assertTrue(BlobUtils.fileFormatSupportsBlobV2("STABLE"));
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
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("legacy"));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2(""));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("."));
    assertFalse(BlobUtils.fileFormatSupportsBlobV2("2.x"));
  }

  @Test
  public void knownToRejectBlobV1OnlyAnswersForVersionsLanceRecognizes() {
    assertTrue(BlobUtils.knownToRejectBlobV1("2.2"));
    assertTrue(BlobUtils.knownToRejectBlobV1("2.3"));
    assertTrue(BlobUtils.knownToRejectBlobV1("stable"));
    assertTrue(BlobUtils.knownToRejectBlobV1("NEXT"));
    assertFalse(BlobUtils.knownToRejectBlobV1("2.1"));
    assertFalse(BlobUtils.knownToRejectBlobV1("legacy"));
    assertFalse(BlobUtils.knownToRejectBlobV1(null));
    // Lance parses versions against a closed set, so these do not name a newer format at all.
    assertFalse(BlobUtils.knownToRejectBlobV1("2.4"));
    assertFalse(BlobUtils.knownToRejectBlobV1("3.0"));
  }

  @Test
  public void checkBlobV1WriteVersionRejectsOnlyVersionsThatCannotStoreV1() {
    StructType nestedV1 =
        new StructType().add("payload", new StructType(new StructField[] {blobV1Field("data")}));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> BlobUtils.checkBlobV1WriteVersion(nestedV1, "stable"));
    assertTrue(e.getMessage().contains("[payload.data]"), e.getMessage());
    BlobUtils.checkBlobV1WriteVersion(nestedV1, "2.1");
    BlobUtils.checkBlobV1WriteVersion(nestedV1, null);
    BlobUtils.checkBlobV1WriteVersion(new StructType(new StructField[] {blobV2Field()}), "2.2");
  }

  @Test
  public void blobV1FieldsInBlobV2FilesOnlyCountsFilesAtTwoTwoAndNewer() {
    Map<Integer, String> blobV1Fields = new HashMap<>();
    blobV1Fields.put(1, "data");
    blobV1Fields.put(3, "payload.data");
    assertEquals(
        Collections.emptySet(),
        BlobUtils.blobV1FieldsInBlobV2Files(
            blobV1Fields, Arrays.asList(dataFile(2, 1, 0, 1, 3), dataFile(2, 2, 0, 2))));
    assertEquals(
        new HashSet<>(Arrays.asList("data", "payload.data")),
        BlobUtils.blobV1FieldsInBlobV2Files(
            blobV1Fields, Arrays.asList(dataFile(2, 2, 0, 1), dataFile(3, 0, 3))));
  }

  /** The dataset is not read: an overwrite keeps none of the table's files. */
  @Test
  public void checkBlobV1CommitChecksAnOverwriteOnItsOwnFiles() {
    StructType nestedV1 =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("payload", new StructType(new StructField[] {blobV1Field("data")}));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> BlobUtils.checkBlobV1Commit(null, overwrite(nestedV1, dataFile(2, 2, 0, 1, 2))));
    assertTrue(e.getMessage().contains("[payload.data]"), e.getMessage());
    BlobUtils.checkBlobV1Commit(null, overwrite(nestedV1, dataFile(2, 1, 0, 1, 2)));
    BlobUtils.checkBlobV1Commit(
        null, overwrite(new StructType(new StructField[] {blobV2Field()}), dataFile(2, 2, 0)));
  }

  private static Overwrite overwrite(StructType schema, DataFile file) {
    return Overwrite.builder()
        .fragments(fragments(file))
        .schema(LanceArrowUtils.toArrowSchema(schema, "UTC", true))
        .build();
  }

  /** A column that a merge adds gets its id when the merge commits, so it is matched by path. */
  @Test
  public void checkBlobV1CommitRejectsAMergeAddingV1FieldsAtTwoTwo(@TempDir Path dir) {
    StructType table = new StructType().add("id", DataTypes.IntegerType);
    StructType merged = table.add(blobV1Field("data"));
    WriteParams params = new WriteParams.Builder().withDataStorageVersion("2.2").build();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset dataset =
            Dataset.create(
                allocator,
                dir.resolve("merge.lance").toString(),
                LanceArrowUtils.toArrowSchema(table, "UTC", true),
                params)) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> BlobUtils.checkBlobV1Commit(dataset, merge(merged, dataFile(2, 2, 0, 1))));
      assertTrue(e.getMessage().contains("[data]"), e.getMessage());
      BlobUtils.checkBlobV1Commit(dataset, merge(merged, dataFile(2, 1, 0, 1)));
    }
  }

  private static Merge merge(StructType schema, DataFile file) {
    return Merge.builder()
        .fragments(fragments(file))
        .schema(LanceArrowUtils.toArrowSchema(schema, "UTC", true))
        .build();
  }

  private static DataFile dataFile(int major, int minor, int... fields) {
    return new DataFile("file.lance", fields, fields, major, minor, null, null);
  }

  private static List<FragmentMetadata> fragments(DataFile file) {
    return Collections.singletonList(
        new FragmentMetadata(0, Collections.singletonList(file), 1L, null, null));
  }

  private static StructField blobV1Field(String name) {
    return new StructField(
        name,
        DataTypes.BinaryType,
        true,
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build());
  }

  private static StructField blobV2Field(String name) {
    return new StructField(
        name,
        DataTypes.BinaryType,
        true,
        new MetadataBuilder()
            .putString(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2)
            .build());
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
