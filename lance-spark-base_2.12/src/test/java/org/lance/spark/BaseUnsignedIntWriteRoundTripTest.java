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
package org.lance.spark;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips Lance unsigned integer columns (uint8/16/32/64) through a Spark read then write.
 *
 * <p>Spark has no unsigned integer types, so {@code LanceArrowUtils.fromArrowField} widens each to
 * the smallest signed type that holds it and records the original width in field metadata. On write
 * {@code toArrowField} restores the unsigned Arrow type, and {@code LanceArrowWriter} needs a
 * matching unsigned field writer or the batch fails at writer initialization. A producer must go
 * through the Arrow C Data interface to create such a dataset, exactly as {@code
 * BaseFixedSizeBinaryReadTest} does for a type Spark cannot express either.
 */
public abstract class BaseUnsignedIntWriteRoundTripTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("unsigned-int-write-roundtrip-test")
            .master("local[*]")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static Field unsignedField(String name, int bitWidth) {
    return new Field(
        name, new FieldType(true, new ArrowType.Int(bitWidth, false), null, null), null);
  }

  /** Creates a Lance dataset with an unsigned column of every width plus a signed control. */
  private static void createUnsignedDataset(String datasetUri) throws Exception {
    Schema arrowSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                unsignedField("u8", 8),
                unsignedField("u16", 16),
                unsignedField("u32", 32),
                unsignedField("u64", 64)));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector id = (IntVector) root.getVector("id");
      UInt1Vector u8 = (UInt1Vector) root.getVector("u8");
      UInt2Vector u16 = (UInt2Vector) root.getVector("u16");
      UInt4Vector u32 = (UInt4Vector) root.getVector("u32");
      UInt8Vector u64 = (UInt8Vector) root.getVector("u64");

      // Row 0 holds the maximum value of each width, which reads as a negative number if the width
      // is ever mistaken for signed, so a correct round trip proves the unsigned type survived.
      id.setSafe(0, 1);
      u8.setSafe(0, 255);
      u16.setSafe(0, 65535);
      u32.setSafe(0, (int) 4294967295L);
      u64.setSafe(0, 9000000000L);
      root.setRowCount(1);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      try (ArrowStreamReader reader =
              new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), allocator);
          ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, arrowStream);
        org.lance.Dataset.write().stream(arrowStream).uri(datasetUri).execute().close();
      }
    }
  }

  private static void assertUnsigned(Schema schema, String name, int expectedWidth) {
    ArrowType.Int type = (ArrowType.Int) schema.findField(name).getType();
    assertEquals(expectedWidth, type.getBitWidth(), name + " width");
    assertFalse(type.getIsSigned(), name + " should be unsigned");
  }

  /**
   * A read -&gt; write cycle must reproduce every unsigned width in the written Lance schema.
   * Without the unsigned field writers this throws {@code UnsupportedOperationException} at batch
   * writer initialization for uint8/16/32 (uint64 already had a writer).
   */
  @Test
  public void testUnsignedWidthsRoundTripThroughSparkWrite() throws Exception {
    String sourceUri = tempDir.resolve("uint_roundtrip_source.lance").toString();
    String targetUri = tempDir.resolve("uint_roundtrip_target.lance").toString();
    createUnsignedDataset(sourceUri);

    spark
        .read()
        .format(LanceDataSource.name)
        .load(sourceUri)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.ErrorIfExists)
        .save(targetUri);

    try (org.lance.Dataset dataset =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(targetUri).build()) {
      Schema written = dataset.getSchema();
      assertUnsigned(written, "u8", 8);
      assertUnsigned(written, "u16", 16);
      assertUnsigned(written, "u32", 32);
      assertUnsigned(written, "u64", 64);
      // The genuinely signed control must stay signed: a copied marker cannot mint an unsigned
      // type.
      ArrowType.Int id = (ArrowType.Int) written.findField("id").getType();
      assertEquals(32, id.getBitWidth());
      assertTrue(id.getIsSigned());
    }

    List<Row> rows =
        spark
            .read()
            .format(LanceDataSource.name)
            .load(targetUri)
            .selectExpr("u8", "u16", "u32", "u64")
            .collectAsList();
    assertEquals(1, rows.size());
    assertEquals((short) 255, rows.get(0).getShort(0));
    assertEquals(65535, rows.get(0).getInt(1));
    assertEquals(4294967295L, rows.get(0).getLong(2));
    assertEquals(9000000000L, rows.get(0).getLong(3));
  }

  /**
   * Appending through Spark to an existing unsigned Lance table must succeed: the writer restores
   * the unsigned Arrow type so the batch passes Lance's schema validation against the existing
   * unsigned columns instead of presenting a signed type.
   */
  @Test
  public void testAppendToExistingUnsignedTable() throws Exception {
    String datasetUri = tempDir.resolve("uint_append.lance").toString();
    createUnsignedDataset(datasetUri);

    StructType readSchema = spark.read().format(LanceDataSource.name).load(datasetUri).schema();
    Row newRow = RowFactory.create(2, (short) 200, 40000, 3000000000L, 5000000000L);
    spark
        .createDataFrame(Collections.singletonList(newRow), readSchema)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.Append)
        .save(datasetUri);

    List<Row> rows =
        spark
            .read()
            .format(LanceDataSource.name)
            .load(datasetUri)
            .orderBy("id")
            .selectExpr("u8", "u16", "u32", "u64")
            .collectAsList();
    assertEquals(2, rows.size(), "the appended row should be present");
    assertEquals((short) 255, rows.get(0).getShort(0));
    assertEquals(4294967295L, rows.get(0).getLong(2));
    assertEquals((short) 200, rows.get(1).getShort(0));
    assertEquals(40000, rows.get(1).getInt(1));
    assertEquals(3000000000L, rows.get(1).getLong(2));
    assertEquals(5000000000L, rows.get(1).getLong(3));
  }

  /**
   * A value Spark's wider signed type can hold but the unsigned width cannot (256 for uint8, or a
   * negative) must be rejected, not silently narrowed and committed, so an append can never store a
   * value different from what the user wrote.
   */
  @Test
  public void testOutOfRangeUnsignedValuesAreRejected() throws Exception {
    String datasetUri = tempDir.resolve("uint_range.lance").toString();
    createUnsignedDataset(datasetUri);
    StructType schema = spark.read().format(LanceDataSource.name).load(datasetUri).schema();

    assertAppendRejected(schema, datasetUri, RowFactory.create(2, (short) 256, 0, 0L, 0L));
    assertAppendRejected(schema, datasetUri, RowFactory.create(2, (short) 0, 65536, 0L, 0L));
    assertAppendRejected(schema, datasetUri, RowFactory.create(2, (short) 0, 0, 4294967296L, 0L));
    assertAppendRejected(schema, datasetUri, RowFactory.create(2, (short) -1, 0, 0L, 0L));

    // None of the rejected appends may have published a row.
    assertEquals(
        1L,
        spark.read().format(LanceDataSource.name).load(datasetUri).count(),
        "a rejected append must not add rows");
  }

  private void assertAppendRejected(StructType schema, String datasetUri, Row row) {
    assertThrows(
        Exception.class,
        () ->
            spark
                .createDataFrame(Collections.singletonList(row), schema)
                .write()
                .format(LanceDataSource.name)
                .mode(SaveMode.Append)
                .save(datasetUri));
  }
}
