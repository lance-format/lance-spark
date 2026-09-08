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
package org.lance.spark.write;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.Float16Utils;

import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Tests that overwrite mode preserves the original Arrow schema (types, nullability, metadata). */
public class OverwriteSchemaPreservationTest {
  @TempDir Path tempDir;

  // ==================== Helpers ====================

  private String datasetUri(String name) {
    return tempDir.resolve(name + LanceSparkReadOptions.LANCE_FILE_SUFFIX).toString();
  }

  private void createEmptyDataset(String uri, Schema schema) {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(allocator, uri, schema, new WriteParams.Builder().build()).close();
    }
  }

  /** Overwrites dataset with given rows, returns the schema read back. */
  private Schema overwriteAndGetSchema(String uri, Schema originalSchema, InternalRow... rows)
      throws IOException {
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(originalSchema);
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    return overwriteAndGetSchemaWithSparkSchema(uri, sparkSchema, writeOptions, rows);
  }

  private LanceBatchWrite buildOverwriteBatch(
      StructType sparkSchema, LanceSparkWriteOptions writeOptions) {
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            sparkSchema,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false);
    builder.truncate();
    return (LanceBatchWrite) builder.build().toBatch();
  }

  /**
   * Overwrites dataset using an explicitly provided Spark schema + write options. Useful for
   * testing schema compatibility branches where Spark schema differs from Lance schema.
   */
  private Schema overwriteAndGetSchemaWithSparkSchema(
      String uri, StructType sparkSchema, LanceSparkWriteOptions writeOptions, InternalRow... rows)
      throws IOException {
    LanceBatchWrite batchWrite = buildOverwriteBatch(sparkSchema, writeOptions);
    LanceDataWriter.WriterFactory factory =
        (LanceDataWriter.WriterFactory) batchWrite.createBatchWriterFactory(null);
    LanceDataWriter writer = (LanceDataWriter) factory.createWriter(0, 0);

    for (InternalRow row : rows) {
      writer.write(row);
    }
    WriterCommitMessage commitMsg = writer.commit();
    writer.close();
    batchWrite.commit(new WriterCommitMessage[] {commitMsg});

    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      return ds.getSchema();
    }
  }

  private boolean causeContains(Throwable error, String message) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(message)) {
        return true;
      }
    }
    return false;
  }

  /** Compares two Arrow schemas field-by-field (type, nullability, metadata, children). */
  private void assertSchemaEquals(Schema expected, Schema actual, String message) {
    assertEquals(
        expected.getFields().size(),
        actual.getFields().size(),
        message + " — field count mismatch");
    assertEquals(
        expected.getCustomMetadata(),
        actual.getCustomMetadata(),
        message + " — schema-level metadata mismatch");
    for (int i = 0; i < expected.getFields().size(); i++) {
      Field ef = expected.getFields().get(i);
      Field af = actual.getFields().get(i);
      assertEquals(ef.getName(), af.getName(), message + " — field[" + i + "] name mismatch");
      assertEquals(
          ef.getType(),
          af.getType(),
          message + " — field[" + i + "] '" + ef.getName() + "' type mismatch");
      assertEquals(
          ef.isNullable(),
          af.isNullable(),
          message + " — field[" + i + "] '" + ef.getName() + "' nullable mismatch");
      assertEquals(
          ef.getFieldType().getMetadata(),
          af.getFieldType().getMetadata(),
          message + " — field[" + i + "] '" + ef.getName() + "' metadata mismatch");
      assertEquals(
          ef.getChildren(),
          af.getChildren(),
          message + " — field[" + i + "] '" + ef.getName() + "' children mismatch");
    }
  }

  private void assertSchemaEquals(Schema expected, Schema actual) {
    assertSchemaEquals(expected, actual, "Schema mismatch");
  }

  // ==================== Unsigned Int Preservation ====================

  @Test
  public void testOverwritePreservesUint32(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row = new GenericInternalRow(new Object[] {1, 42L});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertSchemaEquals(schema, after, "uint32 must remain unsigned after overwrite");
  }

  @Test
  public void testOverwritePreservesUint64(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("u64", FieldType.nullable(new ArrowType.Int(64, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row = new GenericInternalRow(new Object[] {1, 100L});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertSchemaEquals(schema, after, "uint64 must remain unsigned after overwrite");
  }

  // Verifies uint64 high-range values (-1L, Long.MIN_VALUE) round-trip with bit pattern preserved.
  @Test
  public void testUint64HighRangeWriteAndReadBack(TestInfo testInfo) throws Exception {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("u64", FieldType.nullable(new ArrowType.Int(64, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // -1L = 0xFFFFFFFFFFFFFFFF = uint64 max (18446744073709551615)
    // Long.MIN_VALUE = 0x8000000000000000 = uint64 (9223372036854775808)
    InternalRow row1 = new GenericInternalRow(new Object[] {1, -1L});
    InternalRow row2 = new GenericInternalRow(new Object[] {2, Long.MIN_VALUE});
    InternalRow row3 = new GenericInternalRow(new Object[] {3, 42L});
    Schema after = overwriteAndGetSchema(uri, schema, row1, row2, row3);

    // Schema preserved
    assertSchemaEquals(schema, after, "uint64 schema must be preserved");

    // Read through Arrow so values above Long.MAX_VALUE remain observable as unsigned BigInteger.
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(allocator).uri(uri).build();
        Scanner scanner = ds.newScan();
        ArrowReader reader = scanner.scanBatches()) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());
      assertEquals(
          "18446744073709551615",
          Long.toUnsignedString(((Number) root.getVector("u64").getObject(0)).longValue()));
      assertEquals(
          "9223372036854775808",
          Long.toUnsignedString(((Number) root.getVector("u64").getObject(1)).longValue()));
      assertEquals(
          "42", Long.toUnsignedString(((Number) root.getVector("u64").getObject(2)).longValue()));
    }
  }

  @Test
  public void testOverwritePreservesUint8AndUint16(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("u8", FieldType.nullable(new ArrowType.Int(8, false)), null),
                new Field("u16", FieldType.nullable(new ArrowType.Int(16, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Spark maps uint8->short, uint16->int
    InternalRow row = new GenericInternalRow(new Object[] {(short) 255, 65535});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertSchemaEquals(schema, after, "uint8 and uint16 must remain unsigned after overwrite");
  }

  // ==================== Nullable Preservation ====================

  @Test
  public void testOverwritePreservesNotNullable(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row = new GenericInternalRow(new Object[] {1, UTF8String.fromString("hello")});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertFalse(
        after.getFields().get(0).isNullable(), "id field must remain non-nullable after overwrite");
    assertTrue(
        after.getFields().get(1).isNullable(), "name field must remain nullable after overwrite");
    assertSchemaEquals(schema, after);
  }

  @Test
  public void testOverwritePreservesMixedNullability(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("a", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("b", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field(
                    "c",
                    FieldType.notNullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null),
                new Field("d", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row =
        new GenericInternalRow(new Object[] {10L, 20L, 3.14, UTF8String.fromString("test")});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertFalse(after.getFields().get(0).isNullable(), "a: notNullable");
    assertTrue(after.getFields().get(1).isNullable(), "b: nullable");
    assertFalse(after.getFields().get(2).isNullable(), "c: notNullable");
    assertTrue(after.getFields().get(3).isNullable(), "d: nullable");
  }

  // ==================== Complex Types ====================

  @Test
  public void testOverwritePreservesFixedSizeList(TestInfo testInfo) throws Exception {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "vec",
                    FieldType.nullable(new ArrowType.FixedSizeList(4)),
                    Collections.singletonList(
                        new Field(
                            "item",
                            FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                            null)))));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Spark represents FixedSizeList<Float32>(4) as ArrayType(FloatType).
    InternalRow row1 =
        new GenericInternalRow(
            new Object[] {1, new GenericArrayData(new Object[] {1.0f, 2.0f, 3.0f, 4.0f})});
    InternalRow row2 = new GenericInternalRow(new Object[] {2, null});
    InternalRow row3 =
        new GenericInternalRow(
            new Object[] {3, new GenericArrayData(new Object[] {5.0f, 6.0f, 7.0f, 8.0f})});
    Schema after = overwriteAndGetSchema(uri, schema, row1, row2, row3);

    Field vecField = after.getFields().get(1);
    assertTrue(
        vecField.getType() instanceof ArrowType.FixedSizeList,
        "vec must remain FixedSizeList, got: " + vecField.getType());
    assertEquals(
        4,
        ((ArrowType.FixedSizeList) vecField.getType()).getListSize(),
        "FixedSizeList size must be preserved");

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(allocator).uri(uri).build();
        Scanner scanner = ds.newScan();
        ArrowReader reader = scanner.scanBatches()) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());
      assertTrue(root.getVector("vec").isNull(1));
      assertNotNull(root.getVector("vec").getObject(2));
    }
  }

  @Test
  public void testOverwriteRejectsFixedSizeListWithLargeUtf8Elements(TestInfo testInfo) {
    Schema schema =
        new Schema(
            Collections.singletonList(
                new Field(
                    "values",
                    FieldType.nullable(new ArrowType.FixedSizeList(3)),
                    Collections.singletonList(
                        new Field(
                            "item", FieldType.nullable(ArrowType.LargeUtf8.INSTANCE), null)))));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> buildOverwriteBatch(sparkSchema, LanceSparkWriteOptions.from(uri)));
    assertTrue(error.getMessage().contains("incompatible with the existing Lance schema"));
  }

  @Test
  public void testOverwritePreservesFixedSizeBinary(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("hash", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row = new GenericInternalRow(new Object[] {1, new byte[16]});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    Field hashField = after.getFields().get(1);
    assertTrue(
        hashField.getType() instanceof ArrowType.FixedSizeBinary,
        "hash must remain FixedSizeBinary, got: " + hashField.getType());
    assertEquals(
        16,
        ((ArrowType.FixedSizeBinary) hashField.getType()).getByteWidth(),
        "FixedSizeBinary width must be preserved");
  }

  @Test
  public void testOverwritePreservesLargeUtf8(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("text", FieldType.nullable(new ArrowType.LargeUtf8()), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row = new GenericInternalRow(new Object[] {1, UTF8String.fromString("hello")});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    Field textField = after.getFields().get(1);
    assertTrue(
        textField.getType() instanceof ArrowType.LargeUtf8,
        "text must remain LargeUtf8, got: " + textField.getType());
    assertSchemaEquals(schema, after, "LargeUtf8 must be preserved after overwrite");
  }

  @Test
  public void testOverwritePreservesLargeBinary(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("blob", FieldType.nullable(new ArrowType.LargeBinary()), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row = new GenericInternalRow(new Object[] {1, new byte[] {1, 2, 3, 4}});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    Field blobField = after.getFields().get(1);
    assertTrue(
        blobField.getType() instanceof ArrowType.LargeBinary,
        "blob must remain LargeBinary, got: " + blobField.getType());
    assertSchemaEquals(schema, after, "LargeBinary must be preserved after overwrite");
  }

  @Test
  public void testOverwritePreservesDecimal(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("amount", FieldType.nullable(new ArrowType.Decimal(20, 4, 128)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row =
        new GenericInternalRow(new Object[] {1, Decimal.apply(new BigDecimal("12345.6789"))});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    Field amountField = after.getFields().get(1);
    assertTrue(
        amountField.getType() instanceof ArrowType.Decimal,
        "amount must remain Decimal, got: " + amountField.getType());
    assertSchemaEquals(schema, after, "Decimal type must be preserved after overwrite");
  }

  @Test
  public void testOverwritePreservesNestedStruct(TestInfo testInfo) throws IOException {
    Field payloadField =
        new Field(
            "payload",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(
                new Field(
                    "score",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null),
                new Field("label", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                payloadField));

    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow payload =
        new GenericInternalRow(new Object[] {0.95d, UTF8String.fromString("tag-a")});
    InternalRow row = new GenericInternalRow(new Object[] {1, payload});

    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertSchemaEquals(schema, after, "Nested Struct schema must be preserved after overwrite");
  }

  // ==================== Full Schema with All Common Types ====================

  @Test
  public void testOverwritePreservesFullMixedSchema(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("i32", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field("i64", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("u64", FieldType.nullable(new ArrowType.Int(64, false)), null),
                new Field(
                    "f32",
                    FieldType.notNullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    null),
                new Field(
                    "f64",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null),
                new Field("flag", FieldType.notNullable(ArrowType.Bool.INSTANCE), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row =
        new GenericInternalRow(
            new Object[] {1, 2L, 3L, 4L, 1.5f, 3.14, true, UTF8String.fromString("hello")});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertSchemaEquals(schema, after, "Full mixed schema must be preserved after overwrite");
  }

  // ==================== Multiple Rows ====================

  @Test
  public void testOverwriteMultipleRows(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("u64", FieldType.nullable(new ArrowType.Int(64, false)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    InternalRow row1 =
        new GenericInternalRow(new Object[] {1, 100L, UTF8String.fromString("alice")});
    InternalRow row2 = new GenericInternalRow(new Object[] {2, 200L, UTF8String.fromString("bob")});
    InternalRow row3 =
        new GenericInternalRow(new Object[] {3, null, UTF8String.fromString("charlie")});
    Schema after = overwriteAndGetSchema(uri, schema, row1, row2, row3);

    assertSchemaEquals(
        schema, after, "Schema must be preserved after overwrite with multiple rows");

    // Verify data count
    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      assertEquals(3, ds.countRows(), "Dataset must have 3 rows after overwrite");
    }
  }

  // ==================== Consecutive Overwrites ====================

  @Test
  public void testConsecutiveOverwritesPreserveSchema(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("u64", FieldType.nullable(new ArrowType.Int(64, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // First overwrite
    InternalRow row1 = new GenericInternalRow(new Object[] {1, 100L});
    Schema after1 = overwriteAndGetSchema(uri, schema, row1);
    assertSchemaEquals(schema, after1, "Schema after 1st overwrite");

    // Second overwrite
    InternalRow row2 = new GenericInternalRow(new Object[] {2, 200L});
    Schema after2 = overwriteAndGetSchema(uri, after1, row2);
    assertSchemaEquals(schema, after2, "Schema after 2nd overwrite");

    // Third overwrite
    InternalRow row3 = new GenericInternalRow(new Object[] {3, 300L});
    Schema after3 = overwriteAndGetSchema(uri, after2, row3);
    assertSchemaEquals(schema, after3, "Schema after 3rd overwrite");
  }

  @Test
  public void testNullStructWithNonNullableChildHasActionableError(TestInfo testInfo) {
    Field payload =
        new Field(
            "payload",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(
                new Field("value", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
    Schema schema = new Schema(Collections.singletonList(payload));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    IOException error =
        assertThrows(
            IOException.class,
            () -> overwriteAndGetSchema(uri, schema, new GenericInternalRow(new Object[] {null})));
    assertTrue(
        causeContains(error, "null parent struct cannot be written"),
        "Error must explain the preserved struct nullability constraint: " + error);
  }

  // ==================== Schema Mismatch Rejection ====================

  @Test
  public void testOverwriteRejectsFieldCountMismatch(TestInfo testInfo) {
    Schema originalSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, originalSchema);

    // Try to overwrite with a schema that has 3 fields (original has 2)
    Schema wrongSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("extra", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    StructType wrongSparkSchema = LanceArrowUtils.fromArrowSchema(wrongSchema);
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            wrongSparkSchema,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false);
    builder.truncate();

    // Early-fail on driver: field count mismatch is caught in LanceBatchWrite constructor
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.build().toBatch(),
        "Must reject field count mismatch in overwrite mode");
  }

  @Test
  public void testOverwriteAllowsCompatibleTypeDifferenceWithSameFieldCount(TestInfo testInfo)
      throws IOException {
    Schema originalSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, originalSchema);

    // Same field count, but Spark side uses LongType for unsigned uint32 column.
    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, new MetadataBuilder().build()),
              new StructField("u32", DataTypes.LongType, true, new MetadataBuilder().build())
            });

    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            sparkSchema,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false);
    builder.truncate();
    LanceBatchWrite batchWrite = (LanceBatchWrite) builder.build().toBatch();
    LanceDataWriter.WriterFactory factory =
        (LanceDataWriter.WriterFactory) batchWrite.createBatchWriterFactory(null);
    LanceDataWriter writer = (LanceDataWriter) factory.createWriter(0, 0);

    InternalRow row = new GenericInternalRow(new Object[] {1, 42L});
    writer.write(row);
    WriterCommitMessage commitMsg = writer.commit();
    writer.close();
    batchWrite.commit(new WriterCommitMessage[] {commitMsg});

    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      Schema after = ds.getSchema();
      assertSchemaEquals(
          originalSchema,
          after,
          "Compatible type difference with same field count should keep Lance schema unchanged");
      assertEquals(1, ds.countRows());
    }
  }

  @Test
  public void testOverwriteAllowsCompatibleTypeDifferenceU8AndU16WithSameFieldCount(
      TestInfo testInfo) throws IOException {
    Schema originalSchema =
        new Schema(
            Arrays.asList(
                new Field("u8", FieldType.nullable(new ArrowType.Int(8, false)), null),
                new Field("u16", FieldType.nullable(new ArrowType.Int(16, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, originalSchema);

    // Explicit Spark-side widening for unsigned columns: uint8->short, uint16->int
    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField("u8", DataTypes.ShortType, true, new MetadataBuilder().build()),
              new StructField("u16", DataTypes.IntegerType, true, new MetadataBuilder().build())
            });

    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    InternalRow row = new GenericInternalRow(new Object[] {(short) 255, 65535});
    Schema after = overwriteAndGetSchemaWithSparkSchema(uri, sparkSchema, writeOptions, row);

    assertSchemaEquals(
        originalSchema,
        after,
        "Compatible u8/u16 type difference should keep Lance schema unchanged");
  }

  @Test
  public void testOverwriteRejectsIncompatibleTopLevelTypeWithSameFieldCount(TestInfo testInfo) {
    Schema originalSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, originalSchema);

    // Same field count, but incompatible top-level type: id int32 -> string
    StructType incompatibleSparkSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
              new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
            });

    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    assertThrows(
        IllegalArgumentException.class,
        () -> buildOverwriteBatch(incompatibleSparkSchema, writeOptions),
        "Driver must reject incompatible top-level types before creating executor writers");
  }

  @Test
  public void testOverwriteRejectsIncompatibleNestedChildType(TestInfo testInfo) {
    Field payloadField =
        new Field(
            "payload",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(
                new Field(
                    "score",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null),
                new Field("label", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    Schema originalSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                payloadField));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, originalSchema);

    // Child type mismatch under same field count: payload.score double -> string
    StructType incompatiblePayloadType =
        new StructType(
            new StructField[] {
              new StructField("score", DataTypes.StringType, true, new MetadataBuilder().build()),
              new StructField("label", DataTypes.StringType, true, new MetadataBuilder().build())
            });
    StructType incompatibleSparkSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, new MetadataBuilder().build()),
              new StructField(
                  "payload", incompatiblePayloadType, true, new MetadataBuilder().build())
            });

    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    assertThrows(
        IllegalArgumentException.class,
        () -> buildOverwriteBatch(incompatibleSparkSchema, writeOptions),
        "Driver must reject incompatible nested types before creating executor writers");
  }

  @Test
  public void testOverwriteRejectsOutOfRangeUint8(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("u8", FieldType.nullable(new ArrowType.Int(8, false)), null),
                new Field("u16", FieldType.nullable(new ArrowType.Int(16, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // -1 is out of uint8 range [0, 255]
    InternalRow row = new GenericInternalRow(new Object[] {(short) -1, 100});
    assertThrows(
        ArithmeticException.class,
        () -> overwriteAndGetSchema(uri, schema, row),
        "uint8 writer must reject negative values");
  }

  @Test
  public void testOverwriteRejectsOutOfRangeUint16(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("u8", FieldType.nullable(new ArrowType.Int(8, false)), null),
                new Field("u16", FieldType.nullable(new ArrowType.Int(16, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // 65536 is out of uint16 range [0, 65535]
    InternalRow row = new GenericInternalRow(new Object[] {(short) 100, 65536});
    assertThrows(
        ArithmeticException.class,
        () -> overwriteAndGetSchema(uri, schema, row),
        "uint16 writer must reject values above 65535");
  }

  @Test
  public void testOverwriteRejectsOutOfRangeUint32(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // 5000000000L is out of uint32 range [0, 4294967295]
    InternalRow row = new GenericInternalRow(new Object[] {1, 5000000000L});
    assertThrows(
        ArithmeticException.class,
        () -> overwriteAndGetSchema(uri, schema, row),
        "uint32 writer must reject values above 4294967295");
  }

  @Test
  public void testOverwritePreservesSchemaWithQueuedWriteBuffer(TestInfo testInfo)
      throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("u8", FieldType.nullable(new ArrowType.Int(8, false)), null),
                new Field("u16", FieldType.nullable(new ArrowType.Int(16, false)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(uri).toBuilder()
            .useQueuedWriteBuffer(true)
            .queueDepth(2)
            .build();

    InternalRow row =
        new GenericInternalRow(
            new Object[] {(short) 255, 65535, UTF8String.fromString("queued-path")});
    Schema after = overwriteAndGetSchemaWithSparkSchema(uri, sparkSchema, writeOptions, row);

    assertSchemaEquals(schema, after, "Queued write buffer path must preserve Lance schema");
  }

  // ==================== Append Uses Existing Schema ====================

  @Test
  public void testAppendUsesExistingUnsignedSchema(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("value", FieldType.nullable(new ArrowType.Int(32, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Append mode (no truncate)
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            sparkSchema,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false);
    // No builder.truncate() — this is append mode
    LanceBatchWrite batchWrite = (LanceBatchWrite) builder.build().toBatch();
    LanceDataWriter.WriterFactory factory =
        (LanceDataWriter.WriterFactory) batchWrite.createBatchWriterFactory(null);
    LanceDataWriter writer = (LanceDataWriter) factory.createWriter(0, 0);

    InternalRow row = new GenericInternalRow(new Object[] {1, 42L});
    writer.write(row);
    WriterCommitMessage commitMsg = writer.commit();
    writer.close();
    batchWrite.commit(new WriterCommitMessage[] {commitMsg});

    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      assertEquals(1, ds.countRows());
      assertSchemaEquals(schema, ds.getSchema(), "Append must use the existing uint32 schema");
    }
  }

  @Test
  public void testAppendRejectsLargeVarOptionForExistingSmallVarSchema(TestInfo testInfo) {
    Schema schema =
        new Schema(
            Collections.singletonList(
                new Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(uri).useLargeVarTypes(true).build();
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            sparkSchema,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false);

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> builder.build().toBatch());
    assertTrue(error.getMessage().contains("cannot change an existing table schema during append"));
  }

  // ==================== Overwrite Empty Dataset ====================

  @Test
  public void testOverwriteEmptyDataset(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("u64", FieldType.nullable(new ArrowType.Int(64, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Overwrite an empty dataset (0 rows -> N rows)
    InternalRow row = new GenericInternalRow(new Object[] {1, 999L});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertSchemaEquals(schema, after, "Overwriting an empty dataset must preserve original schema");
    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      assertEquals(1, ds.countRows());
    }
  }

  // ==================== Metadata + FixedSizeList (Embedding) Preservation ====================

  @Test
  public void testOverwriteKeepsLanceFieldMetadataWhenSparkMetadataDiffers(TestInfo testInfo)
      throws IOException {
    Map<String, String> idMeta = new HashMap<>();
    idMeta.put("app:owner", "lance");
    idMeta.put("app:pii", "false");

    Map<String, String> nameMeta = new HashMap<>();
    nameMeta.put("app:owner", "lance");
    nameMeta.put("app:tokenized", "true");

    Schema originalSchema =
        new Schema(
            Arrays.asList(
                new Field(
                    "id", new FieldType(true, new ArrowType.Int(32, true), null, idMeta), null),
                new Field(
                    "name", new FieldType(true, ArrowType.Utf8.INSTANCE, null, nameMeta), null)));

    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, originalSchema);

    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField(
                  "id",
                  DataTypes.IntegerType,
                  true,
                  new MetadataBuilder()
                      .putString("app:owner", "spark")
                      .putBoolean("spark", true)
                      .build()),
              new StructField(
                  "name",
                  DataTypes.StringType,
                  true,
                  new MetadataBuilder()
                      .putString("app:owner", "spark")
                      .putBoolean("spark", true)
                      .build())
            });

    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(uri);
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            sparkSchema,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false);
    builder.truncate();
    LanceBatchWrite batchWrite = (LanceBatchWrite) builder.build().toBatch();
    LanceDataWriter.WriterFactory factory =
        (LanceDataWriter.WriterFactory) batchWrite.createBatchWriterFactory(null);
    LanceDataWriter writer = (LanceDataWriter) factory.createWriter(0, 0);

    InternalRow row = new GenericInternalRow(new Object[] {1, UTF8String.fromString("hello")});
    writer.write(row);
    WriterCommitMessage commitMsg = writer.commit();
    writer.close();
    batchWrite.commit(new WriterCommitMessage[] {commitMsg});

    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      Schema after = ds.getSchema();

      assertEquals(
          idMeta,
          after.getFields().get(0).getFieldType().getMetadata(),
          "id field metadata must remain Lance-side metadata");
      assertEquals(
          nameMeta,
          after.getFields().get(1).getFieldType().getMetadata(),
          "name field metadata must remain Lance-side metadata");
      assertNotEquals(
          "spark",
          after.getFields().get(0).getFieldType().getMetadata().get("app:owner"),
          "Spark metadata must not overwrite Lance metadata");
    }
  }

  // Embedding table scenario: FixedSizeList(128) + field-level metadata must survive overwrite.
  @Test
  public void testOverwritePreservesMetadataAndFixedSizeList(TestInfo testInfo) throws IOException {
    // Simulate a typical embedding table: id, text, vector(128-dim), with field-level metadata
    Map<String, String> schemaMetadata = new HashMap<>();
    schemaMetadata.put("lance:encoding", "plain");
    schemaMetadata.put("custom:source", "model-v2");
    schemaMetadata.put("app:version", "1.0.3");

    Map<String, String> idFieldMetadata = new HashMap<>();
    idFieldMetadata.put("lance:storage_class", "primary_key");
    idFieldMetadata.put("app:auto_increment", "true");

    Map<String, String> textFieldMetadata = new HashMap<>();
    textFieldMetadata.put("lance:encoding", "dict");
    textFieldMetadata.put("app:max_length", "4096");

    Map<String, String> vecFieldMetadata = new HashMap<>();
    vecFieldMetadata.put("lance:dimension", "128");
    vecFieldMetadata.put("lance:metric_type", "L2");

    Map<String, String> scoreFieldMetadata = new HashMap<>();
    scoreFieldMetadata.put("app:range", "[0.0, 1.0]");
    scoreFieldMetadata.put("app:description", "similarity score");

    int dim = 128;
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field(
                    "id",
                    new FieldType(false, new ArrowType.Int(32, true), null, idFieldMetadata),
                    null),
                new Field(
                    "text",
                    new FieldType(true, ArrowType.Utf8.INSTANCE, null, textFieldMetadata),
                    null),
                new Field(
                    "embedding",
                    new FieldType(true, new ArrowType.FixedSizeList(dim), null, vecFieldMetadata),
                    Collections.singletonList(
                        new Field(
                            "item",
                            FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                            null))),
                new Field(
                    "score",
                    new FieldType(
                        true,
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                        null,
                        scoreFieldMetadata),
                    null)),
            schemaMetadata);
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Construct a row with array<float> of dim=128
    Object[] floatArr = new Object[dim];
    for (int i = 0; i < dim; i++) {
      floatArr[i] = (float) i * 0.01f;
    }
    InternalRow row1 =
        new GenericInternalRow(
            new Object[] {
              1, UTF8String.fromString("hello world"), new GenericArrayData(floatArr), 0.95
            });

    Object[] floatArr2 = new Object[dim];
    for (int i = 0; i < dim; i++) {
      floatArr2[i] = (float) i * 0.02f;
    }
    InternalRow row2 =
        new GenericInternalRow(
            new Object[] {
              2, UTF8String.fromString("goodbye"), new GenericArrayData(floatArr2), 0.87
            });

    Schema after = overwriteAndGetSchema(uri, schema, row1, row2);

    // Full semantic schema comparison (types, nullable, metadata, children)
    assertSchemaEquals(schema, after, "Schema with metadata and FixedSizeList must be preserved");

    // A few explicit checks for readability
    Field embeddingField = after.getFields().get(2);
    assertTrue(
        embeddingField.getType() instanceof ArrowType.FixedSizeList,
        "embedding must remain FixedSizeList, got: " + embeddingField.getType());
    assertEquals(dim, ((ArrowType.FixedSizeList) embeddingField.getType()).getListSize());

    // Verify row count
    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      assertEquals(2, ds.countRows());
    }

    // Second overwrite to confirm stability
    Schema after2 = overwriteAndGetSchema(uri, after, row1);
    assertSchemaEquals(schema, after2, "Schema must remain stable after consecutive overwrites");
  }

  // ==================== Large Batch Overwrite ====================

  @Test
  public void testOverwriteLargeBatch(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field(
                    "value",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Write 500 rows to exercise batch boundary behavior
    int numRows = 500;
    InternalRow[] rows = new InternalRow[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = new GenericInternalRow(new Object[] {i, (long) (i * 2), i * 0.5});
    }
    Schema after = overwriteAndGetSchema(uri, schema, rows);

    assertSchemaEquals(schema, after, "Schema must be preserved after large batch overwrite");
    try (Dataset ds =
        Dataset.open().allocator(new RootAllocator(Long.MAX_VALUE)).uri(uri).build()) {
      assertEquals(numRows, ds.countRows());
    }
  }

  // ==================== Unsigned Value Round-Trip ====================

  @Test
  public void testUnsignedValueRoundTrip(TestInfo testInfo) throws Exception {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("u8", FieldType.nullable(new ArrowType.Int(8, false)), null),
                new Field("u16", FieldType.nullable(new ArrowType.Int(16, false)), null),
                new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Boundary values: 0, max, and a mid value
    InternalRow row1 = new GenericInternalRow(new Object[] {(short) 0, 0, 0L});
    InternalRow row2 = new GenericInternalRow(new Object[] {(short) 255, 65535, 4294967295L});
    InternalRow row3 = new GenericInternalRow(new Object[] {(short) 128, 32768, 2147483648L});
    Schema after = overwriteAndGetSchema(uri, schema, row1, row2, row3);

    assertSchemaEquals(schema, after, "unsigned schema must be preserved");

    // Verify actual values via Scanner
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(allocator).uri(uri).build();
        Scanner scanner = ds.newScan();
        ArrowReader reader = scanner.scanBatches()) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());

      // Row 0: all zeros
      assertEquals(0, ((Number) root.getVector("u8").getObject(0)).byteValue() & 0xFF);
      assertEquals(0, ((Character) root.getVector("u16").getObject(0)) & 0xFFFF);
      assertEquals(0L, Integer.toUnsignedLong((int) root.getVector("u32").getObject(0)));

      // Row 1: max values — read back as unsigned interpretation
      // uint8 max=255 stored as byte -1, uint16 max=65535 stored as char 0xFFFF,
      // uint32 max=4294967295 stored as int -1
      assertEquals(
          ((short) 255 & 0xFF), ((Number) root.getVector("u8").getObject(1)).byteValue() & 0xFF);
      assertEquals(65535, ((Character) root.getVector("u16").getObject(1)) & 0xFFFF);
      assertEquals(4294967295L, Integer.toUnsignedLong((int) root.getVector("u32").getObject(1)));

      // Row 2: mid values
      assertEquals(128, ((Number) root.getVector("u8").getObject(2)).byteValue() & 0xFF);
      assertEquals(32768, ((Character) root.getVector("u16").getObject(2)) & 0xFFFF);
      assertEquals(2147483648L, Integer.toUnsignedLong((int) root.getVector("u32").getObject(2)));
    }
  }

  @Test
  public void testUnsignedIntWriterValueRoundTrip(TestInfo testInfo) throws Exception {
    Schema schema =
        new Schema(
            Collections.singletonList(
                new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField("u32", DataTypes.IntegerType, true, new MetadataBuilder().build())
            });
    overwriteAndGetSchemaWithSparkSchema(
        uri,
        sparkSchema,
        LanceSparkWriteOptions.from(uri),
        new GenericInternalRow(new Object[] {0}),
        new GenericInternalRow(new Object[] {Integer.MAX_VALUE}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(allocator).uri(uri).build();
        Scanner scanner = ds.newScan();
        ArrowReader reader = scanner.scanBatches()) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(0L, Integer.toUnsignedLong((int) root.getVector("u32").getObject(0)));
      assertEquals(
          Integer.MAX_VALUE, Integer.toUnsignedLong((int) root.getVector("u32").getObject(1)));
    }
  }

  // ==================== Type family compatibility: Timestamp, LargeList, Float16
  // ====================

  @Test
  public void testOverwritePreservesNonUtcTimestamp(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(
                        new ArrowType.Timestamp(TimeUnit.MICROSECOND, "America/New_York")),
                    null)));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Spark reads any timestamp as LongType (micros since epoch)
    InternalRow row = new GenericInternalRow(new Object[] {1, 1000000L});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    assertSchemaEquals(schema, after, "Non-UTC timestamp must be preserved after overwrite");
  }

  @Test
  public void testOverwritePreservesLargeList(TestInfo testInfo) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "tags",
                    FieldType.nullable(new ArrowType.LargeList()),
                    Collections.singletonList(
                        new Field("item", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)))));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Spark sees LargeList as ArrayType(StringType) — same as List
    InternalRow row =
        new GenericInternalRow(
            new Object[] {
              1,
              new GenericArrayData(
                  new Object[] {UTF8String.fromString("a"), UTF8String.fromString("b")})
            });
    Schema after = overwriteAndGetSchema(uri, schema, row);

    Field tagsField = after.getFields().get(1);
    assertTrue(
        tagsField.getType() instanceof ArrowType.LargeList,
        "LargeList must be preserved, got: " + tagsField.getType());
  }

  @Test
  public void testOverwritePreservesFloat16InFixedSizeList(TestInfo testInfo) throws IOException {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "embedding",
                    FieldType.nullable(new ArrowType.FixedSizeList(4)),
                    Collections.singletonList(
                        new Field(
                            "item",
                            FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
                            null)))));
    String uri = datasetUri(testInfo.getTestMethod().get().getName());
    createEmptyDataset(uri, schema);

    // Spark maps Float16 -> Float32 on read; FixedSizeList<Float16>(4) -> Array<Float>
    InternalRow row =
        new GenericInternalRow(
            new Object[] {1, new GenericArrayData(new Object[] {1.0f, 2.0f, 3.0f, 4.0f})});
    Schema after = overwriteAndGetSchema(uri, schema, row);

    Field embField = after.getFields().get(1);
    assertTrue(
        embField.getType() instanceof ArrowType.FixedSizeList,
        "FixedSizeList must be preserved, got: " + embField.getType());
    Field child = embField.getChildren().get(0);
    assertEquals(
        new ArrowType.FloatingPoint(FloatingPointPrecision.HALF),
        child.getType(),
        "Float16 child type must be preserved");
  }
}
