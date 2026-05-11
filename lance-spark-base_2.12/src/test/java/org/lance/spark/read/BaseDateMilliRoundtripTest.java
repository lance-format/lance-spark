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
package org.lance.spark.read;

import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceRuntime;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that Date(MILLISECOND) columns survive a Spark read-write roundtrip without being
 * downgraded to Date(DAY).
 *
 * <p>Spark only supports Date(DAY) natively, so when a Lance dataset already contains
 * Date(MILLISECOND) columns (created from PyArrow or other non-Spark sources), the connector must
 * preserve the original Arrow type on write-back.
 */
public abstract class BaseDateMilliRoundtripTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  // 2024-01-15 = 19737 days since epoch
  private static final long DATE1_MILLIS = 19737L * 86_400_000L;
  // 2024-06-30 = 19904 days since epoch
  private static final long DATE2_MILLIS = 19904L * 86_400_000L;
  // 2025-12-25 = 20447 days since epoch
  private static final long DATE3_MILLIS = 20447L * 86_400_000L;
  // Epoch boundary: 1970-01-01 = day 0
  private static final long EPOCH_MILLIS = 0L;
  // Pre-epoch: 1969-12-31 = day -1
  private static final long PRE_EPOCH_MILLIS = -1L * 86_400_000L;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("date-milli-roundtrip-test")
            .master("local[*]")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Writes a Lance dataset from an Arrow VectorSchemaRoot via IPC round-trip. Shared helper used by
   * all tests that create datasets with the Lance Java API.
   */
  private static void writeLanceDataset(String datasetUri, VectorSchemaRoot root) throws Exception {
    BufferAllocator allocator = LanceRuntime.allocator();
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

  /** Creates a Lance dataset with an id (Int32) column and a dt (Date MILLISECOND) column. */
  private static void writeDateMilliDataset(String datasetUri, int[] ids, long[] dateMillis)
      throws Exception {
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field dateField =
        new Field("dt", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
    Schema arrowSchema = new Schema(Arrays.asList(idField, dateField));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      DateMilliVector dateVec = (DateMilliVector) root.getVector("dt");

      for (int i = 0; i < ids.length; i++) {
        idVec.setSafe(i, ids[i]);
        dateVec.setSafe(i, dateMillis[i]);
      }
      root.setRowCount(ids.length);
      writeLanceDataset(datasetUri, root);
    }
  }

  /** Opens a Lance dataset and returns its Arrow schema. Caller does NOT need to close. */
  private static Schema openAndGetSchema(String path) {
    try (org.lance.Dataset dataset =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(path).build()) {
      return dataset.getSchema();
    }
  }

  @Test
  public void testDateMilliRoundtrip() throws Exception {
    String srcPath = tempDir.resolve("roundtrip_src.lance").toString();
    String dstPath = tempDir.resolve("roundtrip_dst.lance").toString();

    writeDateMilliDataset(srcPath, new int[] {1, 2}, new long[] {DATE1_MILLIS, DATE2_MILLIS});

    // Read via Spark, write back to a new Lance dataset
    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(srcPath);
    df.write().format(LanceDataSource.name).save(dstPath);

    // Verify the output Arrow schema preserves Date(MILLISECOND)
    Schema outputSchema = openAndGetSchema(dstPath);
    Field dtField = outputSchema.findField("dt");
    assertEquals(
        new ArrowType.Date(DateUnit.MILLISECOND),
        dtField.getType(),
        "Roundtrip should preserve Date(MILLISECOND)");

    // Verify values read back correctly
    List<Row> rows =
        spark.read().format(LanceDataSource.name).load(dstPath).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals(Date.valueOf("2024-01-15"), rows.get(0).getDate(rows.get(0).fieldIndex("dt")));
    assertEquals(Date.valueOf("2024-06-30"), rows.get(1).getDate(rows.get(1).fieldIndex("dt")));
  }

  @Test
  public void testNewTableDefaultsToDateDay() {
    String dstPath = tempDir.resolve("new_table_day.lance").toString();

    // Create a pure-Spark DataFrame with DateType from SQL literal
    Dataset<Row> df = spark.sql("SELECT DATE '2024-01-15' AS dt");
    df.write().format(LanceDataSource.name).save(dstPath);

    Schema outputSchema = openAndGetSchema(dstPath);
    Field dtField = outputSchema.findField("dt");
    assertEquals(
        new ArrowType.Date(DateUnit.DAY),
        dtField.getType(),
        "A new table written from Spark DateType should default to Date(DAY)");
  }

  @Test
  public void testMixedDateDayAndDateMilliColumns() throws Exception {
    String srcPath = tempDir.resolve("mixed_src.lance").toString();
    String dstPath = tempDir.resolve("mixed_dst.lance").toString();

    // Create a dataset with both Date(DAY) and Date(MILLISECOND) columns
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field dayField =
        new Field("day_dt", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
    Field milliField =
        new Field("milli_dt", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
    Schema arrowSchema = new Schema(Arrays.asList(idField, dayField, milliField));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      // DateDayVector stores days since epoch
      org.apache.arrow.vector.DateDayVector dayVec =
          (org.apache.arrow.vector.DateDayVector) root.getVector("day_dt");
      DateMilliVector milliVec = (DateMilliVector) root.getVector("milli_dt");

      idVec.setSafe(0, 1);
      dayVec.setSafe(0, 19737); // 2024-01-15
      milliVec.setSafe(0, DATE1_MILLIS);

      idVec.setSafe(1, 2);
      dayVec.setSafe(1, 19904); // 2024-06-30
      milliVec.setSafe(1, DATE2_MILLIS);

      root.setRowCount(2);
      writeLanceDataset(srcPath, root);
    }

    // Roundtrip through Spark
    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(srcPath);
    df.write().format(LanceDataSource.name).save(dstPath);

    // Verify each column retains its original type
    Schema outputSchema = openAndGetSchema(dstPath);
    assertEquals(
        new ArrowType.Date(DateUnit.DAY),
        outputSchema.findField("day_dt").getType(),
        "Date(DAY) column should stay Date(DAY)");
    assertEquals(
        new ArrowType.Date(DateUnit.MILLISECOND),
        outputSchema.findField("milli_dt").getType(),
        "Date(MILLISECOND) column should stay Date(MILLISECOND)");

    // Verify values
    List<Row> rows =
        spark.read().format(LanceDataSource.name).load(dstPath).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals(Date.valueOf("2024-01-15"), rows.get(0).getDate(rows.get(0).fieldIndex("day_dt")));
    assertEquals(
        Date.valueOf("2024-01-15"), rows.get(0).getDate(rows.get(0).fieldIndex("milli_dt")));
    assertEquals(Date.valueOf("2024-06-30"), rows.get(1).getDate(rows.get(1).fieldIndex("day_dt")));
    assertEquals(
        Date.valueOf("2024-06-30"), rows.get(1).getDate(rows.get(1).fieldIndex("milli_dt")));
  }

  @Test
  public void testDateMilliWithNulls() throws Exception {
    String srcPath = tempDir.resolve("nulls_src.lance").toString();
    String dstPath = tempDir.resolve("nulls_dst.lance").toString();

    // Create dataset with nulls: row 0 = value, row 1 = null, row 2 = value
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field dateField =
        new Field("dt", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
    Schema arrowSchema = new Schema(Arrays.asList(idField, dateField));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      DateMilliVector dateVec = (DateMilliVector) root.getVector("dt");

      idVec.setSafe(0, 1);
      dateVec.setSafe(0, DATE1_MILLIS);

      idVec.setSafe(1, 2);
      dateVec.setNull(1);

      idVec.setSafe(2, 3);
      dateVec.setSafe(2, DATE3_MILLIS);

      root.setRowCount(3);
      writeLanceDataset(srcPath, root);
    }

    // Roundtrip through Spark
    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(srcPath);
    df.write().format(LanceDataSource.name).save(dstPath);

    // Verify output schema preserves Date(MILLISECOND)
    Schema outputSchema = openAndGetSchema(dstPath);
    assertEquals(
        new ArrowType.Date(DateUnit.MILLISECOND),
        outputSchema.findField("dt").getType(),
        "Date(MILLISECOND) with nulls should be preserved");

    // Verify values and null positions
    List<Row> rows =
        spark.read().format(LanceDataSource.name).load(dstPath).orderBy("id").collectAsList();
    assertEquals(3, rows.size());
    assertEquals(Date.valueOf("2024-01-15"), rows.get(0).getDate(rows.get(0).fieldIndex("dt")));
    assertTrue(rows.get(1).isNullAt(rows.get(1).fieldIndex("dt")), "Row 1 dt should be null");
    assertEquals(Date.valueOf("2025-12-25"), rows.get(2).getDate(rows.get(2).fieldIndex("dt")));
  }

  @Test
  public void testDateMilliInStruct() throws Exception {
    String srcPath = tempDir.resolve("struct_src.lance").toString();
    String dstPath = tempDir.resolve("struct_dst.lance").toString();

    // Build schema: id INT, info STRUCT<dt: Date(MILLISECOND)>
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field dtChild =
        new Field("dt", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
    Field structField =
        new Field("info", FieldType.nullable(ArrowType.Struct.INSTANCE), Arrays.asList(dtChild));
    Schema arrowSchema = new Schema(Arrays.asList(idField, structField));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      StructVector structVec = (StructVector) root.getVector("info");
      DateMilliVector dtVec = (DateMilliVector) structVec.getChild("dt");

      idVec.setSafe(0, 1);
      dtVec.setSafe(0, DATE1_MILLIS);
      structVec.setIndexDefined(0);

      idVec.setSafe(1, 2);
      dtVec.setSafe(1, DATE2_MILLIS);
      structVec.setIndexDefined(1);

      root.setRowCount(2);
      structVec.setValueCount(2);
      writeLanceDataset(srcPath, root);
    }

    // Roundtrip through Spark
    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(srcPath);
    df.write().format(LanceDataSource.name).save(dstPath);

    // Verify the struct's child field is still Date(MILLISECOND)
    Schema outputSchema = openAndGetSchema(dstPath);
    Field infoField = outputSchema.findField("info");
    assertInstanceOf(ArrowType.Struct.class, infoField.getType());
    assertFalse(infoField.getChildren().isEmpty(), "Struct should have children");

    Field childDt = infoField.getChildren().get(0);
    assertEquals("dt", childDt.getName());
    assertEquals(
        new ArrowType.Date(DateUnit.MILLISECOND),
        childDt.getType(),
        "Struct child Date(MILLISECOND) should be preserved after roundtrip");

    // Verify values
    List<Row> rows =
        spark.read().format(LanceDataSource.name).load(dstPath).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    Row info0 = rows.get(0).getStruct(rows.get(0).fieldIndex("info"));
    Row info1 = rows.get(1).getStruct(rows.get(1).fieldIndex("info"));
    assertEquals(Date.valueOf("2024-01-15"), info0.getDate(0));
    assertEquals(Date.valueOf("2024-06-30"), info1.getDate(0));
  }

  @Test
  public void testDateMilliPreEpochRoundtrip() throws Exception {
    String srcPath = tempDir.resolve("pre_epoch_src.lance").toString();
    String dstPath = tempDir.resolve("pre_epoch_dst.lance").toString();

    writeDateMilliDataset(
        srcPath, new int[] {1, 2, 3}, new long[] {EPOCH_MILLIS, PRE_EPOCH_MILLIS, DATE1_MILLIS});

    // Read via Spark, write back to a new Lance dataset
    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(srcPath);
    df.write().format(LanceDataSource.name).save(dstPath);

    // Verify the output Arrow schema preserves Date(MILLISECOND)
    Schema outputSchema = openAndGetSchema(dstPath);
    Field dtField = outputSchema.findField("dt");
    assertEquals(
        new ArrowType.Date(DateUnit.MILLISECOND),
        dtField.getType(),
        "Pre-epoch roundtrip should preserve Date(MILLISECOND)");

    // Verify values read back correctly
    List<Row> rows =
        spark.read().format(LanceDataSource.name).load(dstPath).orderBy("id").collectAsList();
    assertEquals(3, rows.size());
    assertEquals(Date.valueOf("1970-01-01"), rows.get(0).getDate(rows.get(0).fieldIndex("dt")));
    assertEquals(Date.valueOf("1969-12-31"), rows.get(1).getDate(rows.get(1).fieldIndex("dt")));
    assertEquals(Date.valueOf("2024-01-15"), rows.get(2).getDate(rows.get(2).fieldIndex("dt")));
  }

  /**
   * Documents the known limitation: Date(MILLISECOND) inside a List degrades to Date(DAY) on
   * roundtrip because {@code LanceArrowUtils.fromArrowField} returns {@code DataType} without
   * metadata for array elements.
   */
  @Test
  public void testDateMilliInArrayDegradesToDay() throws Exception {
    String srcPath = tempDir.resolve("array_src.lance").toString();
    String dstPath = tempDir.resolve("array_dst.lance").toString();

    // Build schema: id INT, dates List<Date(MILLISECOND)>
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field dateChild =
        new Field("item", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
    Field listField =
        new Field("dates", FieldType.nullable(ArrowType.List.INSTANCE), Arrays.asList(dateChild));
    Schema arrowSchema = new Schema(Arrays.asList(idField, listField));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      ListVector listVec = (ListVector) root.getVector("dates");
      DateMilliVector dateVec = (DateMilliVector) listVec.getDataVector();

      // Row 0: [2024-01-15, 2024-06-30]
      listVec.startNewValue(0);
      dateVec.setSafe(0, DATE1_MILLIS);
      dateVec.setSafe(1, DATE2_MILLIS);
      listVec.endValue(0, 2);
      idVec.setSafe(0, 1);

      // Row 1: [2025-12-25]
      listVec.startNewValue(1);
      dateVec.setSafe(2, DATE3_MILLIS);
      listVec.endValue(1, 1);
      idVec.setSafe(1, 2);

      root.setRowCount(2);
      writeLanceDataset(srcPath, root);
    }

    // Verify source has Date(MILLISECOND) inside the list
    Schema srcSchema = openAndGetSchema(srcPath);
    Field srcList = srcSchema.findField("dates");
    assertEquals(
        new ArrowType.Date(DateUnit.MILLISECOND),
        srcList.getChildren().get(0).getType(),
        "Source list child should be Date(MILLISECOND)");

    // Roundtrip through Spark
    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(srcPath);
    df.write().format(LanceDataSource.name).save(dstPath);

    // Known limitation: list child degrades to Date(DAY)
    Schema outputSchema = openAndGetSchema(dstPath);
    Field outList = outputSchema.findField("dates");
    assertEquals(
        new ArrowType.Date(DateUnit.DAY),
        outList.getChildren().get(0).getType(),
        "Known limitation: Date(MILLISECOND) inside List degrades to Date(DAY) on roundtrip");

    // Values should still be correct (dates are the right day values)
    List<Row> rows =
        spark.read().format(LanceDataSource.name).load(dstPath).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    List<Date> dates0 = rows.get(0).getList(rows.get(0).fieldIndex("dates"));
    assertEquals(2, dates0.size());
    assertEquals(Date.valueOf("2024-01-15"), dates0.get(0));
    assertEquals(Date.valueOf("2024-06-30"), dates0.get(1));
    List<Date> dates1 = rows.get(1).getList(rows.get(1).fieldIndex("dates"));
    assertEquals(1, dates1.size());
    assertEquals(Date.valueOf("2025-12-25"), dates1.get(0));
  }
}
