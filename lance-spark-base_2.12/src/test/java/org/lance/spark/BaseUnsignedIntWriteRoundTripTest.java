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

import org.lance.Dataset;
import org.lance.WriteParams;

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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Spark cannot create unsigned columns, so these tests create their tables with the Lance API. */
public abstract class BaseUnsignedIntWriteRoundTripTest {

  protected static SparkSession spark;

  @TempDir protected static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("unsigned-int-write-roundtrip-test")
            .master("local[*]")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.unsigned_ns", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.unsigned_ns.impl", "dir")
            .config("spark.sql.catalog.unsigned_ns.root", tempDir.toString())
            .config("spark.sql.catalog.unsigned_ns.single_level_ns", "true")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  protected static Field unsignedField(String name, int bitWidth) {
    return new Field(
        name, new FieldType(true, new ArrowType.Int(bitWidth, false), null, null), null);
  }

  /** Creates a Lance dataset with an unsigned column of every width plus a signed control. */
  protected static void createUnsignedDataset(String datasetUri) throws Exception {
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

      // uint8/16/32 hold their maximum, which reads back negative if taken as signed.
      id.setSafe(0, 1);
      u8.setSafe(0, 255);
      u16.setSafe(0, 65535);
      u32.setSafe(0, (int) 4294967295L);
      u64.setSafe(0, 9000000000L);
      root.setRowCount(1);
      writeDataset(datasetUri, root);
    }
  }

  /** Writes {@code root} to a new Lance dataset through the Arrow C Data interface. */
  protected static void writeDataset(String datasetUri, VectorSchemaRoot root) throws Exception {
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
      Dataset.write().stream(arrowStream).uri(datasetUri).execute().close();
    }
  }

  protected static Dataset open(String datasetUri) {
    return Dataset.open().allocator(LanceRuntime.allocator()).uri(datasetUri).build();
  }

  protected static void assertIntType(Schema schema, String name, int width, boolean signed) {
    Field field = schema.findField(name);
    assertEquals(new ArrowType.Int(width, signed), field.getType(), name);
    assertTrue(field.getMetadata().isEmpty(), name + " metadata " + field.getMetadata());
  }

  protected static void assertUnsignedColumns(String datasetUri) {
    try (Dataset dataset = open(datasetUri)) {
      Schema schema = dataset.getSchema();
      assertIntType(schema, "u8", 8, false);
      assertIntType(schema, "u16", 16, false);
      assertIntType(schema, "u32", 32, false);
      assertIntType(schema, "u64", 64, false);
      assertIntType(schema, "id", 32, true);
    }
  }

  @Test
  public void testNewTableFollowsSparkTypes() throws Exception {
    String sourceUri = tempDir.resolve("uint_new_table_source.lance").toString();
    String targetUri = tempDir.resolve("uint_new_table_target.lance").toString();
    createUnsignedDataset(sourceUri);

    spark
        .read()
        .format(LanceDataSource.name)
        .load(sourceUri)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.ErrorIfExists)
        .save(targetUri);

    try (Dataset dataset = open(targetUri)) {
      Schema written = dataset.getSchema();
      assertIntType(written, "u8", 16, true);
      assertIntType(written, "u16", 32, true);
      assertIntType(written, "u32", 64, true);
      assertIntType(written, "u64", 64, true);
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

    String source = "lance.`" + sourceUri + "`";
    String[] queries = {
      "SELECT u32 FROM " + source + " UNION ALL SELECT -5L",
      "SELECT -5L AS u32 UNION ALL SELECT u32 FROM " + source
    };
    for (int i = 0; i < queries.length; i++) {
      String unionUri = tempDir.resolve("uint_union_" + i + ".lance").toString();
      spark.sql("CREATE TABLE lance.`" + unionUri + "` AS " + queries[i]);
      try (Dataset dataset = open(unionUri)) {
        assertIntType(dataset.getSchema(), "u32", 64, true);
        assertEquals(2L, dataset.countRows());
      }
    }

    String replaceUri = tempDir.resolve("uint_replace.lance").toString();
    createUnsignedDataset(replaceUri);
    spark.sql("REPLACE TABLE lance.`" + replaceUri + "` AS SELECT * FROM " + source);
    try (Dataset dataset = open(replaceUri)) {
      assertIntType(dataset.getSchema(), "u8", 16, true);
      assertIntType(dataset.getSchema(), "u32", 64, true);
    }
  }

  @Test
  public void testInsertOverwriteKeepsUnsignedTypes() throws Exception {
    String datasetUri = tempDir.resolve("uint_overwrite.lance").toString();
    createUnsignedDataset(datasetUri);

    spark.sql(
        "INSERT OVERWRITE lance.`"
            + datasetUri
            + "` VALUES (2, 200, 40000, 3000000000, 5000000000)");

    assertUnsignedColumns(datasetUri);
    List<Row> rows =
        spark
            .read()
            .format(LanceDataSource.name)
            .load(datasetUri)
            .selectExpr("id", "u8", "u16", "u32", "u64")
            .collectAsList();
    assertEquals(1, rows.size());
    assertEquals(RowFactory.create(2, (short) 200, 40000, 3000000000L, 5000000000L), rows.get(0));
  }

  @Test
  public void testUpdateColumnsKeepsUnsignedTypes() throws Exception {
    String datasetUri = tempDir.resolve("uint_update_columns.lance").toString();
    createUnsignedDataset(datasetUri);
    String table = "lance.`" + datasetUri + "`";

    spark
        .sql("SELECT _rowaddr, _fragid, CAST(7 AS SMALLINT) AS u8 FROM " + table)
        .createOrReplaceTempView("uint_update_source");
    spark.sql("ALTER TABLE " + table + " UPDATE COLUMNS u8 FROM uint_update_source");

    assertUnsignedColumns(datasetUri);
    assertEquals(
        (short) 7,
        spark
            .read()
            .format(LanceDataSource.name)
            .load(datasetUri)
            .selectExpr("u8")
            .first()
            .getShort(0));
  }

  @Test
  public void testAddColumnsKeepsUnsignedTypes() throws Exception {
    String datasetUri = tempDir.resolve("uint_add_columns.lance").toString();
    createUnsignedDataset(datasetUri);
    String table = "lance.`" + datasetUri + "`";

    spark
        .sql("SELECT _rowaddr, _fragid, id * 10 AS extra FROM " + table)
        .createOrReplaceTempView("uint_add_source");
    spark.sql("ALTER TABLE " + table + " ADD COLUMNS extra FROM uint_add_source");

    assertUnsignedColumns(datasetUri);
    try (Dataset dataset = open(datasetUri)) {
      assertIntType(dataset.getSchema(), "extra", 32, true);
    }
    assertEquals(
        RowFactory.create(1, (short) 255, 65535, 4294967295L, 9000000000L, 10),
        spark
            .read()
            .format(LanceDataSource.name)
            .load(datasetUri)
            .selectExpr("id", "u8", "u16", "u32", "u64", "extra")
            .first());
  }

  @Test
  public void testRangeBTreeIndexOnUnsignedColumns() throws Exception {
    createUnsignedDataset(tempDir.resolve("uint_index.lance").toString());
    String table = "unsigned_ns.default.uint_index";
    for (String column : new String[] {"u8", "u32"}) {
      spark.sql(
          "ALTER TABLE "
              + table
              + " CREATE INDEX "
              + column
              + "_idx USING btree ("
              + column
              + ") WITH (build_mode = 'range')");
    }

    List<Row> expected = Collections.singletonList(RowFactory.create(1));
    assertEquals(
        expected, spark.sql("SELECT id FROM " + table + " WHERE u8 = 255").collectAsList());
    assertEquals(
        expected,
        spark.sql("SELECT id FROM " + table + " WHERE u32 >= 3000000000").collectAsList());

    // Values of 2^63 or more read as negative; the index must still sort them after small values.
    String uint64Uri = tempDir.resolve("uint64_index.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                unsignedField("u64", 64)));
    Dataset.write()
        .allocator(LanceRuntime.allocator())
        .uri(uint64Uri)
        .schema(schema)
        .mode(WriteParams.WriteMode.CREATE)
        .execute()
        .close();
    StructType uint64Schema = spark.read().format(LanceDataSource.name).load(uint64Uri).schema();
    // One partition, so all four values land in one fragment and one index page.
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, 1L),
                RowFactory.create(2, 2L),
                RowFactory.create(3, Long.MIN_VALUE + 1),
                RowFactory.create(4, -1L)),
            uint64Schema)
        .coalesce(1)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.Append)
        .save(uint64Uri);
    String uint64Table = "unsigned_ns.default.uint64_index";
    spark.sql(
        "ALTER TABLE "
            + uint64Table
            + " CREATE INDEX u64_idx USING btree (u64) WITH (build_mode = 'range')");
    assertEquals(
        Collections.singletonList(RowFactory.create(2)),
        spark.sql("SELECT id FROM " + uint64Table + " WHERE u64 = 2").collectAsList());
  }

  @Test
  public void testWidenedColumnCanBeRewrittenAfterReload() throws Exception {
    String sourceUri = tempDir.resolve("uint_widened_source.lance").toString();
    String targetUri = tempDir.resolve("uint_widened_target.lance").toString();
    createUnsignedDataset(sourceUri);

    StructType widened =
        new StructType().add("id", DataTypes.IntegerType).add("u8", DataTypes.IntegerType);
    spark
        .read()
        .format(LanceDataSource.name)
        .load(sourceUri)
        .select("id", "u8")
        .to(widened)
        .write()
        .format(LanceDataSource.name)
        .save(targetUri);
    spark
        .read()
        .format(LanceDataSource.name)
        .load(targetUri)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.Append)
        .save(targetUri);

    try (Dataset dataset = open(targetUri)) {
      assertIntType(dataset.getSchema(), "u8", 32, true);
      assertEquals(2L, dataset.countRows());
    }
  }

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

    assertUnsignedColumns(datasetUri);
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

  @Test
  public void testInsertKeepsNestedUnsignedTypes() throws Exception {
    String datasetUri = tempDir.resolve("uint_nested.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "s",
                    FieldType.nullable(ArrowType.Struct.INSTANCE),
                    Collections.singletonList(unsignedField("u", 32))),
                new Field(
                    "a",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(unsignedField("item", 8)))));
    Dataset.write()
        .allocator(LanceRuntime.allocator())
        .uri(datasetUri)
        .schema(schema)
        .mode(WriteParams.WriteMode.CREATE)
        .execute()
        .close();

    spark.sql(
        "INSERT INTO lance.`"
            + datasetUri
            + "` VALUES (1, named_struct('u', 4294967295L), array(255S, NULL))");

    try (Dataset dataset = open(datasetUri)) {
      assertEquals(schema, dataset.getSchema());
    }
    Row row = spark.read().format(LanceDataSource.name).load(datasetUri).first();
    assertEquals(4294967295L, row.getStruct(1).getLong(0));
    assertEquals(Arrays.asList((short) 255, null), row.getList(2));
  }

  @Test
  public void testOutOfRangeUnsignedValuesAreRejected() throws Exception {
    String datasetUri = tempDir.resolve("uint_range.lance").toString();
    createUnsignedDataset(datasetUri);
    StructType schema = spark.read().format(LanceDataSource.name).load(datasetUri).schema();

    Object[][] appends = {
      {RowFactory.create(2, (short) 256, 0, 0L, 0L), "[0, 255], got 256"},
      {RowFactory.create(2, (short) 0, 65536, 0L, 0L), "[0, 65535], got 65536"},
      {RowFactory.create(2, (short) 0, 0, 4294967296L, 0L), "[0, 4294967295], got 4294967296"},
      {RowFactory.create(2, (short) -1, 0, 0L, 0L), "[0, 255], got -1"}
    };
    for (Object[] append : appends) {
      assertWriteRejected(
          datasetUri,
          () ->
              spark
                  .createDataFrame(Collections.singletonList((Row) append[0]), schema)
                  .write()
                  .format(LanceDataSource.name)
                  .mode(SaveMode.Append)
                  .save(datasetUri),
          (String) append[1]);
    }
    assertWriteRejected(
        datasetUri,
        () -> spark.sql("INSERT OVERWRITE lance.`" + datasetUri + "` VALUES (2, 300, 0, 0, 0)"),
        "[0, 255], got 300");
  }

  /** Asserts that {@code write} fails with {@code expected} and leaves the table as it was. */
  private void assertWriteRejected(String datasetUri, Executable write, String expected) {
    long version;
    Schema schema;
    try (Dataset dataset = open(datasetUri)) {
      version = dataset.version();
      schema = dataset.getSchema();
    }
    List<Row> rows =
        spark.read().format(LanceDataSource.name).load(datasetUri).orderBy("id").collectAsList();

    Exception e = assertThrows(Exception.class, write);
    boolean found = false;
    for (Throwable t = e; t != null && !found; t = t.getCause()) {
      found = t.getMessage() != null && t.getMessage().contains(expected);
    }
    assertTrue(found, "expected \"" + expected + "\" in the failure chain of " + e);

    try (Dataset dataset = open(datasetUri)) {
      assertEquals(version, dataset.version());
      assertEquals(schema, dataset.getSchema());
    }
    assertEquals(
        rows,
        spark.read().format(LanceDataSource.name).load(datasetUri).orderBy("id").collectAsList());
  }
}
