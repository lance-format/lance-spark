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
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@code arrow_batch_reads} read option ({@link
 * LanceSparkReadOptions#CONFIG_ARROW_BATCH_READS}): each test reads a dataset with and without
 * Arrow batches and compares the rows, and checks the batches through the scan's own columnar
 * reader.
 */
public abstract class BaseArrowBatchReadTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder().appName("arrow-batch-read-test").master("local[*]").getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static Metadata vectorMetadata(int size) {
    return new MetadataBuilder().putLong("arrow.fixed-size-list.size", size).build();
  }

  /**
   * Writes 20 rows of vector, struct, list, and map columns through Spark. Row {@code i} has a null
   * {@code vec} when {@code i % 5 == 3}, a null {@code doc} when {@code i % 5 == 4}, and an empty
   * {@code multi} when {@code i % 3 == 0}.
   */
  private static String writeSparkDataset(String name) {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("name", DataTypes.StringType)
            .add(
                "vec",
                DataTypes.createArrayType(DataTypes.FloatType, false),
                true,
                vectorMetadata(4))
            .add(
                "doc",
                new StructType()
                    .add("label", DataTypes.StringType)
                    .add(
                        "emb",
                        DataTypes.createArrayType(DataTypes.DoubleType, false),
                        true,
                        vectorMetadata(2)))
            .add(
                "multi",
                DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.FloatType, false)),
                true,
                new MetadataBuilder().putString("_lance.element", vectorMetadata(2).json()).build())
            .add("tags", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      float f = i;
      List<List<Float>> multi = new ArrayList<>();
      for (int j = 0; j < i % 3; j++) {
        multi.add(Arrays.asList(f * 10 + j, f * 10 + j + 0.5f));
      }
      Map<String, Integer> tags = new HashMap<>();
      tags.put("k" + i, i);
      rows.add(
          RowFactory.create(
              i,
              i % 4 == 0 ? null : "n" + i,
              i % 5 == 3 ? null : Arrays.asList(f, f + 1, f + 2, f + 3),
              i % 5 == 4 ? null : RowFactory.create("d" + i, Arrays.asList(i * 2.0, i * 2.0 + 1)),
              multi,
              tags));
    }
    String uri = tempDir.resolve(name + ".lance").toString();
    spark.createDataFrame(rows, schema).write().format(LanceDataSource.name).save(uri);
    return uri;
  }

  private static Dataset<Row> read(String uri, boolean arrowBatches) {
    return spark
        .read()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_ARROW_BATCH_READS, String.valueOf(arrowBatches))
        .load(uri);
  }

  /** Asserts that {@code query} over {@code uri} returns the same rows with Arrow batches. */
  private static void assertSameRows(String uri, String query) {
    assertSameRows(uri, df -> spark.sql(query), query);
  }

  /** Asserts that {@code query} over {@code uri} returns the same rows with Arrow batches. */
  private static void assertSameRows(
      String uri, Function<Dataset<Row>, Dataset<Row>> query, String description) {
    Dataset<Row> withoutArrowBatches = read(uri, false);
    withoutArrowBatches.createOrReplaceTempView("t");
    List<Row> expected = query.apply(withoutArrowBatches).collectAsList();
    Dataset<Row> withArrowBatches = read(uri, true);
    withArrowBatches.createOrReplaceTempView("t");
    List<Row> actual = query.apply(withArrowBatches).collectAsList();
    assertEquals(expected, actual, description);
  }

  /** The {@link LanceScan} of {@code df}'s executed plan. */
  private static LanceScan lanceScan(Dataset<Row> df) {
    LanceScan scan = findLanceScan(df.queryExecution().executedPlan());
    assertTrue(scan != null, "No LanceScan in\n" + df.queryExecution().executedPlan());
    return scan;
  }

  private static LanceScan findLanceScan(SparkPlan plan) {
    if (plan instanceof AdaptiveSparkPlanExec) {
      return findLanceScan(((AdaptiveSparkPlanExec) plan).executedPlan());
    }
    if (plan instanceof BatchScanExec && ((BatchScanExec) plan).scan() instanceof LanceScan) {
      return (LanceScan) ((BatchScanExec) plan).scan();
    }
    for (int i = 0; i < plan.children().size(); i++) {
      LanceScan scan = findLanceScan((SparkPlan) plan.children().apply(i));
      if (scan != null) {
        return scan;
      }
    }
    return null;
  }

  /** Reads every batch of {@code scan} through its columnar reader. */
  private static void forEachBatch(LanceScan scan, Consumer<ColumnarBatch> consumer)
      throws Exception {
    for (InputPartition partition : scan.planInputPartitions()) {
      try (PartitionReader<ColumnarBatch> reader =
          scan.createReaderFactory().createColumnarReader(partition)) {
        while (reader.next()) {
          consumer.accept(reader.get());
        }
      }
    }
  }

  /**
   * Passes the Arrow vectors of every batch to {@code consumer} while the batch is valid, checking
   * that every column is a Spark Arrow column whose buffers come from Spark's root allocator.
   * Returns the number of batches.
   */
  private static int forEachArrowBatch(LanceScan scan, Consumer<List<FieldVector>> consumer)
      throws Exception {
    int[] batches = {0};
    forEachBatch(
        scan,
        batch -> {
          List<FieldVector> vectors = new ArrayList<>();
          for (int i = 0; i < batch.numCols(); i++) {
            ColumnVector column = batch.column(i);
            assertSame(ArrowColumnVector.class, column.getClass());
            FieldVector vector = (FieldVector) ((ArrowColumnVector) column).getValueVector();
            assertSame(ArrowUtils.rootAllocator(), vector.getAllocator().getRoot());
            vectors.add(vector);
          }
          consumer.accept(vectors);
          batches[0]++;
        });
    return batches[0];
  }

  @Test
  public void readsVectorStructListAndMapColumnsAsArrowBatches() throws Exception {
    String uri = writeSparkDataset("columns");
    assertSameRows(uri, "select * from t order by id");
    assertSameRows(uri, "select id, vec[1], doc.emb, multi from t where id % 2 = 0 order by id");

    LanceScan scan = lanceScan(read(uri, true));
    assertTrue(scan.readOptions().isArrowBatchReadsEnabled());
    Schema schema = scan.arrowBatchSchema().get();
    assertEquals(ArrowType.List.INSTANCE, schema.findField("vec").getType());
    assertEquals(
        ArrowType.List.INSTANCE,
        schema.findField("doc").getChildren().get(1).getType(),
        "the vector inside the struct");
    assertEquals(
        ArrowType.List.INSTANCE,
        schema.findField("multi").getChildren().get(0).getType(),
        "the vectors inside the list");

    int batches =
        forEachArrowBatch(
            scan,
            batch -> {
              assertEquals(schema, new Schema(fieldsOf(batch)));
              assertInstanceOf(ListVector.class, batch.get(2));
            });
    assertTrue(batches > 0);
  }

  @Test
  public void readsAsUsualWithoutTheOption() {
    String uri = writeSparkDataset("default");
    LanceScan scan = lanceScan(read(uri, false));
    assertFalse(scan.readOptions().isArrowBatchReadsEnabled());
    // The plan is there for a consumer that turns Arrow batches on (withArrowBatches).
    assertTrue(scan.arrowBatchSchema().isPresent());
  }

  @Test
  public void readsMetadataColumnsAsArrowBatches() throws Exception {
    String uri =
        TestUtils.getDatasetUri(
            TestUtils.TestTable1Config.dbPath, TestUtils.TestTable1Config.datasetName);
    // Metadata columns resolve through the DataFrame API.
    Function<Dataset<Row>, Dataset<Row>> query =
        df ->
            df.select(
                    "x",
                    "_rowid",
                    "_rowaddr",
                    "_fragid",
                    "_row_created_at_version",
                    "_row_last_updated_at_version")
                .orderBy("x");
    assertSameRows(uri, query, "metadata columns");

    LanceScan scan = lanceScan(query.apply(read(uri, true)));
    StructType readSchema = scan.readSchema();
    int batches =
        forEachArrowBatch(
            scan,
            batch -> {
              for (String column :
                  Arrays.asList(
                      "_rowid",
                      "_rowaddr",
                      "_row_created_at_version",
                      "_row_last_updated_at_version")) {
                assertInstanceOf(BigIntVector.class, batch.get(readSchema.fieldIndex(column)));
              }
              assertInstanceOf(IntVector.class, batch.get(readSchema.fieldIndex("_fragid")));
            });
    assertTrue(batches > 0);
  }

  /** Columns without an Arrow batch form make the scan read as usual. */
  @Test
  public void readsAsUsualWithoutAnArrowBatchForm() throws Exception {
    String uri = tempDir.resolve("no-arrow-form.lance").toString();
    writeArrowDataset(uri, false);
    assertSameRows(uri, "select id, hex(fsb) from t order by id");
    assertSameRows(uri, "select id, u from t order by id");

    read(uri, true).createOrReplaceTempView("t");
    for (String column : Arrays.asList("fsb", "u")) {
      LanceScan scan = lanceScan(spark.sql("select id, " + column + " from t"));
      assertFalse(scan.readOptions().isArrowBatchReadsEnabled());
      assertFalse(scan.arrowBatchSchema().isPresent());
      assertTrue(
          scan.arrowBatchUnsupportedReason().get().contains("column " + column),
          scan.arrowBatchUnsupportedReason().get());
      assertThrows(IllegalStateException.class, scan::withArrowBatches);
      forEachBatch(
          scan, batch -> assertNotEquals(ArrowColumnVector.class, batch.column(1).getClass()));
    }
  }

  /**
   * A FixedSizeList reads as a Spark array whatever its element type, so integer vectors and
   * vectors of vectors are read as Lists like float vectors.
   */
  @Test
  public void readsVectorsOfAnyElementTypeAsArrowBatches() throws Exception {
    String uri = tempDir.resolve("any-vectors.lance").toString();
    writeArrowDataset(uri, true);
    assertSameRows(uri, "select id, i8, nested from t order by id");

    read(uri, true).createOrReplaceTempView("t");
    LanceScan scan = lanceScan(spark.sql("select id, i8, nested from t"));
    Schema schema = scan.arrowBatchSchema().get();
    assertEquals(ArrowType.List.INSTANCE, schema.findField("i8").getType());
    assertEquals(
        ArrowType.List.INSTANCE, schema.findField("nested").getChildren().get(0).getType());
    int batches =
        forEachArrowBatch(
            scan,
            batch -> {
              assertInstanceOf(ListVector.class, batch.get(1));
              assertInstanceOf(ListVector.class, ((ListVector) batch.get(2)).getDataVector());
            });
    assertTrue(batches > 0);
  }

  /**
   * Selecting a struct field reads the whole struct, which Spark then projects, so the Arrow struct
   * lines up with the declared one and the rows match.
   */
  @Test
  public void readsStructFieldsThroughTheWholeStruct() {
    String uri = writeSparkDataset("struct-fields");
    read(uri, true).createOrReplaceTempView("t");
    LanceScan scan = lanceScan(spark.sql("select id, doc.emb from t"));
    assertEquals(2, ((StructType) scan.readSchema().apply("doc").dataType()).size());
    assertTrue(scan.arrowBatchSchema().isPresent());
    assertSameRows(uri, "select id, doc.emb from t order by id");
    assertSameRows(uri, "select id, doc.label, vec from t order by id");
  }

  @Test
  public void withArrowBatchesTurnsArrowBatchesOnForAPlannedScan() throws Exception {
    String uri = writeSparkDataset("planned");
    LanceScan scan = lanceScan(read(uri, false));
    int partitions = scan.planInputPartitions().length;
    LanceScan arrowScan = scan.withArrowBatches();
    // The copy reports the planned partitioning without being planned again.
    assertEquals(partitions, arrowScan.outputPartitioning().numPartitions());
    assertTrue(arrowScan.readOptions().isArrowBatchReadsEnabled());
    assertNotEquals(scan, arrowScan);
    assertSame(arrowScan, arrowScan.withArrowBatches());
    int[] rows = {0};
    forEachArrowBatch(arrowScan, batch -> rows[0] += batch.get(0).getValueCount());
    assertEquals(20, rows[0]);
  }

  /**
   * Reads many fragments in many batches, and checks that the vectors an Arrow batch adds (here the
   * vector and {@code _fragid} columns) are released once the reader moves on and closes.
   */
  @Test
  public void releasesArrowBatchVectorsAcrossBatchesAndFragments() throws Exception {
    String uri = writeSparkDataset("batches-source");
    String many = tempDir.resolve("many.lance").toString();
    Dataset<Row> source = spark.read().format(LanceDataSource.name).load(uri);
    // One writing task, so the fragments hold 13 rows each and are read in several batches.
    source
        .union(source)
        .union(source)
        .coalesce(1)
        .write()
        .format(LanceDataSource.name)
        .option("max_row_per_file", "13")
        .save(many);
    Dataset<Row> df =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_ARROW_BATCH_READS, "true")
            .option(LanceSparkReadOptions.CONFIG_BATCH_SIZE, "4")
            .load(many)
            .select("id", "vec", "_fragid");
    LanceScan scan = lanceScan(df);

    // Arrow batches are allocated from Spark's root allocator.
    BufferAllocator allocator = ArrowUtils.rootAllocator();
    long before = allocator.getAllocatedMemory();
    int[] batches = {0};
    int[] rows = {0};
    FieldVector[] previousFragmentIds = {null};
    forEachBatch(
        scan,
        batch -> {
          // The _fragid vector of the previous batch is released once this one is loaded.
          if (previousFragmentIds[0] != null) {
            assertEquals(0, previousFragmentIds[0].getBufferSize());
          }
          FieldVector fragmentIds =
              (FieldVector) ((ArrowColumnVector) batch.column(2)).getValueVector();
          assertEquals(batch.numRows(), fragmentIds.getValueCount());
          previousFragmentIds[0] = fragmentIds;
          batches[0]++;
          rows[0] += batch.numRows();
        });
    assertEquals(before, allocator.getAllocatedMemory());
    assertEquals(60, rows[0]);
    assertTrue(batches[0] > scan.planInputPartitions().length, "expected several batches");
  }

  private static List<Field> fieldsOf(List<FieldVector> vectors) {
    List<Field> fields = new ArrayList<>(vectors.size());
    for (FieldVector vector : vectors) {
      fields.add(vector.getField());
    }
    return fields;
  }

  private static Field element(ArrowType type) {
    return new Field("item", FieldType.notNullable(type), null);
  }

  private static Field vector(String name, int size, Field element) {
    return new Field(
        name,
        FieldType.nullable(new ArrowType.FixedSizeList(size)),
        Collections.singletonList(element));
  }

  /**
   * Writes 3 rows through lance-core's Arrow API: an int8 vector {@code i8} and a vector of float
   * vectors {@code nested} if {@code vectors}, else a FixedSizeBinary {@code fsb} and an unsigned
   * {@code u}, which have no Arrow batch form.
   */
  private static void writeArrowDataset(String uri, boolean vectors) throws Exception {
    Field id = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    List<Field> fields =
        vectors
            ? Arrays.asList(
                id,
                vector("i8", 4, element(new ArrowType.Int(8, true))),
                vector(
                    "nested",
                    2,
                    vector(
                        "item",
                        2,
                        element(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))))
            : Arrays.asList(
                id,
                new Field("fsb", FieldType.nullable(new ArrowType.FixedSizeBinary(4)), null),
                new Field("u", FieldType.nullable(new ArrowType.Int(32, false)), null));
    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator)) {
      root.allocateNew();
      for (int r = 0; r < 3; r++) {
        ((IntVector) root.getVector("id")).setSafe(r, r);
        if (vectors) {
          FixedSizeListVector i8 = (FixedSizeListVector) root.getVector("i8");
          FixedSizeListVector nested = (FixedSizeListVector) root.getVector("nested");
          FixedSizeListVector inner = (FixedSizeListVector) nested.getDataVector();
          i8.setNotNull(r);
          nested.setNotNull(r);
          for (int j = 0; j < 4; j++) {
            ((TinyIntVector) i8.getDataVector()).setSafe(r * 4 + j, r - j);
          }
          for (int j = 0; j < 2; j++) {
            inner.setNotNull(r * 2 + j);
            ((Float4Vector) inner.getDataVector()).setSafe((r * 2 + j) * 2, r + j);
            ((Float4Vector) inner.getDataVector()).setSafe((r * 2 + j) * 2 + 1, r + j + 0.5f);
          }
        } else {
          ((FixedSizeBinaryVector) root.getVector("fsb"))
              .setSafe(r, new byte[] {(byte) r, 1, 2, 3});
          ((UInt4Vector) root.getVector("u")).setSafe(r, 4_000_000 + r);
        }
      }
      root.setRowCount(3);
      ByteArrayOutputStream ipc = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, ipc)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      try (ArrowStreamReader reader =
              new ArrowStreamReader(new ByteArrayInputStream(ipc.toByteArray()), allocator);
          ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, stream);
        org.lance.Dataset.write().stream(stream).uri(uri).execute().close();
      }
    }
  }
}
