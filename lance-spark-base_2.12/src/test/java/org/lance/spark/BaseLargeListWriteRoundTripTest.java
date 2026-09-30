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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips an Arrow LargeList (64-bit offset) column through a Spark read then write.
 *
 * <p>Spark's ArrayType records no offset width, so {@code LanceArrowUtils.fromArrowField} marks a
 * LargeList column in field metadata and {@code toArrowField} restores it on write. That restored
 * schema still needs a {@code LargeListVector} field writer in {@code LanceArrowWriter} or the
 * batch fails at writer initialization. A producer must go through the Arrow C Data interface to
 * create such a dataset, exactly as {@code BaseFixedSizeBinaryReadTest} does for a type Spark
 * cannot express either.
 */
public abstract class BaseLargeListWriteRoundTripTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("large-list-write-roundtrip-test")
            .master("local[*]")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static Field intItem() {
    return new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  /**
   * Creates a Lance dataset with a LargeList column plus an ordinary List control, so the marker is
   * shown to reproduce LargeList without widening every array column.
   */
  private static void createLargeListDataset(String datasetUri) throws Exception {
    Schema arrowSchema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "biglist",
                    FieldType.nullable(new ArrowType.LargeList()),
                    Collections.singletonList(intItem())),
                new Field(
                    "smalllist",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(intItem()))));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector id = (IntVector) root.getVector("id");
      LargeListVector big = (LargeListVector) root.getVector("biglist");
      ListVector small = (ListVector) root.getVector("smalllist");
      IntVector bigElems = (IntVector) big.getDataVector();
      IntVector smallElems = (IntVector) small.getDataVector();

      id.setSafe(0, 1);
      big.startNewValue(0);
      bigElems.setSafe(0, 10);
      bigElems.setSafe(1, 20);
      bigElems.setSafe(2, 30);
      big.endValue(0, 3);
      small.startNewValue(0);
      smallElems.setSafe(0, 1);
      smallElems.setSafe(1, 2);
      small.endValue(0, 2);
      bigElems.setValueCount(3);
      smallElems.setValueCount(2);
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

  /**
   * A read -&gt; write cycle must keep the LargeList column a LargeList and leave an ordinary List
   * column a List. Without the {@code LargeListVector} writer this throws {@code
   * UnsupportedOperationException} at batch writer initialization.
   */
  @Test
  public void testLargeListRoundTripsThroughSparkWrite() throws Exception {
    String sourceUri = tempDir.resolve("large_list_source.lance").toString();
    String targetUri = tempDir.resolve("large_list_target.lance").toString();
    createLargeListDataset(sourceUri);

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
      assertTrue(
          written.findField("biglist").getType() instanceof ArrowType.LargeList,
          () -> "biglist should stay a LargeList, got " + written.findField("biglist").getType());
      // The plain List control must not be widened by the marker.
      assertTrue(
          written.findField("smalllist").getType() instanceof ArrowType.List,
          () -> "smalllist should stay a List, got " + written.findField("smalllist").getType());
    }

    List<Row> rows =
        spark
            .read()
            .format(LanceDataSource.name)
            .load(targetUri)
            .selectExpr(
                "size(biglist)", "biglist[0]", "biglist[2]", "size(smalllist)", "smalllist[0]")
            .collectAsList();
    assertEquals(1, rows.size());
    assertEquals(3, rows.get(0).getInt(0));
    assertEquals(10, rows.get(0).getInt(1));
    assertEquals(30, rows.get(0).getInt(2));
    assertEquals(2, rows.get(0).getInt(3));
    assertEquals(1, rows.get(0).getInt(4));
  }

  /**
   * Appending through Spark to an existing LargeList Lance table must succeed: the writer restores
   * the LargeList type so the batch passes Lance's schema validation against the existing column
   * instead of presenting a 32-bit List.
   */
  @Test
  public void testAppendToExistingLargeListTable() throws Exception {
    String datasetUri = tempDir.resolve("large_list_append.lance").toString();
    createLargeListDataset(datasetUri);

    StructType readSchema = spark.read().format(LanceDataSource.name).load(datasetUri).schema();
    Row newRow = RowFactory.create(2, Arrays.asList(40, 50), Arrays.asList(3));
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
            .selectExpr("size(biglist)", "biglist[0]")
            .collectAsList();
    assertEquals(2, rows.size(), "the appended row should be present");
    assertEquals(3, rows.get(0).getInt(0));
    assertEquals(10, rows.get(0).getInt(1));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals(40, rows.get(1).getInt(1));
  }
}
