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
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BinaryType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests that lance datasets with FixedSizeBinary columns can be read via Spark SQL.
 *
 * <p>FixedSizeBinary is not representable in Spark's type system (Spark only has BinaryType).
 * LanceArrowUtils.fromArrowField maps FixedSizeBinary to BinaryType so that
 * LanceArrowUtils.fromArrowSchema can build a valid Spark StructType.
 *
 * <p>We create the lance dataset directly via the lance Java API (bypassing Spark DDL, which cannot
 * express FixedSizeBinary), then read back via {@code spark.read().format("lance")}.
 */
public abstract class BaseFixedSizeBinaryReadTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  private static final byte[] UUID1 = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
  private static final byte[] UUID2 = "fedcba9876543210".getBytes(StandardCharsets.UTF_8);
  private static final int BYTE_WIDTH = 16;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("fixed-size-binary-read-test")
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
   * Writes a lance dataset with a FixedSizeBinary(16) column to the given path.
   *
   * <p>We cannot use Spark DDL (CREATE TABLE / INSERT) because Spark has no FixedSizeBinary type.
   * Instead we:
   *
   * <ol>
   *   <li>Build a VectorSchemaRoot with the Arrow schema directly.
   *   <li>Serialize to an Arrow IPC stream (ByteArrayOutputStream).
   *   <li>Re-read via ArrowStreamReader and export through the C Data interface using
   *       LanceRuntime.allocator(), so that lance's JNI bridge and our buffers share the same
   *       allocator root (required by the C Data interface).
   *   <li>Pass the exported ArrowArrayStream to Dataset.write().stream().uri().execute().
   * </ol>
   */
  private static void writeLanceDatasetWithFixedSizeBinary(String datasetUri) throws Exception {
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field uuidField =
        new Field("uuid", FieldType.nullable(new ArrowType.FixedSizeBinary(BYTE_WIDTH)), null);
    Schema arrowSchema = new Schema(Arrays.asList(idField, uuidField));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      FixedSizeBinaryVector uuidVec = (FixedSizeBinaryVector) root.getVector("uuid");

      idVec.setSafe(0, 1);
      uuidVec.setSafe(0, UUID1);
      idVec.setSafe(1, 2);
      uuidVec.setSafe(1, UUID2);
      root.setRowCount(2);

      // Serialize to IPC bytes.
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      // Re-read via ArrowStreamReader and export via C Data interface.
      // Using .stream() avoids the allocator-root mismatch that occurs when the native Rust code
      // imports a Java ArrowReader directly via .reader().
      try (ArrowStreamReader reader =
              new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), allocator);
          ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, arrowStream);
        org.lance.Dataset.write().stream(arrowStream).uri(datasetUri).execute().close();
      }
    }
  }

  @Test
  public void testFixedSizeBinaryColumnSurfacesAsBinaryType() throws Exception {
    String datasetUri = tempDir.resolve("fsb_schema_test.lance").toString();
    writeLanceDatasetWithFixedSizeBinary(datasetUri);

    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(datasetUri);

    // FixedSizeBinary should surface as BinaryType in Spark schema
    assertInstanceOf(
        BinaryType.class,
        df.schema().apply("uuid").dataType(),
        "FixedSizeBinary should surface as BinaryType in Spark schema");
  }

  @Test
  public void testFixedSizeBinaryValuesAreReadCorrectly() throws Exception {
    String datasetUri = tempDir.resolve("fsb_values_test.lance").toString();
    writeLanceDatasetWithFixedSizeBinary(datasetUri);

    spark.read().format(LanceDataSource.name).load(datasetUri).createOrReplaceTempView("fsb_test");
    List<Row> rows = spark.sql("SELECT id, uuid FROM fsb_test ORDER BY id").collectAsList();

    assertEquals(2, rows.size(), "Expected 2 rows");
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(2, rows.get(1).getInt(0));
    assertArrayEquals(UUID1, (byte[]) rows.get(0).get(1));
    assertArrayEquals(UUID2, (byte[]) rows.get(1).get(1));
  }

  @Test
  public void testFixedSizeBinaryColumnProjection() throws Exception {
    // Verify that projecting only the uuid column works (schema normalization happens at load time)
    String datasetUri = tempDir.resolve("fsb_projection_test.lance").toString();
    writeLanceDatasetWithFixedSizeBinary(datasetUri);

    spark.read().format(LanceDataSource.name).load(datasetUri).createOrReplaceTempView("fsb_proj");

    List<Row> rows = spark.sql("SELECT uuid FROM fsb_proj ORDER BY uuid").collectAsList();
    assertEquals(2, rows.size(), "Expected 2 rows");
    // Both UUID byte arrays should be readable as byte[]
    assertInstanceOf(byte[].class, rows.get(0).get(0));
    assertInstanceOf(byte[].class, rows.get(1).get(0));
  }
}
