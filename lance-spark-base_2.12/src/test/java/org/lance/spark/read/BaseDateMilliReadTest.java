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
import org.apache.spark.sql.types.DataTypes;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests that lance datasets with Date(MILLISECOND) columns can be read via Spark SQL.
 *
 * <p>Spark only supports Date(DAY) natively. Date(MILLISECOND) can appear in Lance datasets created
 * from non-Spark sources (e.g., Python PyArrow with pa.date64()). This test verifies that the
 * schema conversion and vectorized read path handle this correctly.
 *
 * <p>We create the lance dataset directly via the lance Java API (bypassing Spark DDL, which always
 * writes Date(DAY)), then read back via {@code spark.read().format("lance")}.
 */
public abstract class BaseDateMilliReadTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  // 2024-01-15 = 19737 days since epoch
  private static final long DATE1_MILLIS = 19737L * 86_400_000L;
  // 2024-06-30 = 19904 days since epoch
  private static final long DATE2_MILLIS = 19904L * 86_400_000L;

  @BeforeAll
  static void setup() {
    spark = SparkSession.builder().appName("date-milli-read-test").master("local[*]").getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Writes a lance dataset with a Date(MILLISECOND) column to the given path.
   *
   * <p>Spark DDL always writes Date(DAY), so we use the Arrow/Lance Java API directly.
   */
  private static void writeLanceDatasetWithDateMilli(String datasetUri) throws Exception {
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
      dateVec.setSafe(1, DATE2_MILLIS);
      root.setRowCount(2);

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

  @Test
  public void testDateMilliColumnSurfacesAsDateType() throws Exception {
    String datasetUri = tempDir.resolve("date_milli_schema_test.lance").toString();
    writeLanceDatasetWithDateMilli(datasetUri);

    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(datasetUri);

    // Date(MILLISECOND) should surface as DateType in Spark schema
    assertEquals(
        DataTypes.DateType,
        df.schema().apply("dt").dataType(),
        "Date(MILLISECOND) should surface as DateType in Spark schema");
  }

  @Test
  public void testDateMilliValuesAreReadCorrectly() throws Exception {
    String datasetUri = tempDir.resolve("date_milli_values_test.lance").toString();
    writeLanceDatasetWithDateMilli(datasetUri);

    spark.read().format(LanceDataSource.name).load(datasetUri).createOrReplaceTempView("dm_test");
    List<Row> rows = spark.sql("SELECT id, dt FROM dm_test ORDER BY id").collectAsList();

    assertEquals(2, rows.size(), "Expected 2 rows");
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(2, rows.get(1).getInt(0));
    // Spark DateType values are java.sql.Date
    assertEquals(Date.valueOf("2024-01-15"), rows.get(0).getDate(1));
    assertEquals(Date.valueOf("2024-06-30"), rows.get(1).getDate(1));
  }

  @Test
  public void testDateMilliColumnProjection() throws Exception {
    String datasetUri = tempDir.resolve("date_milli_projection_test.lance").toString();
    writeLanceDatasetWithDateMilli(datasetUri);

    spark.read().format(LanceDataSource.name).load(datasetUri).createOrReplaceTempView("dm_proj");

    List<Row> rows = spark.sql("SELECT dt FROM dm_proj ORDER BY dt").collectAsList();
    assertEquals(2, rows.size(), "Expected 2 rows");
    assertInstanceOf(Date.class, rows.get(0).get(0));
    assertInstanceOf(Date.class, rows.get(1).get(0));
  }
}
