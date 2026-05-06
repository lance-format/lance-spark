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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that Lance datasets with non-microsecond Timestamp columns can be read via Spark SQL.
 *
 * <p>Spark only supports Timestamp(MICROSECOND) natively. Other timestamp units (SECOND,
 * MILLISECOND, NANOSECOND) can appear in Lance datasets created from non-Spark sources (e.g.,
 * Python PyArrow). This test verifies that the schema conversion and vectorized read path handle
 * them correctly.
 *
 * <p>We create lance datasets directly via the Lance Java API (bypassing Spark DDL, which always
 * writes Timestamp(MICROSECOND)), then read back via {@code spark.read().format("lance")}.
 */
public abstract class BaseTimestampUnitReadTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  // 2024-06-15T12:30:00Z in various units
  private static final long TS1_SECONDS = 1718451000L;
  private static final long TS1_MILLIS = 1718451000_000L;
  private static final long TS1_MICROS = 1718451000_000_000L;
  private static final long TS1_NANOS = 1718451000_000_000_000L;

  // 1970-01-01T00:00:01Z — near epoch, tests edge behavior
  private static final long TS2_SECONDS = 1L;
  private static final long TS2_MILLIS = 1_000L;
  private static final long TS2_MICROS = 1_000_000L;
  private static final long TS2_NANOS = 1_000_000_000L;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("timestamp-unit-read-test")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static void writeLanceDataset(String datasetUri, Schema arrowSchema, DataWriter writer)
      throws Exception {
    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      writer.write(root);
      root.setRowCount(2);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, baos)) {
        w.start();
        w.writeBatch();
        w.end();
      }

      try (ArrowStreamReader reader =
              new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), allocator);
          ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, arrowStream);
        org.lance.Dataset.write().stream(arrowStream).uri(datasetUri).execute().close();
      }
    }
  }

  @FunctionalInterface
  private interface DataWriter {
    void write(VectorSchemaRoot root);
  }

  // ---- Timestamp(SECOND, "UTC") ----

  @Test
  public void testTimestampSecondTZSchema() throws Exception {
    String uri = tempDir.resolve("ts_sec_tz_schema.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")),
                    null)));
    writeLanceDataset(
        uri,
        schema,
        root -> {
          ((IntVector) root.getVector("id")).setSafe(0, 1);
          ((IntVector) root.getVector("id")).setSafe(1, 2);
          ((TimeStampSecTZVector) root.getVector("ts")).setSafe(0, TS1_SECONDS);
          ((TimeStampSecTZVector) root.getVector("ts")).setSafe(1, TS2_SECONDS);
        });

    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(uri);
    assertEquals(DataTypes.TimestampType, df.schema().apply("ts").dataType());
  }

  @Test
  public void testTimestampSecondTZValues() throws Exception {
    String uri = tempDir.resolve("ts_sec_tz_values.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")),
                    null)));
    writeLanceDataset(
        uri,
        schema,
        root -> {
          ((IntVector) root.getVector("id")).setSafe(0, 1);
          ((IntVector) root.getVector("id")).setSafe(1, 2);
          ((TimeStampSecTZVector) root.getVector("ts")).setSafe(0, TS1_SECONDS);
          ((TimeStampSecTZVector) root.getVector("ts")).setSafe(1, TS2_SECONDS);
        });

    spark.read().format(LanceDataSource.name).load(uri).createOrReplaceTempView("ts_sec_tz");
    List<Row> rows = spark.sql("SELECT id, ts FROM ts_sec_tz ORDER BY id").collectAsList();

    assertEquals(2, rows.size());
    assertEquals(Timestamp.from(Instant.ofEpochSecond(TS1_SECONDS)), rows.get(0).getTimestamp(1));
    assertEquals(Timestamp.from(Instant.ofEpochSecond(TS2_SECONDS)), rows.get(1).getTimestamp(1));
  }

  // ---- Timestamp(MILLISECOND, "UTC") ----

  @Test
  public void testTimestampMilliTZValues() throws Exception {
    String uri = tempDir.resolve("ts_milli_tz_values.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                    null)));
    writeLanceDataset(
        uri,
        schema,
        root -> {
          ((IntVector) root.getVector("id")).setSafe(0, 1);
          ((IntVector) root.getVector("id")).setSafe(1, 2);
          ((TimeStampMilliTZVector) root.getVector("ts")).setSafe(0, TS1_MILLIS);
          ((TimeStampMilliTZVector) root.getVector("ts")).setSafe(1, TS2_MILLIS);
        });

    spark.read().format(LanceDataSource.name).load(uri).createOrReplaceTempView("ts_milli_tz");
    List<Row> rows = spark.sql("SELECT id, ts FROM ts_milli_tz ORDER BY id").collectAsList();

    assertEquals(2, rows.size());
    assertEquals(Timestamp.from(Instant.ofEpochMilli(TS1_MILLIS)), rows.get(0).getTimestamp(1));
    assertEquals(Timestamp.from(Instant.ofEpochMilli(TS2_MILLIS)), rows.get(1).getTimestamp(1));
  }

  // ---- Timestamp(NANOSECOND, "UTC") ----

  @Test
  public void testTimestampNanoTZValues() throws Exception {
    String uri = tempDir.resolve("ts_nano_tz_values.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
                    null)));
    writeLanceDataset(
        uri,
        schema,
        root -> {
          ((IntVector) root.getVector("id")).setSafe(0, 1);
          ((IntVector) root.getVector("id")).setSafe(1, 2);
          ((TimeStampNanoTZVector) root.getVector("ts")).setSafe(0, TS1_NANOS);
          ((TimeStampNanoTZVector) root.getVector("ts")).setSafe(1, TS2_NANOS);
        });

    spark.read().format(LanceDataSource.name).load(uri).createOrReplaceTempView("ts_nano_tz");
    List<Row> rows = spark.sql("SELECT id, ts FROM ts_nano_tz ORDER BY id").collectAsList();

    assertEquals(2, rows.size());
    // Nanoseconds truncated to microseconds
    assertEquals(Timestamp.from(Instant.ofEpochSecond(TS1_SECONDS)), rows.get(0).getTimestamp(1));
    assertEquals(Timestamp.from(Instant.ofEpochSecond(TS2_SECONDS)), rows.get(1).getTimestamp(1));
  }

  // ---- Timestamp without timezone (NTZ variants) ----

  @Test
  public void testTimestampSecondNTZSchema() throws Exception {
    String uri = tempDir.resolve("ts_sec_ntz_schema.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)),
                    null)));
    writeLanceDataset(
        uri,
        schema,
        root -> {
          ((IntVector) root.getVector("id")).setSafe(0, 1);
          ((IntVector) root.getVector("id")).setSafe(1, 2);
          ((TimeStampSecVector) root.getVector("ts")).setSafe(0, TS1_SECONDS);
          ((TimeStampSecVector) root.getVector("ts")).setSafe(1, TS2_SECONDS);
        });

    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(uri);
    assertEquals(DataTypes.TimestampNTZType, df.schema().apply("ts").dataType());
  }

  @Test
  public void testTimestampMilliNTZValues() throws Exception {
    String uri = tempDir.resolve("ts_milli_ntz_values.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                    null)));
    writeLanceDataset(
        uri,
        schema,
        root -> {
          ((IntVector) root.getVector("id")).setSafe(0, 1);
          ((IntVector) root.getVector("id")).setSafe(1, 2);
          ((TimeStampMilliVector) root.getVector("ts")).setSafe(0, TS1_MILLIS);
          ((TimeStampMilliVector) root.getVector("ts")).setSafe(1, TS2_MILLIS);
        });

    spark.read().format(LanceDataSource.name).load(uri).createOrReplaceTempView("ts_milli_ntz");
    List<Row> rows = spark.sql("SELECT id, ts FROM ts_milli_ntz ORDER BY id").collectAsList();

    assertEquals(2, rows.size());
    // TimestampNTZ values surface as LocalDateTime via getAs
    LocalDateTime expected1 =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(TS1_MILLIS), ZoneOffset.UTC);
    LocalDateTime expected2 =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(TS2_MILLIS), ZoneOffset.UTC);
    assertEquals(expected1, rows.get(0).<LocalDateTime>getAs(1));
    assertEquals(expected2, rows.get(1).<LocalDateTime>getAs(1));
  }

  // ---- Null handling ----

  @Test
  public void testTimestampSecondWithNulls() throws Exception {
    String uri = tempDir.resolve("ts_sec_nulls.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")),
                    null)));
    writeLanceDataset(
        uri,
        schema,
        root -> {
          ((IntVector) root.getVector("id")).setSafe(0, 1);
          ((IntVector) root.getVector("id")).setSafe(1, 2);
          ((TimeStampSecTZVector) root.getVector("ts")).setSafe(0, TS1_SECONDS);
          // row 1: leave ts null (don't set it)
          root.setRowCount(2);
        });

    spark.read().format(LanceDataSource.name).load(uri).createOrReplaceTempView("ts_nulls");
    List<Row> rows = spark.sql("SELECT id, ts FROM ts_nulls ORDER BY id").collectAsList();

    assertEquals(2, rows.size());
    assertEquals(Timestamp.from(Instant.ofEpochSecond(TS1_SECONDS)), rows.get(0).getTimestamp(1));
    assertTrue(rows.get(1).isNullAt(1));
  }
}
