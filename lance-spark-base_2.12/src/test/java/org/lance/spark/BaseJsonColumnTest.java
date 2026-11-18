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

import org.lance.spark.utils.JsonUtils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests reading and writing Lance JSON columns through Spark.
 *
 * <p>Lance physically persists JSON as JSONB in a LargeBinary column carrying the {@code
 * lance.json} extension name. Its Arrow read/write interface uses UTF-8 JSON text with the {@code
 * arrow.json} extension name: Lance encodes the text to JSONB on write and decodes it on read.
 * Spark has no JSON type, so the connector surfaces this logical representation as StringType
 * carrying {@code arrow.json} in field metadata.
 *
 * <p>The datasets are created through the lance Java API rather than Spark DDL, matching what
 * {@code BaseFixedSizeBinaryReadTest} does for a type Spark cannot express either.
 *
 * <p>Current lance-core exposes the physical {@code lance.json} schema from {@code
 * Dataset.getSchema()}, so the connector translates that field to its logical UTF-8/{@code
 * arrow.json} representation at the Spark boundary. Both extension names are recognized there to
 * remain compatible if lance-core begins returning the logical Arrow schema.
 */
public abstract class BaseJsonColumnTest {

  private static SparkSession spark;

  @TempDir static Path tempDir;

  private static final String JSON_1 = "{\"ingested_at\":\"2026-09-04T17:00:00Z\",\"version\":7}";
  private static final String JSON_2 = "{\"ingested_at\":\"2026-09-05T09:30:00Z\",\"version\":8}";

  @BeforeAll
  static void setup() {
    spark = SparkSession.builder().appName("json-column-test").master("local[*]").getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Builds the Arrow field a producer would declare for a JSON column: Utf8 + the extension name.
   */
  private static Field jsonField(String name) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("ARROW:extension:name", JsonUtils.ARROW_JSON_EXTENSION_NAME);
    return new Field(name, new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata), null);
  }

  /**
   * Creates a lance dataset with an id column and a JSON column, optionally populated.
   *
   * <p>Goes through the Arrow C Data interface for the same reason {@code
   * BaseFixedSizeBinaryReadTest} does: it keeps lance's JNI bridge and our buffers on the same
   * allocator root.
   */
  private static void createJsonDataset(String datasetUri, String... jsonValues) throws Exception {
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema arrowSchema = new Schema(Arrays.asList(idField, jsonField("payload")));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      VarCharVector jsonVec = (VarCharVector) root.getVector("payload");

      for (int i = 0; i < jsonValues.length; i++) {
        idVec.setSafe(i, i + 1);
        jsonVec.setSafe(i, jsonValues[i].getBytes(StandardCharsets.UTF_8));
      }
      root.setRowCount(jsonValues.length);

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
  public void testJsonColumnSurfacesAsStringType() throws Exception {
    String datasetUri = tempDir.resolve("json_schema_test.lance").toString();
    createJsonDataset(datasetUri, JSON_1);

    Dataset<Row> df = spark.read().format(LanceDataSource.name).load(datasetUri);
    StructField field = df.schema().apply("payload");

    assertInstanceOf(
        StringType.class,
        field.dataType(),
        "A JSON column should surface as StringType, not binary");
    assertEquals(
        JsonUtils.ARROW_JSON_EXTENSION_NAME,
        field.metadata().getString("ARROW:extension:name"),
        "The Spark field must carry the canonical Arrow JSON extension name");
  }

  @Test
  public void testJsonValuesAreReadAsText() throws Exception {
    String datasetUri = tempDir.resolve("json_values_test.lance").toString();
    createJsonDataset(datasetUri, JSON_1, JSON_2);

    spark.read().format(LanceDataSource.name).load(datasetUri).createOrReplaceTempView("json_read");
    List<Row> rows = spark.sql("SELECT id, payload FROM json_read ORDER BY id").collectAsList();

    assertEquals(2, rows.size());
    assertEquals(JSON_1, rows.get(0).getString(1));
    assertEquals(JSON_2, rows.get(1).getString(1));
  }

  /** The JSON text is an ordinary string to Spark, so Spark's own JSON functions apply to it. */
  @Test
  public void testJsonColumnIsQueryableWithSparkJsonFunctions() throws Exception {
    String datasetUri = tempDir.resolve("json_query_test.lance").toString();
    createJsonDataset(datasetUri, JSON_1, JSON_2);

    spark
        .read()
        .format(LanceDataSource.name)
        .load(datasetUri)
        .createOrReplaceTempView("json_query");
    List<Row> rows =
        spark
            .sql("SELECT id FROM json_query WHERE get_json_object(payload, '$.version') = '8'")
            .collectAsList();

    assertEquals(1, rows.size());
    assertEquals(2, rows.get(0).getInt(0));
  }

  /**
   * Appends to a JSON column created by another producer.
   *
   * <p>The DataFrame is built from the logical schema Spark read back: StringType with the
   * canonical {@code arrow.json} extension name. This verifies that the normalized schema can be
   * written back to a table whose physical column is JSONB/LargeBinary with {@code lance.json}.
   */
  @Test
  public void testWriteToExistingJsonColumn() throws Exception {
    String datasetUri = tempDir.resolve("json_append_test.lance").toString();
    createJsonDataset(datasetUri, JSON_1);

    StructType readSchema = spark.read().format(LanceDataSource.name).load(datasetUri).schema();
    Dataset<Row> toAppend =
        spark.createDataFrame(Collections.singletonList(RowFactory.create(2, JSON_2)), readSchema);

    toAppend.write().format(LanceDataSource.name).mode(SaveMode.Append).save(datasetUri);

    spark
        .read()
        .format(LanceDataSource.name)
        .load(datasetUri)
        .createOrReplaceTempView("json_appended");
    List<Row> rows = spark.sql("SELECT id, payload FROM json_appended ORDER BY id").collectAsList();

    assertEquals(2, rows.size(), "The appended row should be present");
    assertEquals(JSON_1, rows.get(0).getString(1));
    assertEquals(JSON_2, rows.get(1).getString(1), "The appended JSON should round-trip intact");
  }

  /** A JSON column must survive a full read-transform-write cycle without losing its type. */
  @Test
  public void testJsonColumnRoundTripsThroughSparkWrite() throws Exception {
    String sourceUri = tempDir.resolve("json_roundtrip_source.lance").toString();
    String targetUri = tempDir.resolve("json_roundtrip_target.lance").toString();
    createJsonDataset(sourceUri, JSON_1, JSON_2);

    spark
        .read()
        .format(LanceDataSource.name)
        .load(sourceUri)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.ErrorIfExists)
        .save(targetUri);

    StructField field =
        spark.read().format(LanceDataSource.name).load(targetUri).schema().apply("payload");
    assertInstanceOf(
        StringType.class, field.dataType(), "The copied column should still be a string");
    assertTrue(
        JsonUtils.isJsonSparkField(field),
        "The copied column should still be a JSON column, not a plain string");
  }

  /**
   * A plain string column must not become a JSON column, and a JSON marker must not be invented
   * where the producer declared none.
   */
  @Test
  public void testPlainStringColumnIsUnaffected() throws Exception {
    String datasetUri = tempDir.resolve("plain_string_test.lance").toString();

    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field textField = new Field("payload", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
    Schema arrowSchema = new Schema(Arrays.asList(idField, textField));

    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      ((IntVector) root.getVector("id")).setSafe(0, 1);
      ((VarCharVector) root.getVector("payload"))
          .setSafe(0, JSON_1.getBytes(StandardCharsets.UTF_8));
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

    StructField field =
        spark.read().format(LanceDataSource.name).load(datasetUri).schema().apply("payload");
    assertInstanceOf(StringType.class, field.dataType());
    assertFalse(
        JsonUtils.isJsonSparkField(field),
        "A column declared without the extension name must not be treated as JSON");
  }

  /** Spark writes logical Arrow JSON fields as Lance's physical JSONB representation. */
  @Test
  public void testSparkCanCreateJsonColumnViaMetadata() {
    String datasetUri = tempDir.resolve("json_create_test.lance").toString();

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
              new StructField(
                  "payload",
                  DataTypes.StringType,
                  true,
                  new MetadataBuilder()
                      .putString("ARROW:extension:name", JsonUtils.ARROW_JSON_EXTENSION_NAME)
                      .build())
            });

    spark
        .createDataFrame(Collections.singletonList(RowFactory.create(1, JSON_1)), schema)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.ErrorIfExists)
        .save(datasetUri);

    StructField field =
        spark.read().format(LanceDataSource.name).load(datasetUri).schema().apply("payload");
    assertTrue(
        JsonUtils.isJsonSparkField(field),
        "A string column marked with the JSON extension name should be created as a JSON column");

    try (org.lance.Dataset lanceDataset =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(datasetUri).build()) {
      Field physicalField = lanceDataset.getSchema().findField("payload");
      assertEquals(
          ArrowType.LargeBinary.INSTANCE,
          physicalField.getType(),
          "Lance must persist JSON as LargeBinary JSONB");
      assertEquals(
          JsonUtils.LANCE_JSON_EXTENSION_NAME,
          physicalField.getMetadata().get("ARROW:extension:name"),
          "Lance must persist the internal JSON extension name");
    }
  }
}
