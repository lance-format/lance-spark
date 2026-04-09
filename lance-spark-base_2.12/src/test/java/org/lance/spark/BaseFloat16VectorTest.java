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

import org.lance.index.DistanceType;
import org.lance.ipc.Query;
import org.lance.spark.utils.Float16Utils;
import org.lance.spark.utils.QueryUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** Integration tests for float16 (half-precision) vector support. */
public abstract class BaseFloat16VectorTest {
  private SparkSession spark;
  private static final String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("float16-vector-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + "." + "root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateTableWithFloat16VectorSqlTblProperties() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_sql_" + System.currentTimeMillis();

    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "embeddings ARRAY<FLOAT> NOT NULL"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'embeddings.arrow.fixed-size-list.size' = '32', "
            + "'embeddings.arrow.float16' = 'true'"
            + ")");

    // Insert data via SQL
    StringBuilder floatArray = new StringBuilder();
    Random random = new Random(42);
    for (int i = 0; i < 32; i++) {
      if (i > 0) floatArray.append(", ");
      floatArray.append(random.nextFloat());
    }
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES (1, array("
            + floatArray
            + "))");

    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(1L, result.collectAsList().get(0).getLong(0));

    // Read back and verify we get float data
    Dataset<Row> data =
        spark.sql("SELECT id, embeddings FROM " + catalogName + ".default." + tableName);
    List<Row> rows = data.collectAsList();
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    List<Object> emb = rows.get(0).getList(1);
    assertEquals(32, emb.size());

    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithFloat16VectorDataFrameApi() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_df_api_" + System.currentTimeMillis();

    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < 5; i++) {
      float[] vector = new float[64];
      for (int j = 0; j < 64; j++) {
        vector[j] = random.nextFloat();
      }
      rows.add(RowFactory.create(i, vector));
    }

    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.FloatType, false), false)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.writeTo(catalogName + ".default." + tableName)
        .using("lance")
        .tableProperty("embeddings.arrow.fixed-size-list.size", "64")
        .tableProperty("embeddings.arrow.float16", "true")
        .createOrReplace();

    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(5L, result.collectAsList().get(0).getLong(0));

    // Verify we can read back
    Dataset<Row> readBack =
        spark.sql(
            "SELECT id, size(embeddings) as dim FROM "
                + catalogName
                + ".default."
                + tableName
                + " WHERE id = 2");
    List<Row> readRows = readBack.collectAsList();
    assertEquals(1, readRows.size());
    assertEquals(64, readRows.get(0).getInt(1));

    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testFloat16RoundTripPrecision() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_precision_" + System.currentTimeMillis();

    // Use known values that have different float16 behaviors
    // 1.0 and 0.5 are exactly representable, 0.1 is not
    float[] testValues = {1.0f, 0.5f, 0.0f, -1.0f, 0.1f, 3.14f, 100.0f};
    int dim = testValues.length;

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, testValues));

    Metadata vectorMetadata =
        new MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", dim)
            .putString("arrow.float16", "true")
            .build();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings",
                  DataTypes.createArrayType(DataTypes.FloatType, false),
                  false,
                  vectorMetadata)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.writeTo(catalogName + ".default." + tableName)
        .using("lance")
        .tableProperty("embeddings.arrow.fixed-size-list.size", String.valueOf(dim))
        .tableProperty("embeddings.arrow.float16", "true")
        .createOrReplace();

    Dataset<Row> readBack =
        spark.sql("SELECT embeddings FROM " + catalogName + ".default." + tableName);
    List<Row> readRows = readBack.collectAsList();
    assertEquals(1, readRows.size());

    List<Float> emb = readRows.get(0).getList(0);
    assertEquals(dim, emb.size());

    // Exactly representable values should round-trip exactly
    assertEquals(1.0f, emb.get(0), 0.0f, "1.0 should be exact");
    assertEquals(0.5f, emb.get(1), 0.0f, "0.5 should be exact");
    assertEquals(0.0f, emb.get(2), 0.0f, "0.0 should be exact");
    assertEquals(-1.0f, emb.get(3), 0.0f, "-1.0 should be exact");

    // Values with precision loss should be close (float16 ~3 decimal digits)
    assertEquals(0.1f, emb.get(4), 0.001f, "0.1 should be close");
    assertEquals(3.14f, emb.get(5), 0.01f, "3.14 should be close");
    assertEquals(100.0f, emb.get(6), 0.0f, "100.0 should be exact");

    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testFloat16SchemaMetadataPersistence() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_metadata_" + System.currentTimeMillis();

    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "embeddings ARRAY<FLOAT> NOT NULL"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'embeddings.arrow.fixed-size-list.size' = '16', "
            + "'embeddings.arrow.float16' = 'true'"
            + ")");

    // Verify metadata
    StructType tableSchema = spark.table(catalogName + ".default." + tableName).schema();
    StructField embField = tableSchema.apply("embeddings");
    assertTrue(
        embField.metadata().contains("arrow.fixed-size-list.size"),
        "Should have fixed-size-list metadata");
    assertEquals(16L, embField.metadata().getLong("arrow.fixed-size-list.size"));
    assertTrue(embField.metadata().contains("arrow.float16"), "Should have float16 metadata");
    assertEquals("true", embField.metadata().getString("arrow.float16"));

    // Insert data and verify metadata persists
    StringBuilder floatArray = new StringBuilder();
    Random random = new Random(42);
    for (int i = 0; i < 16; i++) {
      if (i > 0) floatArray.append(", ");
      floatArray.append(random.nextFloat());
    }
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES (1, array("
            + floatArray
            + "))");

    // Re-check metadata after write
    StructType schemaAfterWrite = spark.table(catalogName + ".default." + tableName).schema();
    StructField embFieldAfter = schemaAfterWrite.apply("embeddings");
    assertTrue(
        embFieldAfter.metadata().contains("arrow.float16"),
        "Float16 metadata should persist after write");

    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testMixedFloat16Float32Float64Columns() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_mixed_" + System.currentTimeMillis();

    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "vec_f16 ARRAY<FLOAT> NOT NULL, "
            + "vec_f32 ARRAY<FLOAT> NOT NULL, "
            + "vec_f64 ARRAY<DOUBLE> NOT NULL"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'vec_f16.arrow.fixed-size-list.size' = '8', "
            + "'vec_f16.arrow.float16' = 'true', "
            + "'vec_f32.arrow.fixed-size-list.size' = '8', "
            + "'vec_f64.arrow.fixed-size-list.size' = '8'"
            + ")");

    // Build SQL INSERT with arrays
    StringBuilder f16Array = new StringBuilder();
    StringBuilder f32Array = new StringBuilder();
    StringBuilder f64Array = new StringBuilder();
    for (int i = 0; i < 8; i++) {
      if (i > 0) {
        f16Array.append(", ");
        f32Array.append(", ");
        f64Array.append(", ");
      }
      f16Array.append((float) (i + 1));
      f32Array.append((float) (i + 10));
      f64Array.append((double) (i + 100));
    }

    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES (1, array("
            + f16Array
            + "), array("
            + f32Array
            + "), array("
            + f64Array
            + "))");

    Dataset<Row> data = spark.sql("SELECT * FROM " + catalogName + ".default." + tableName);
    List<Row> rows = data.collectAsList();
    assertEquals(1, rows.size());

    Row row = rows.get(0);
    assertEquals(1, row.getInt(0));

    // Float16 values (exact for small integers)
    List<Float> vecF16 = row.getList(1);
    assertEquals(8, vecF16.size());
    assertEquals(1.0f, vecF16.get(0), 0.0f);

    // Float32 values
    List<Float> vecF32 = row.getList(2);
    assertEquals(8, vecF32.size());
    assertEquals(10.0f, vecF32.get(0), 0.0f);

    // Float64 values
    List<Double> vecF64 = row.getList(3);
    assertEquals(8, vecF64.size());
    assertEquals(100.0, vecF64.get(0), 0.0);

    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testFloat16NullHandling() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_null_" + System.currentTimeMillis();

    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "embeddings ARRAY<FLOAT>"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'embeddings.arrow.fixed-size-list.size' = '4', "
            + "'embeddings.arrow.float16' = 'true'"
            + ")");

    // Insert one row with data and one with null
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES (1, array(1.0, 2.0, 3.0, 4.0)), (2, null)");

    Dataset<Row> data =
        spark.sql(
            "SELECT id, embeddings FROM " + catalogName + ".default." + tableName + " ORDER BY id");
    List<Row> rows = data.collectAsList();
    assertEquals(2, rows.size());

    // Row with data
    assertFalse(rows.get(0).isNullAt(1));
    List<Float> emb = rows.get(0).getList(1);
    assertEquals(4, emb.size());
    assertEquals(1.0f, emb.get(0), 0.0f);

    // Row with null
    assertTrue(rows.get(1).isNullAt(1));

    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testFloat16InvalidOnDoubleArray() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_invalid_double_" + System.currentTimeMillis();

    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "embeddings ARRAY<DOUBLE> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'embeddings.arrow.fixed-size-list.size' = '8', "
              + "'embeddings.arrow.float16' = 'true'"
              + ")");
      fail("Should throw exception for float16 on DOUBLE array");
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("must have element type FLOAT")
              || (e.getCause() != null
                  && e.getCause().getMessage().contains("must have element type FLOAT")),
          "Error should mention FLOAT requirement, got: " + e.getMessage());
    }
  }

  @Test
  public void testFloat16WithoutFixedSizeList() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    String tableName = "float16_no_fixedlist_" + System.currentTimeMillis();

    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "embeddings ARRAY<FLOAT> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'embeddings.arrow.float16' = 'true'"
              + ")");
      fail("Should throw exception for float16 without fixed-size-list");
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("arrow.fixed-size-list.size")
              || (e.getCause() != null
                  && e.getCause().getMessage().contains("arrow.fixed-size-list.size")),
          "Error should mention fixed-size-list requirement, got: " + e.getMessage());
    }
  }

  @Test
  public void testFloat16UnsupportedOnOlderArrow() {
    Assumptions.assumeFalse(
        Float16Utils.isFloat2VectorAvailable(),
        "This test only runs on older Arrow versions (Spark 3.x)");

    String tableName = "float16_unsupported_" + System.currentTimeMillis();

    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "embeddings ARRAY<FLOAT> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'embeddings.arrow.fixed-size-list.size' = '8', "
              + "'embeddings.arrow.float16' = 'true'"
              + ")");
      // If table creation doesn't immediately trigger Arrow usage,
      // try inserting data to trigger the float16 code path
      spark.sql(
          "INSERT INTO "
              + catalogName
              + ".default."
              + tableName
              + " VALUES (1, array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0))");
      fail("Should throw UnsupportedOperationException on older Arrow");
    } catch (Exception e) {
      String msg = getDeepMessage(e);
      assertTrue(
          msg.contains("Float16") || msg.contains("Float2Vector"),
          "Error should mention Float16/Float2Vector, got: " + msg);
    }
  }

  @Test
  public void testFloat16VectorSearchKnn() {
    Assumptions.assumeTrue(
        Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");

    // Create a float16 vector dataset via DataSource API so the URI is deterministic.
    String datasetUri = tempDir.toString() + "/float16_knn_dataset";

    // Build rows with known vectors so we can predict KNN results.
    // Vector 0: [0, 0, 0, 0]  — closest to query [0, 0, 0, 0]
    // Vector 1: [1, 1, 1, 1]  — L2 distance = 4
    // Vector 2: [10, 10, 10, 10] — L2 distance = 400
    // Vector 3: [100, 100, 100, 100] — L2 distance = 40000
    List<Row> writeRows = new ArrayList<>();
    writeRows.add(RowFactory.create(0, new float[] {0.0f, 0.0f, 0.0f, 0.0f}));
    writeRows.add(RowFactory.create(1, new float[] {1.0f, 1.0f, 1.0f, 1.0f}));
    writeRows.add(RowFactory.create(2, new float[] {10.0f, 10.0f, 10.0f, 10.0f}));
    writeRows.add(RowFactory.create(3, new float[] {100.0f, 100.0f, 100.0f, 100.0f}));

    Metadata vecMetadata =
        new MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 4)
            .putString("arrow.float16", "true")
            .build();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "vec", DataTypes.createArrayType(DataTypes.FloatType, false), false, vecMetadata)
            });

    Dataset<Row> df = spark.createDataFrame(writeRows, schema);
    df.write().format(LanceDataSource.name).save(datasetUri);

    // Build KNN query: find 2 nearest neighbors to [0, 0, 0, 0]
    Query.Builder builder = new Query.Builder();
    builder.setK(2);
    builder.setColumn("vec");
    builder.setKey(new float[] {0.0f, 0.0f, 0.0f, 0.0f});
    builder.setUseIndex(false); // brute-force scan (no index needed)
    builder.setDistanceType(DistanceType.L2);

    // Read via DataSource API with vector search
    Dataset<Row> result =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_NEAREST, QueryUtils.queryToString(builder.build()))
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, datasetUri)
            .load();

    List<Row> rows = result.collectAsList();
    // K=2, so we expect 2 results
    assertTrue(rows.size() >= 2, "Should return at least 2 KNN results, got: " + rows.size());

    // Verify the closest neighbors are id=0 and id=1
    // (L2 distances: 0 and 4 respectively)
    java.util.Set<Integer> resultIds = new java.util.HashSet<>();
    for (Row row : rows) {
      resultIds.add(row.getInt(row.fieldIndex("id")));
    }
    assertTrue(resultIds.contains(0), "Closest neighbor (id=0) should be in results");
    assertTrue(resultIds.contains(1), "Second closest neighbor (id=1) should be in results");

    // Verify vector values from the float16 column are readable
    for (Row row : rows) {
      List<Float> vec = row.getList(row.fieldIndex("vec"));
      assertEquals(4, vec.size(), "Vector dimension should be 4");
    }
  }

  private static String getDeepMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    Throwable current = t;
    while (current != null) {
      if (current.getMessage() != null) {
        sb.append(current.getMessage()).append(" | ");
      }
      current = current.getCause();
    }
    return sb.toString();
  }
}
