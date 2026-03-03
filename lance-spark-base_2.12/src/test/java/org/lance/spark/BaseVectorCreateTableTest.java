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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public abstract class BaseVectorCreateTableTest {
  private SparkSession spark;
  private static final String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("vector-create-table-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + "." + "root", tempDir.toString())
            .getOrCreate();
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateEmptyTableWithVectorAndSQLInsert() {
    String tableName = "vector_empty_table_" + System.currentTimeMillis();

    // Create empty table with vector column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "text STRING, "
            + "embeddings ARRAY<FLOAT> NOT NULL"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'embeddings.arrow.fixed-size-list.size' = '128'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Insert data using SQL
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'first text', array("
            + generateFloatArray(128)
            + ")), "
            + "(2, 'second text', array("
            + generateFloatArray(128)
            + "))");

    // Query the table to verify data was inserted
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(2L, result.collectAsList().get(0).getLong(0));

    // Query with projection
    Dataset<Row> projection =
        spark.sql("SELECT id, text FROM " + catalogName + ".default." + tableName + " ORDER BY id");
    List<Row> rows = projection.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("first text", rows.get(0).getString(1));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("second text", rows.get(1).getString(1));

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  private String generateFloatArray(int size) {
    Random random = new Random(42);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      if (i > 0) sb.append(", ");
      sb.append(random.nextFloat());
    }
    return sb.toString();
  }

  @Test
  public void testCreateTableWithVectorColumnFloat32() {
    String tableName = "vector_table_float32_" + System.currentTimeMillis();

    // Create table with vector column using TBLPROPERTIES
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
            + "'embeddings.arrow.fixed-size-list.size' = '128'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Insert data into the table
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < 10; i++) {
      float[] vector = new float[128];
      for (int j = 0; j < 128; j++) {
        vector[j] = random.nextFloat();
      }
      rows.add(RowFactory.create(i, vector));
    }

    // Create DataFrame with proper schema
    Metadata vectorMetadata =
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", 128).build();
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
    try {
      df.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append data to table: " + e.getMessage());
    }

    // Query the table
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(10L, result.collectAsList().get(0).getLong(0));

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithVectorColumnFloat64() {
    String tableName = "vector_table_float64_" + System.currentTimeMillis();

    // Create table with vector column using TBLPROPERTIES
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
            + "'embeddings.arrow.fixed-size-list.size' = '64'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Insert data into the table
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < 10; i++) {
      double[] vector = new double[64];
      for (int j = 0; j < 64; j++) {
        vector[j] = random.nextDouble();
      }
      rows.add(RowFactory.create(i, vector));
    }

    // Create DataFrame with proper schema
    Metadata vectorMetadata =
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", 64).build();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings",
                  DataTypes.createArrayType(DataTypes.DoubleType, false),
                  false,
                  vectorMetadata)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    try {
      df.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append data to table: " + e.getMessage());
    }

    // Query the table
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(10L, result.collectAsList().get(0).getLong(0));

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithMultipleVectorColumns() {
    String tableName = "vector_table_multi_" + System.currentTimeMillis();

    // Create table with multiple vector columns using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "embeddings1 ARRAY<FLOAT> NOT NULL, "
            + "embeddings2 ARRAY<DOUBLE> NOT NULL"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'embeddings1.arrow.fixed-size-list.size' = '128', "
            + "'embeddings2.arrow.fixed-size-list.size' = '256'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithInvalidVectorType() {
    String tableName = "vector_table_invalid_" + System.currentTimeMillis();

    // Try to create table with INT vector column (should fail)
    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "embeddings ARRAY<INT> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'embeddings.arrow.fixed-size-list.size' = '128'"
              + ")");
      fail("Should throw exception for non-float/double vector column");
    } catch (Exception e) {
      // Expected exception
      assertTrue(
          e.getMessage().contains("must have element type FLOAT or DOUBLE")
              || e.getCause().getMessage().contains("must have element type FLOAT or DOUBLE"));
    }
  }

  // ==================== Fixed-Size-List Tests ====================
  // These tests verify that when a table is created with fixed-size-list property,
  // subsequent DataFrame writes don't need to set the arrow type extension metadata.

  /** Helper method to verify a field has fixed-size-list metadata set. */
  private void assertFixedSizeListMetadata(StructType schema, String fieldName, long expectedSize) {
    StructField field = schema.apply(fieldName);
    assertNotNull(field, fieldName + " field should exist in schema");
    assertTrue(
        field.metadata().contains("arrow.fixed-size-list.size"),
        fieldName + " field should have arrow.fixed-size-list.size metadata");
    assertEquals(
        expectedSize,
        field.metadata().getLong("arrow.fixed-size-list.size"),
        "arrow.fixed-size-list.size metadata should be " + expectedSize);
  }

  @Test
  public void testFixedSizeListWithSqlTblProperties() {
    // Test creating table with SQL TBLPROPERTIES and mixed INSERT INTO / DataFrame writes
    String tableName = "fixed_size_list_sql_" + System.currentTimeMillis();

    // Create table with fixed-size-list column using SQL TBLPROPERTIES
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
            + "'embeddings.arrow.fixed-size-list.size' = '128'"
            + ")");

    // Verify schema has fixed-size-list metadata
    assertFixedSizeListMetadata(
        spark.table(catalogName + ".default." + tableName).schema(), "embeddings", 128L);

    // Write 1: SQL INSERT
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, array("
            + generateFloatArray(128)
            + ")), "
            + "(2, array("
            + generateFloatArray(128)
            + "))");

    // Verify schema still has metadata after write
    assertFixedSizeListMetadata(
        spark.table(catalogName + ".default." + tableName).schema(), "embeddings", 128L);

    // Write 2: DataFrame append with plain schema (no metadata needed)
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 10; i < 13; i++) {
      float[] vector = new float[128];
      for (int j = 0; j < 128; j++) {
        vector[j] = random.nextFloat();
      }
      rows.add(RowFactory.create(i, vector));
    }

    // Plain schema WITHOUT fixed-size-list metadata
    StructType plainSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.FloatType, false), false)
            });

    Dataset<Row> df = spark.createDataFrame(rows, plainSchema);
    try {
      df.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append DataFrame: " + e.getMessage());
    }

    // Verify schema still has metadata
    assertFixedSizeListMetadata(
        spark.table(catalogName + ".default." + tableName).schema(), "embeddings", 128L);

    // Write 3: Another SQL INSERT
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(20, array("
            + generateFloatArray(128)
            + "))");

    // Verify total count (2 + 3 + 1 = 6 rows)
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(6L, result.collectAsList().get(0).getLong(0));

    // Verify data integrity - check embeddings size
    Dataset<Row> embData =
        spark.sql(
            "SELECT id, size(embeddings) as emb_size FROM "
                + catalogName
                + ".default."
                + tableName
                + " WHERE id = 11");
    List<Row> embRows = embData.collectAsList();
    assertEquals(1, embRows.size());
    assertEquals(128, embRows.get(0).getInt(1), "Embeddings should have 128 elements");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testFixedSizeListWithTablePropertyAPI() {
    // Test creating table with DataFrame tableProperty() API and subsequent writes
    String tableName = "fixed_size_list_df_api_" + System.currentTimeMillis();

    // Initial data
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < 3; i++) {
      float[] vector = new float[128];
      for (int j = 0; j < 128; j++) {
        vector[j] = random.nextFloat();
      }
      rows.add(RowFactory.create(i, vector));
    }

    // Plain schema WITHOUT fixed-size-list metadata
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings", DataTypes.createArrayType(DataTypes.FloatType, false), false)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Create table with tableProperty API
    df.writeTo(catalogName + ".default." + tableName)
        .using("lance")
        .tableProperty("embeddings.arrow.fixed-size-list.size", "128")
        .createOrReplace();

    // Verify schema has fixed-size-list metadata
    assertFixedSizeListMetadata(
        spark.table(catalogName + ".default." + tableName).schema(), "embeddings", 128L);

    // Subsequent DataFrame append (no metadata needed)
    List<Row> moreRows = new ArrayList<>();
    for (int i = 10; i < 13; i++) {
      float[] vector = new float[128];
      for (int j = 0; j < 128; j++) {
        vector[j] = random.nextFloat();
      }
      moreRows.add(RowFactory.create(i, vector));
    }
    Dataset<Row> df2 = spark.createDataFrame(moreRows, schema);
    try {
      df2.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append DataFrame: " + e.getMessage());
    }

    // Verify schema still has metadata
    assertFixedSizeListMetadata(
        spark.table(catalogName + ".default." + tableName).schema(), "embeddings", 128L);

    // Verify total count (3 + 3 = 6 rows)
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(6L, result.collectAsList().get(0).getLong(0));

    // Verify data integrity
    Dataset<Row> dataResult =
        spark.sql(
            "SELECT id, size(embeddings) as emb_size FROM "
                + catalogName
                + ".default."
                + tableName
                + " WHERE id = 11");
    List<Row> dataRows = dataResult.collectAsList();
    assertEquals(1, dataRows.size());
    assertEquals(128, dataRows.get(0).getInt(1), "Embeddings should have 128 elements");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }
}
