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

import org.lance.spark.utils.BlobUtils;

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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.lance.spark.LanceConstant.BLOB_POSITION_SUFFIX;
import static org.lance.spark.LanceConstant.BLOB_SIZE_SUFFIX;

public abstract class BaseBlobCreateTableTest {
  private SparkSession spark;
  private static final String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("blob-create-table-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + "." + "root", tempDir.toString())
            .getOrCreate();
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateTableWithBlobColumn() {
    String tableName = "blob_table_" + System.currentTimeMillis();

    // Create table with blob column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "data BINARY"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob'"
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
      // Create large binary data (> 64KB to ensure blob encoding is needed)
      byte[] largeData = new byte[100000]; // 100KB
      random.nextBytes(largeData);
      rows.add(RowFactory.create(i, largeData));
    }

    // Create DataFrame with proper schema
    Metadata blobMetadata =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("data", DataTypes.BinaryType, true, blobMetadata)
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

    // Verify we can read the blob data back
    Dataset<Row> dataResult =
        spark.sql(
            "SELECT id, data FROM " + catalogName + ".default." + tableName + " WHERE id = 0");

    List<Row> dataRows = dataResult.collectAsList();
    assertEquals(1, dataRows.size());
    assertEquals(0, dataRows.get(0).getInt(0));

    // Verify blob column is returned as empty byte array
    // Lance stores blobs out-of-line and returns position/size references internally,
    // but Spark sees them as empty byte arrays since we don't materialize the data
    Object blobData = dataRows.get(0).get(1);
    assertNotNull(blobData);
    assertTrue(blobData instanceof byte[], "Blob data should be byte array");

    byte[] blobBytes = (byte[]) blobData;
    // Blob data is not materialized, so we get empty array
    assertEquals(0, blobBytes.length, "Blob data should be empty (not materialized)");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateEmptyTableWithBlobAndSQLInsert() {
    String tableName = "blob_empty_table_" + System.currentTimeMillis();

    // Create empty table with blob column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "text STRING, "
            + "blob_data BINARY"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'blob_data.lance.encoding' = 'blob'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Insert data using SQL (with smaller test data for SQL insert)
    String testData1 = "This is test blob data 1";
    String testData2 = "This is test blob data 2";
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'first text', X'"
            + bytesToHex(testData1.getBytes(StandardCharsets.UTF_8))
            + "'), "
            + "(2, 'second text', X'"
            + bytesToHex(testData2.getBytes(StandardCharsets.UTF_8))
            + "')");

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

    // Also verify the blob data structure
    Dataset<Row> blobQuery =
        spark.sql(
            "SELECT id, blob_data FROM " + catalogName + ".default." + tableName + " ORDER BY id");
    List<Row> blobRows = blobQuery.collectAsList();
    assertEquals(2, blobRows.size());

    // Verify each blob is returned as empty binary data (not materialized)
    for (Row row : blobRows) {
      Object blobData = row.get(1);
      assertNotNull(blobData);
      assertTrue(blobData instanceof byte[], "Blob data should be byte array");

      byte[] blobBytes = (byte[]) blobData;
      // Blob data is not materialized, so we get empty arrays
      assertEquals(0, blobBytes.length, "Blob data should be empty (not materialized)");
    }

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithMultipleBlobColumns() {
    String tableName = "blob_table_multi_" + System.currentTimeMillis();

    // Create table with multiple blob columns using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "blob1 BINARY, "
            + "regular_binary BINARY, "
            + "blob2 BINARY"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'blob1.lance.encoding' = 'blob', "
            + "'blob2.lance.encoding' = 'blob'"
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
  public void testCreateTableWithInvalidBlobType() {
    String tableName = "blob_table_invalid_" + System.currentTimeMillis();

    // Try to create table with non-binary blob column (should fail)
    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "blob_data STRING"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'blob_data.lance.encoding' = 'blob'"
              + ")");
      fail("Should throw exception for non-binary blob column");
    } catch (Exception e) {
      // Expected exception
      assertTrue(
          e.getMessage().contains("must have BINARY type")
              || e.getCause().getMessage().contains("must have BINARY type"));
    }
  }

  @Test
  public void testBlobVirtualColumns() {
    String tableName = "blob_virtual_columns_" + System.currentTimeMillis();

    // Create table with blob column
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "data BINARY"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob'"
            + ")");

    // Insert test data
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < 5; i++) {
      byte[] largeData = new byte[100000]; // 100KB
      random.nextBytes(largeData);
      rows.add(RowFactory.create(i, largeData));
    }

    Metadata blobMetadata =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("data", DataTypes.BinaryType, true, blobMetadata)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    try {
      // Use coalesce(1) to write all data to a single partition/file
      // This ensures all blobs are in the same blob file with sequential positions
      df.coalesce(1).writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append data to table: " + e.getMessage());
    }

    // Test that we can select virtual columns for blob position and size
    Dataset<Row> result =
        spark.sql(
            "SELECT id, data, data"
                + BLOB_POSITION_SUFFIX
                + ", data"
                + BLOB_SIZE_SUFFIX
                + " FROM "
                + catalogName
                + ".default."
                + tableName
                + " ORDER BY id");

    List<Row> resultRows = result.collectAsList();
    assertEquals(5, resultRows.size());

    // Track all positions to verify they are all covered
    Set<Long> positions = new HashSet<>();
    int positionCount = 0;

    // Verify blob data and virtual columns
    for (Row row : resultRows) {
      // Verify blob data is returned as empty byte array (not materialized)
      Object blobData = row.get(1);
      assertNotNull(blobData);
      assertTrue(blobData instanceof byte[], "Blob data should be byte array");
      byte[] blobBytes = (byte[]) blobData;
      assertEquals(0, blobBytes.length, "Blob data should be empty (not materialized)");

      // Verify virtual columns for position and size
      long position = row.getLong(2);
      long size = row.getLong(3);

      // Position should be non-negative
      assertTrue(position >= 0, "Blob position should be non-negative");

      // Size should match the original data size (100KB)
      assertEquals(100000L, size, "Blob size should match original data size");

      // Collect all positions to verify they all exist
      positions.add(position);
      positionCount++;
    }

    // Verify all positions are covered (all rows have positions in the set)
    assertEquals(5, positionCount, "All blob rows should have positions");
    assertEquals(5, positions.size(), "All blob positions should be unique");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  private String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      hexString.append(String.format("%02X", b));
    }
    return hexString.toString();
  }

  // ==================== Large VarChar Tests ====================

  @Test
  public void testCreateTableWithLargeVarCharColumn() {
    String tableName = "large_varchar_table_" + System.currentTimeMillis();

    // Create table with large varchar column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "content STRING"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'content.arrow.large_var_char' = 'true'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // IMPORTANT: Verify the table schema has large varchar metadata preserved
    // When Lance stores LargeUtf8, fromArrowSchema adds "arrow:large-var-char" metadata
    StructType tableSchema = spark.table(catalogName + ".default." + tableName).schema();
    StructField contentField = tableSchema.apply("content");
    assertNotNull(contentField, "Content field should exist in schema");
    assertTrue(
        contentField.metadata().contains("arrow:large-var-char"),
        "Content field should have arrow:large-var-char metadata, indicating LargeUtf8 storage");
    assertEquals(
        "true",
        contentField.metadata().getString("arrow:large-var-char"),
        "arrow:large-var-char metadata should be 'true'");

    // Insert data into the table using DataFrame API
    // NO metadata needed - table schema already has large varchar property
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(RowFactory.create(i, "This is test content for row " + i));
    }

    // Create DataFrame with plain schema - NO large varchar metadata needed
    // The table's schema will automatically apply large varchar encoding
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("content", DataTypes.StringType, true)
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

    // Verify we can read the string data back
    Dataset<Row> dataResult =
        spark.sql(
            "SELECT id, content FROM " + catalogName + ".default." + tableName + " WHERE id = 0");

    List<Row> dataRows = dataResult.collectAsList();
    assertEquals(1, dataRows.size());
    assertEquals(0, dataRows.get(0).getInt(0));
    assertEquals("This is test content for row 0", dataRows.get(0).getString(1));

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithLargeVarCharAndSQLInsert() {
    String tableName = "large_varchar_sql_table_" + System.currentTimeMillis();

    // Create table with large varchar column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "description STRING, "
            + "large_content STRING"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'large_content.arrow.large_var_char' = 'true'"
            + ")");

    // Insert data using SQL
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'first description', 'This is a large content string 1'), "
            + "(2, 'second description', 'This is a large content string 2')");

    // Query the table to verify data was inserted
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(2L, result.collectAsList().get(0).getLong(0));

    // Query with projection
    Dataset<Row> projection =
        spark.sql(
            "SELECT id, description, large_content FROM "
                + catalogName
                + ".default."
                + tableName
                + " ORDER BY id");
    List<Row> projRows = projection.collectAsList();
    assertEquals(2, projRows.size());
    assertEquals(1, projRows.get(0).getInt(0));
    assertEquals("first description", projRows.get(0).getString(1));
    assertEquals("This is a large content string 1", projRows.get(0).getString(2));

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithInvalidLargeVarCharType() {
    String tableName = "large_varchar_invalid_" + System.currentTimeMillis();

    // Try to create table with non-string large varchar column (should fail)
    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "invalid_large_varchar INT"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'invalid_large_varchar.arrow.large_var_char' = 'true'"
              + ")");
      fail("Should throw exception for non-string large varchar column");
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("must have STRING type")
              || e.getCause().getMessage().contains("must have STRING type"));
    }
  }

  @Test
  public void testLargeVarCharWithLargeStrings() {
    String tableName = "large_varchar_large_strings_" + System.currentTimeMillis();

    // Create table with large varchar column
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "content STRING"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'content.arrow.large_var_char' = 'true'"
            + ")");

    // Insert data with large strings using DataFrame API
    List<Row> rows = new ArrayList<>();
    StringBuilder largeString = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      largeString.append("This is a repeated string to make it large. ");
    }
    String content = largeString.toString();

    for (int i = 0; i < 5; i++) {
      rows.add(RowFactory.create(i, content + " Row " + i));
    }

    // Create DataFrame with plain schema - NO large varchar metadata needed
    // The table's schema will automatically apply large varchar encoding
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("content", DataTypes.StringType, true)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    try {
      df.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append data to table with large strings: " + e.getMessage());
    }

    // Query the table to verify row count
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(5L, result.collectAsList().get(0).getLong(0));

    // Verify we can read the large string data back correctly
    Dataset<Row> dataResult =
        spark.sql(
            "SELECT id, content FROM " + catalogName + ".default." + tableName + " WHERE id = 2");

    List<Row> dataRows = dataResult.collectAsList();
    assertEquals(1, dataRows.size());
    assertEquals(2, dataRows.get(0).getInt(0));
    assertTrue(dataRows.get(0).getString(1).endsWith(" Row 2"));
    assertTrue(
        dataRows.get(0).getString(1).length() > 400000, "Content should be larger than 400KB");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testLargeVarCharSubsequentWritesWithoutMetadata() {
    // This test verifies that subsequent writes to an existing table with arrow.large_var_char
    // property work WITHOUT needing to specify metadata on the DataFrame.
    // The metadata should be automatically picked up from the table schema.
    String tableName = "large_varchar_auto_metadata_" + System.currentTimeMillis();

    // Step 1: Create table with large varchar column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "content STRING"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'content.arrow.large_var_char' = 'true'"
            + ")");

    // Step 2: First write - use SQL INSERT to populate initial data
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'Initial content 1'), "
            + "(2, 'Initial content 2')");

    // Verify initial data
    Dataset<Row> initialResult =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(2L, initialResult.collectAsList().get(0).getLong(0));

    // Step 3: Subsequent write using DataFrame API WITHOUT specifying large varchar metadata
    // This should work because the table schema already has the metadata
    List<Row> rows = new ArrayList<>();
    StringBuilder largeString = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      largeString.append("Large content string for testing automatic metadata. ");
    }
    String content = largeString.toString();

    for (int i = 10; i < 15; i++) {
      rows.add(RowFactory.create(i, content + " Row " + i));
    }

    // Create DataFrame with plain schema - NO large varchar metadata
    StructType plainSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("content", DataTypes.StringType, true)
            });

    Dataset<Row> df = spark.createDataFrame(rows, plainSchema);
    try {
      // This should work because the table's schema has the large varchar metadata
      // and it should be automatically applied during write
      df.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail(
          "Failed to append data without explicit metadata. "
              + "Table schema should automatically apply large varchar encoding: "
              + e.getMessage());
    }

    // Step 4: Verify all data was written correctly
    Dataset<Row> finalResult =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(7L, finalResult.collectAsList().get(0).getLong(0));

    // Verify the large string data can be read back correctly
    Dataset<Row> dataResult =
        spark.sql(
            "SELECT id, content FROM "
                + catalogName
                + ".default."
                + tableName
                + " WHERE id = 12 ORDER BY id");

    List<Row> dataRows = dataResult.collectAsList();
    assertEquals(1, dataRows.size());
    assertEquals(12, dataRows.get(0).getInt(0));
    assertTrue(dataRows.get(0).getString(1).endsWith(" Row 12"));
    assertTrue(
        dataRows.get(0).getString(1).length() > 200000, "Content should be larger than 200KB");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testLargeVarCharWithTablePropertyAPI() {
    // Test creating a table with large varchar using df.writeTo().tableProperty().createOrReplace()
    // This is the recommended way to create new tables with large varchar columns via DataFrame
    String tableName = "large_varchar_table_property_" + System.currentTimeMillis();

    // Create data with large strings
    List<Row> rows = new ArrayList<>();
    StringBuilder largeString = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      largeString.append("Large content for tableProperty API test. ");
    }
    String content = largeString.toString();

    for (int i = 0; i < 5; i++) {
      rows.add(RowFactory.create(i, content + " Row " + i));
    }

    // Create DataFrame with plain schema - NO metadata needed
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("content", DataTypes.StringType, true)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Use tableProperty API to specify large varchar - this is the recommended approach
    try {
      df.writeTo(catalogName + ".default." + tableName)
          .using("lance")
          .tableProperty("content.arrow.large_var_char", "true")
          .createOrReplace();
    } catch (Exception e) {
      fail("Failed to create table with tableProperty API: " + e.getMessage());
    }

    // IMPORTANT: Verify the table schema has large varchar metadata preserved
    // When Lance stores LargeUtf8, fromArrowSchema adds "arrow:large-var-char" metadata
    StructType tableSchema = spark.table(catalogName + ".default." + tableName).schema();
    StructField contentField = tableSchema.apply("content");
    assertNotNull(contentField, "Content field should exist in schema");
    assertTrue(
        contentField.metadata().contains("arrow:large-var-char"),
        "Content field should have arrow:large-var-char metadata, indicating LargeUtf8 storage");
    assertEquals(
        "true",
        contentField.metadata().getString("arrow:large-var-char"),
        "arrow:large-var-char metadata should be 'true'");

    // Verify data was written correctly
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(5L, result.collectAsList().get(0).getLong(0));

    // Verify we can read the large string data back correctly
    Dataset<Row> dataResult =
        spark.sql(
            "SELECT id, content FROM " + catalogName + ".default." + tableName + " WHERE id = 2");

    List<Row> dataRows = dataResult.collectAsList();
    assertEquals(1, dataRows.size());
    assertEquals(2, dataRows.get(0).getInt(0));
    assertTrue(dataRows.get(0).getString(1).endsWith(" Row 2"));
    assertTrue(
        dataRows.get(0).getString(1).length() > 200000, "Content should be larger than 200KB");

    // Verify subsequent writes also work without specifying metadata
    List<Row> moreRows = new ArrayList<>();
    for (int i = 10; i < 15; i++) {
      moreRows.add(RowFactory.create(i, content + " Row " + i));
    }
    Dataset<Row> df2 = spark.createDataFrame(moreRows, schema);
    try {
      df2.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append data after tableProperty create: " + e.getMessage());
    }

    // Verify total count
    Dataset<Row> finalResult =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(10L, finalResult.collectAsList().get(0).getLong(0));

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testLargeVarCharMultipleSubsequentWrites() {
    // Test multiple rounds of writes to ensure metadata is consistently preserved
    String tableName = "large_varchar_multi_writes_" + System.currentTimeMillis();

    // Create table with large varchar column
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "content STRING"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'content.arrow.large_var_char' = 'true'"
            + ")");

    // Plain schema without metadata
    StructType plainSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("content", DataTypes.StringType, true)
            });

    // Generate large content
    StringBuilder largeString = new StringBuilder();
    for (int i = 0; i < 3000; i++) {
      largeString.append("Repeated content for multi-write test. ");
    }
    String content = largeString.toString();

    // Perform multiple writes without metadata
    for (int batch = 0; batch < 3; batch++) {
      List<Row> rows = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        int id = batch * 10 + i;
        rows.add(RowFactory.create(id, content + " Batch " + batch + " Row " + i));
      }

      Dataset<Row> df = spark.createDataFrame(rows, plainSchema);
      try {
        df.writeTo(catalogName + ".default." + tableName).append();
      } catch (Exception e) {
        fail("Failed on batch " + batch + ": " + e.getMessage());
      }
    }

    // Verify total count
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(15L, result.collectAsList().get(0).getLong(0));

    // Verify data from each batch can be read correctly
    for (int batch = 0; batch < 3; batch++) {
      int expectedId = batch * 10 + 2;
      Dataset<Row> batchResult =
          spark.sql(
              "SELECT id, content FROM "
                  + catalogName
                  + ".default."
                  + tableName
                  + " WHERE id = "
                  + expectedId);

      List<Row> batchRows = batchResult.collectAsList();
      assertEquals(1, batchRows.size());
      assertEquals(expectedId, batchRows.get(0).getInt(0));
      assertTrue(batchRows.get(0).getString(1).contains("Batch " + batch));
    }

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }
}
