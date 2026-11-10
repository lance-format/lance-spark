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

import org.lance.CommitBuilder;
import org.lance.Transaction;
import org.lance.operation.Merge;
import org.lance.operation.UpdateConfig;
import org.lance.operation.UpdateMap;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.spark.sql.connector.catalog.TableChange.removeProperty;
import static org.apache.spark.sql.connector.catalog.TableChange.setProperty;
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
  public void testDescribeExtendedShowsVectorDimensions() throws Exception {
    String fullName = catalogName + ".default.vector_describe_table";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (embedding ARRAY<FLOAT>, embedding_double ARRAY<DOUBLE>, values ARRAY<FLOAT>)"
            + " USING lance TBLPROPERTIES ("
            + "'embedding.arrow.fixed-size-list.size' = '756', "
            + "'embedding_double.arrow.fixed-size-list.size' = '64')");

    // Reload from disk: dimensions must come from the schema, not CREATE TABLE's options.
    spark.sql("REFRESH TABLE " + fullName);
    spark.sql("ALTER TABLE " + fullName + " SET TBLPROPERTIES ('team' = 'search')");
    List<Row> description = spark.sql("DESCRIBE EXTENDED " + fullName).collectAsList();
    Row embedding =
        description.stream()
            .filter(row -> "embedding".equals(row.getString(0)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing embedding column"));
    assertEquals("array<float>", embedding.getString(1));
    String properties =
        description.stream()
            .filter(row -> "Table Properties".equals(row.getString(0)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing Table Properties row"))
            .getString(1);
    assertTrue(properties.contains("embedding.arrow.fixed-size-list.size=756"));
    assertTrue(properties.contains("embedding_double.arrow.fixed-size-list.size=64"));
    assertTrue(properties.contains("team=search"));
    assertFalse(properties.contains("values.arrow.fixed-size-list.size"));

    List<Row> dimension =
        spark
            .sql("SHOW TBLPROPERTIES " + fullName + " ('embedding.arrow.fixed-size-list.size')")
            .collectAsList();
    assertEquals("756", dimension.get(0).getString(1));

    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset)
            catalog.loadTable(Identifier.of(new String[] {"default"}, "vector_describe_table"));
    // Model legacy config independently of the catalog's ALTER TABLE mutation policy.
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      Map<String, String> config = new HashMap<>(dataset.getConfig());
      config.put("embedding.arrow.fixed-size-list.size", "128");
      dataset.updateConfig(config);
    }
    spark.sql("REFRESH TABLE " + fullName);
    List<Row> conflictingDimension =
        spark
            .sql("SHOW TBLPROPERTIES " + fullName + " ('embedding.arrow.fixed-size-list.size')")
            .collectAsList();
    assertEquals("756", conflictingDimension.get(0).getString(1));

    spark.sql(
        "ALTER TABLE "
            + fullName
            + " UNSET TBLPROPERTIES ('embedding.arrow.fixed-size-list.size')");
    spark.sql("REFRESH TABLE " + fullName);
    List<Row> updatedDimension =
        spark
            .sql("SHOW TBLPROPERTIES " + fullName + " ('embedding.arrow.fixed-size-list.size')")
            .collectAsList();
    assertEquals("756", updatedDimension.get(0).getString(1));
    assertFixedSizeListMetadata(spark.table(fullName).schema(), "embedding", 756);
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

  @Test
  public void testVectorDimensionSetIsRejected() {
    String fullName = catalogName + ".default.vector_dimension_set";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (embedding ARRAY<FLOAT>) USING lance "
            + "TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '756')");
    for (String size : new String[] {"128", "756"}) {
      UnsupportedOperationException error =
          assertThrows(
              UnsupportedOperationException.class,
              () ->
                  spark.sql(
                      "ALTER TABLE "
                          + fullName
                          + " SET TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '"
                          + size
                          + "')"));
      assertTrue(
          error
              .getMessage()
              .contains(
                  "Cannot set property 'embedding.arrow.fixed-size-list.size'"
                      + " because column 'embedding' already exists."));
    }
    assertFixedSizeListMetadata(spark.table(fullName).schema(), "embedding", 756);
  }

  @Test
  public void testSchemaPropertySetIsRejectedAtomically() throws Exception {
    String tableName = "protected_properties";
    String fullName = catalogName + ".default." + tableName;
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (embedding ARRAY<FLOAT>, payload BINARY, text STRING) USING lance "
            + "TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '756')");
    spark.sql(
        "ALTER TABLE "
            + fullName
            + " SET TBLPROPERTIES ('team' = 'search', 'custom.arrow.note' = 'keep')");
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier ident = Identifier.of(new String[] {"default"}, tableName);
    LanceDataset table = (LanceDataset) catalog.loadTable(ident);
    Map<String, String> originalConfig;
    long originalVersion;
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      originalConfig = dataset.getConfig();
      originalVersion = dataset.getVersion().getId();
    }
    for (String property :
        List.of(
            "embedding.arrow.fixed-size-list.size",
            "embedding.arrow.float16",
            "payload.arrow.fixed-size-binary.byte-width",
            "text.arrow.large_var_char",
            "payload.lance.encoding",
            "text.lance.compression",
            "text.lance.compression-level",
            "text.lance.structural-encoding",
            "text.lance.rle-threshold",
            "text.lance.bss")) {
      UnsupportedOperationException error =
          assertThrows(
              UnsupportedOperationException.class,
              () ->
                  catalog.alterTable(
                      ident,
                      setProperty("team", "changed"),
                      removeProperty("custom.arrow.note"),
                      setProperty(property, "1")));
      assertTrue(error.getMessage().contains(property));
    }
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      assertEquals(originalConfig, dataset.getConfig());
      assertEquals(originalVersion, dataset.getVersion().getId());
    }
  }

  @Test
  public void testFutureColumnEncodingPropertyCanBeSet() throws Exception {
    String tableName = "future_blob_encoding";
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id INT, name STRING) USING lance");
    spark.sql(
        "ALTER TABLE "
            + fullName
            + " SET TBLPROPERTIES ('content.lance.encoding' = 'blob', 'team' = 'search')");
    spark.sql("REFRESH TABLE " + fullName);
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, tableName));
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      assertEquals("blob", dataset.getConfig().get("content.lance.encoding"));
      assertEquals("search", dataset.getConfig().get("team"));
    }
  }

  @Test
  public void testConcurrentColumnAdditionRejectsStaleEncodingConfig() throws Exception {
    String tableName = "concurrent_blob_encoding";
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id INT) USING lance");
    spark.sql("ALTER TABLE " + fullName + " SET TBLPROPERTIES ('team' = 'search')");
    BaseLanceNamespaceSparkCatalog catalog =
        (BaseLanceNamespaceSparkCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier ident = Identifier.of(new String[] {"default"}, tableName);
    LanceDataset table = (LanceDataset) catalog.loadTable(ident);
    try (var stale = Utils.openDatasetBuilder(table.readOptions()).build();
        var concurrent = Utils.openDatasetBuilder(table.readOptions()).build()) {
      assertFalse(
          stale.getSchema().getFields().stream().anyMatch(f -> f.getName().equals("content")));
      concurrent.addColumns(
          List.of(new Field("content", FieldType.nullable(new ArrowType.Binary()), null)));
      long concurrentVersion = concurrent.version();
      assertThrows(
          RuntimeException.class,
          () ->
              catalog.alterTableProperties(
                  stale,
                  Map.of("content.lance.encoding", "blob", "other", "value"),
                  Set.of("team"),
                  false,
                  List.of("default", tableName)));
      try (var latest = Utils.openDatasetBuilder(table.readOptions()).build()) {
        assertEquals(concurrentVersion, latest.version());
        assertEquals(Map.of("team", "search"), latest.getConfig());
        assertEquals(new ArrowType.Binary(), latest.getSchema().findField("content").getType());
      }
    }
    assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.alterTable(ident, setProperty("content.lance.encoding", "blob")));
  }

  @Test
  public void testEncodingConfigRejectsStaleColumnAddition() throws Exception {
    String tableName = "stale_blob_column";
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id INT) USING lance");
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, tableName));
    Map<String, String> schemaMetadata = Map.of("owner", "search");
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build();
        Transaction txn =
            new Transaction.Builder()
                .readVersion(dataset.version())
                .operation(
                    UpdateConfig.builder()
                        .schemaMetadataUpdates(
                            UpdateMap.builder().updates(schemaMetadata).replace(true).build())
                        .build())
                .build();
        var committed = new CommitBuilder(dataset).execute(txn)) {
      assertEquals(schemaMetadata, committed.getSchema().getCustomMetadata());
    }
    try (var stale = Utils.openDatasetBuilder(table.readOptions()).build()) {
      List<Field> fields = new ArrayList<>(stale.getSchema().getFields());
      fields.add(new Field("content", FieldType.nullable(new ArrowType.Binary()), null));
      try (Transaction txn =
          new Transaction.Builder()
              .readVersion(stale.version())
              .operation(
                  Merge.builder()
                      .fragments(List.of())
                      .schema(new Schema(fields, schemaMetadata))
                      .build())
              .build()) {
        spark.sql(
            "ALTER TABLE " + fullName + " SET TBLPROPERTIES ('content.lance.encoding' = 'blob')");
        long configVersion;
        try (var latest = Utils.openDatasetBuilder(table.readOptions()).build()) {
          configVersion = latest.version();
          assertEquals(schemaMetadata, latest.getSchema().getCustomMetadata());
        }
        assertThrows(
            RuntimeException.class,
            () -> {
              try (var ignored = new CommitBuilder(stale).execute(txn)) {
                fail("A stale ADD COLUMNS must not commit over the encoding configuration");
              }
            });
        try (var latest = Utils.openDatasetBuilder(table.readOptions()).build()) {
          assertEquals(configVersion, latest.version());
          assertEquals("blob", latest.getConfig().get("content.lance.encoding"));
          assertEquals(1, latest.getSchema().getFields().size());
          assertEquals(schemaMetadata, latest.getSchema().getCustomMetadata());
        }
      }
    }
  }

  @Test
  public void testMixedExistingAndFutureSchemaPropertiesRejectedAtomically() throws Exception {
    String tableName = "mixed_schema_properties";
    String fullName = catalogName + ".default." + tableName;
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (embedding ARRAY<FLOAT>, name STRING) USING lance "
            + "TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '756')");
    spark.sql("ALTER TABLE " + fullName + " SET TBLPROPERTIES ('team' = 'search')");
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier ident = Identifier.of(new String[] {"default"}, tableName);
    LanceDataset table = (LanceDataset) catalog.loadTable(ident);
    Map<String, String> originalConfig;
    long originalVersion;
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      originalConfig = dataset.getConfig();
      originalVersion = dataset.getVersion().getId();
    }
    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                catalog.alterTable(
                    ident,
                    setProperty("team", "changed"),
                    setProperty("content.lance.encoding", "blob"),
                    setProperty("embedding.arrow.fixed-size-list.size", "128")));
    assertTrue(error.getMessage().contains("embedding.arrow.fixed-size-list.size"));
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      assertEquals(originalConfig, dataset.getConfig());
      assertEquals(originalVersion, dataset.getVersion().getId());
      assertFalse(dataset.getConfig().containsKey("content.lance.encoding"));
    }
  }

  @Test
  public void testUnsetLegacyVectorConfigKeepsSchemaDimension() throws Exception {
    String tableName = "legacy_vector_config";
    String fullName = catalogName + ".default." + tableName;
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (embedding ARRAY<FLOAT>) USING lance "
            + "TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '756')");
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, tableName));
    // Simulate config persisted by a connector version that allowed conflicting SET operations.
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      Map<String, String> config = new HashMap<>(dataset.getConfig());
      config.put("embedding.arrow.fixed-size-list.size", "128");
      config.put("team", "search");
      dataset.updateConfig(config);
    }
    spark.sql(
        "ALTER TABLE "
            + fullName
            + " UNSET TBLPROPERTIES ('embedding.arrow.fixed-size-list.size', 'missing.arrow.float16')");
    spark.sql("REFRESH TABLE " + fullName);
    try (var dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      assertFalse(dataset.getConfig().containsKey("embedding.arrow.fixed-size-list.size"));
      assertEquals("search", dataset.getConfig().get("team"));
    }
    assertFixedSizeListMetadata(spark.table(fullName).schema(), "embedding", 756);
  }
}
