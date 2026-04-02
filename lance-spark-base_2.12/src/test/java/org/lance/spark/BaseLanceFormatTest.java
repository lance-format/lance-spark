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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base test class for reading/writing Lance tables using spark.read.format("lance").load() and
 * spark.write.format("lance").save() without configuring a catalog. This verifies that the
 * DataSource works directly via path-based access (auto-registered default catalog).
 */
public abstract class BaseLanceFormatTest {
  private static SparkSession spark;
  private static String datasetUri;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    // Create SparkSession WITHOUT configuring any lance catalog
    spark = SparkSession.builder().appName("lance-format-read-test").master("local").getOrCreate();
    datasetUri =
        TestUtils.getDatasetUri(
            TestUtils.TestTable1Config.dbPath, TestUtils.TestTable1Config.datasetName);
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private void validateData(Dataset<Row> data, List<List<Long>> expectedValues) {
    List<Row> rows = data.collectAsList();
    assertEquals(expectedValues.size(), rows.size());

    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      List<Long> expectedRow = expectedValues.get(i);
      assertEquals(expectedRow.size(), row.size());

      for (int j = 0; j < expectedRow.size(); j++) {
        long expectedValue = expectedRow.get(j);
        long actualValue = row.getLong(j);
        assertEquals(expectedValue, actualValue, "Mismatch at row " + i + " column " + j);
      }
    }
  }

  @Test
  public void testReadWithLoad() {
    // Test reading using spark.read.format("lance").load(path)
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(df, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void testReadWithPathOption() {
    // Test reading using spark.read.format("lance").option("path", ...).load()
    Dataset<Row> df =
        spark
            .read()
            .format("lance")
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, datasetUri)
            .load();
    validateData(df, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void testFilterPushdown() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.filter("x > 1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .collect(Collectors.toList()));
  }

  @Test
  public void testColumnProjection() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.select("y", "b"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .collect(Collectors.toList()));
  }

  @Test
  public void testFilterAndSelect() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.select("y", "b").filter("y > 3"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .filter(row -> row.get(0) > 3)
            .collect(Collectors.toList()));
  }

  @Test
  public void testWriteAndReadWithFormat() {
    // Test writing and reading using spark.write.format("lance").save(path)
    String outputPath = tempDir.resolve("test_write_format.lance").toString();

    // Create test data
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("name", DataTypes.StringType)
            .add("value", DataTypes.DoubleType);

    List<Row> data =
        Arrays.asList(
            RowFactory.create(1L, "Alice", 100.0),
            RowFactory.create(2L, "Bob", 200.0),
            RowFactory.create(3L, "Charlie", 300.0));

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write using format("lance").save(path) - use ErrorIfExists for creating new table
    df.write().format("lance").mode(SaveMode.ErrorIfExists).save(outputPath);

    // Read back and verify
    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    List<Row> result = readDf.orderBy("id").collectAsList();

    assertEquals(3, result.size());
    assertEquals(1L, result.get(0).getLong(0));
    assertEquals("Alice", result.get(0).getString(1));
    assertEquals(100.0, result.get(0).getDouble(2), 0.001);
    assertEquals(2L, result.get(1).getLong(0));
    assertEquals("Bob", result.get(1).getString(1));
    assertEquals(200.0, result.get(1).getDouble(2), 0.001);
    assertEquals(3L, result.get(2).getLong(0));
    assertEquals("Charlie", result.get(2).getString(1));
    assertEquals(300.0, result.get(2).getDouble(2), 0.001);
  }

  @Test
  public void testAppendMode() {
    // Test appending data using format("lance")
    String outputPath = tempDir.resolve("test_append_format.lance").toString();

    StructType schema =
        new StructType().add("id", DataTypes.LongType).add("value", DataTypes.LongType);

    // Create table first using ErrorIfExists mode
    List<Row> data1 = Arrays.asList(RowFactory.create(1L, 100L), RowFactory.create(2L, 200L));
    Dataset<Row> df1 = spark.createDataFrame(data1, schema);
    df1.write().format("lance").mode(SaveMode.ErrorIfExists).save(outputPath);

    // Append more data
    List<Row> data2 = Arrays.asList(RowFactory.create(3L, 300L), RowFactory.create(4L, 400L));
    Dataset<Row> df2 = spark.createDataFrame(data2, schema);
    df2.write().format("lance").mode(SaveMode.Append).save(outputPath);

    // Verify all data is present
    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    assertEquals(4, readDf.count());

    List<Row> result = readDf.orderBy("id").collectAsList();
    assertEquals(1L, result.get(0).getLong(0));
    assertEquals(2L, result.get(1).getLong(0));
    assertEquals(3L, result.get(2).getLong(0));
    assertEquals(4L, result.get(3).getLong(0));
  }

  /** Helper: writes data to a Lance dataset and reads it back ordered by "id". */
  private List<Row> writeAndReadStruct(
      List<Row> data, StructType schema, String version, String tag) {
    String outputPath = tempDir.resolve("test_struct_" + tag + "_" + version + ".lance").toString();
    spark
        .createDataFrame(data, schema)
        .write()
        .format("lance")
        .option("file_format_version", version)
        .mode(SaveMode.ErrorIfExists)
        .save(outputPath);
    return spark.read().format("lance").load(outputPath).orderBy("id").collectAsList();
  }

  private static StructType createAddressSchema() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("city", DataTypes.StringType, true),
          DataTypes.createStructField("country", DataTypes.StringType, true)
        });
  }

  private static StructType createIdAddressSchema(StructType addressSchema) {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("address", addressSchema, true)
        });
  }

  /**
   * Tests that struct columns are correctly written and read back for all Lance format versions.
   * Reproduces issue #119: struct columns written with Lance format v2.1/v2.2 were always NULL
   * because StructWriter did not set the validity bit on the parent StructVector.
   */
  @ParameterizedTest
  @ValueSource(strings = {"2.0", "2.1", "2.2"})
  public void testStructColumnWithFormatVersion(String version) {
    StructType schema = createIdAddressSchema(createAddressSchema());
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Beijing", "China")),
            RowFactory.create(2, RowFactory.create("New York", "USA")));

    List<Row> result = writeAndReadStruct(data, schema, version, "non_null");

    assertEquals(2, result.size());
    assertNotNull(
        result.get(0).getStruct(1),
        "Struct column should not be NULL for format version " + version);
    assertEquals("Beijing", result.get(0).getStruct(1).getString(0));
    assertEquals("China", result.get(0).getStruct(1).getString(1));
    assertNotNull(
        result.get(1).getStruct(1),
        "Struct column should not be NULL for format version " + version);
    assertEquals("New York", result.get(1).getStruct(1).getString(0));
    assertEquals("USA", result.get(1).getStruct(1).getString(1));
  }

  /**
   * Tests that null struct values mixed with non-null structs are correctly written and read back.
   * Verifies the setNull() fix in StructWriter that propagates null to child vectors, keeping child
   * vector counts aligned with the parent StructVector. Without this fix, child vectors become
   * shorter than the parent, causing index misalignment for all rows after a null struct.
   *
   * <p>Only tests V2_1+ because V2_0 legacy encoders read null structs as empty structs rather than
   * null — this is a known behavioral difference in V2_0's CoreFieldEncodingStrategy, not a bug.
   */
  @ParameterizedTest
  @ValueSource(strings = {"2.1", "2.2"})
  public void testNullStructColumnWithFormatVersion(String version) {
    StructType schema = createIdAddressSchema(createAddressSchema());
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Beijing", "China")),
            RowFactory.create(2, null),
            RowFactory.create(3, null), // consecutive nulls
            RowFactory.create(4, RowFactory.create("New York", "USA")));

    List<Row> result = writeAndReadStruct(data, schema, version, "null_mixed");

    assertEquals(4, result.size());
    assertNotNull(result.get(0).getStruct(1));
    assertEquals("Beijing", result.get(0).getStruct(1).getString(0));
    assertEquals("China", result.get(0).getStruct(1).getString(1));
    assertTrue(
        result.get(1).isNullAt(1),
        "Null struct should be read back as NULL for format version " + version);
    assertTrue(
        result.get(2).isNullAt(1),
        "Consecutive null struct should be read back as NULL for format version " + version);
    // Row after consecutive nulls — validates count alignment accumulates correctly
    assertNotNull(
        result.get(3).getStruct(1),
        "Non-null struct after consecutive nulls should not be NULL for format version " + version);
    assertEquals("New York", result.get(3).getStruct(1).getString(0));
    assertEquals("USA", result.get(3).getStruct(1).getString(1));
  }

  /**
   * Tests that V2_0 legacy encoders read null structs as empty structs rather than null. This is a
   * known behavioral difference: V2_0's CoreFieldEncodingStrategy does not preserve null semantics
   * for struct types, so a null struct is read back as a non-null struct with null child fields.
   */
  @Test
  public void testNullStructV2_0ReadsAsEmptyStruct() {
    StructType schema = createIdAddressSchema(createAddressSchema());
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Beijing", "China")),
            RowFactory.create(2, null),
            RowFactory.create(3, RowFactory.create("New York", "USA")));

    List<Row> result = writeAndReadStruct(data, schema, "2.0", "v20_null_behavior");

    assertEquals(3, result.size());
    // V2_0: null struct is read back as non-null with null child fields (known behavior)
    assertFalse(
        result.get(1).isNullAt(1),
        "V2_0 legacy encoder reads null struct as empty struct, not null");
    Row emptyStruct = result.get(1).getStruct(1);
    assertNotNull(emptyStruct);
    assertTrue(emptyStruct.isNullAt(0), "city should be null in V2_0 empty struct");
    assertTrue(emptyStruct.isNullAt(1), "country should be null in V2_0 empty struct");
    // Non-null rows should still be correct
    assertEquals("Beijing", result.get(0).getStruct(1).getString(0));
    assertEquals("New York", result.get(2).getStruct(1).getString(0));
  }

  /**
   * Tests that nested structs with null values at different levels are correctly handled. Verifies
   * that StructWriter.setNull() recursively propagates null to all descendant child vectors,
   * keeping counts aligned at every nesting level.
   */
  @ParameterizedTest
  @ValueSource(strings = {"2.1", "2.2"})
  public void testNestedStructWithNullValues(String version) {
    // Schema: id INT, person STRUCT<name: STRING, address: STRUCT<city: STRING, country: STRING>>
    StructType addressSchema = createAddressSchema();
    StructType personSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("address", addressSchema, true)
            });
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("person", personSchema, true)
            });

    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Alice", RowFactory.create("Beijing", "China"))),
            RowFactory.create(2, null), // entire outer struct is null
            RowFactory.create(3, RowFactory.create("Bob", RowFactory.create("New York", "USA"))),
            RowFactory.create(4, RowFactory.create("Charlie", null)), // inner struct is null
            RowFactory.create(5, RowFactory.create("Diana", RowFactory.create("Tokyo", "Japan"))));

    List<Row> result = writeAndReadStruct(data, schema, version, "nested");

    assertEquals(5, result.size());

    // Row 1 (id=1): fully populated nested struct
    assertNotNull(result.get(0).getStruct(1));
    assertEquals("Alice", result.get(0).getStruct(1).getString(0));
    assertNotNull(result.get(0).getStruct(1).getStruct(1));
    assertEquals("Beijing", result.get(0).getStruct(1).getStruct(1).getString(0));

    // Row 2 (id=2): entire outer struct is null
    assertTrue(
        result.get(1).isNullAt(1), "Outer struct should be NULL for format version " + version);

    // Row 3 (id=3): fully populated — validates alignment after outer null
    assertNotNull(result.get(2).getStruct(1));
    assertEquals("Bob", result.get(2).getStruct(1).getString(0));
    assertEquals("New York", result.get(2).getStruct(1).getStruct(1).getString(0));
    assertEquals("USA", result.get(2).getStruct(1).getStruct(1).getString(1));

    // Row 4 (id=4): outer struct non-null, inner address struct is null
    assertNotNull(result.get(3).getStruct(1));
    assertEquals("Charlie", result.get(3).getStruct(1).getString(0));
    assertTrue(
        result.get(3).getStruct(1).isNullAt(1),
        "Inner address struct should be NULL for format version " + version);

    // Row 5 (id=5): fully populated — validates alignment after inner null
    assertNotNull(result.get(4).getStruct(1));
    assertEquals("Diana", result.get(4).getStruct(1).getString(0));
    assertEquals("Tokyo", result.get(4).getStruct(1).getStruct(1).getString(0));
    assertEquals("Japan", result.get(4).getStruct(1).getStruct(1).getString(1));
  }

  /**
   * Tests that a table with a Map column can be created, written to, and read back correctly.
   * Covers non-null maps, null maps, empty maps, and multiple rows.
   */
  @Test
  public void testMapColumnWriteAndRead() {
    String outputPath = tempDir.resolve("test_map_column.lance").toString();

    // Schema: id + map<string, string>
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add(
                "props",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true),
                true);

    // Row 0: normal map
    Map<String, String> map0 = new HashMap<>();
    map0.put("color", "red");
    map0.put("size", "large");

    // Row 1: null map
    // Row 2: empty map
    Map<String, String> map2 = new HashMap<>();

    // Row 3: single-entry map
    Map<String, String> map3 = new HashMap<>();
    map3.put("key", "value");

    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, map0),
            RowFactory.create(1, null),
            RowFactory.create(2, map2),
            RowFactory.create(3, map3));
    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write
    df.write()
        .format("lance")
        .option("file_format_version", "2.2")
        .mode(SaveMode.ErrorIfExists)
        .save(outputPath);

    // Read back and sort by id to get deterministic order
    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    List<Row> result = readDf.sort("id").collectAsList();

    assertEquals(4, result.size());

    // Row 0: {"color" -> "red", "size" -> "large"}
    assertFalse(result.get(0).isNullAt(1));
    @SuppressWarnings("unchecked")
    Map<String, String> readMap0 = result.get(0).getJavaMap(1);
    assertEquals(2, readMap0.size());
    assertEquals("red", readMap0.get("color"));
    assertEquals("large", readMap0.get("size"));

    // Row 1: null
    assertTrue(result.get(1).isNullAt(1));

    // Row 2: empty map
    assertFalse(result.get(2).isNullAt(1));
    @SuppressWarnings("unchecked")
    Map<String, String> readMap2 = result.get(2).getJavaMap(1);
    assertEquals(0, readMap2.size());

    // Row 3: {"key" -> "value"}
    assertFalse(result.get(3).isNullAt(1));
    @SuppressWarnings("unchecked")
    Map<String, String> readMap3 = result.get(3).getJavaMap(1);
    assertEquals(1, readMap3.size());
    assertEquals("value", readMap3.get("key"));
  }

  /** Tests Map with non-string value type: Map(StringType, IntegerType). */
  @Test
  public void testMapColumnWithIntValues() {
    String outputPath = tempDir.resolve("test_map_int_values.lance").toString();

    StructType schema =
        new StructType()
            .add(
                "scores",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
                true);

    Map<String, Integer> mapValue = new HashMap<>();
    mapValue.put("math", 95);
    mapValue.put("science", 88);

    List<Row> data = Arrays.asList(RowFactory.create(mapValue));
    Dataset<Row> df = spark.createDataFrame(data, schema);

    df.write()
        .format("lance")
        .option("file_format_version", "2.2")
        .mode(SaveMode.ErrorIfExists)
        .save(outputPath);

    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    List<Row> result = readDf.collectAsList();

    assertEquals(1, result.size());
    assertFalse(result.get(0).isNullAt(0));

    @SuppressWarnings("unchecked")
    Map<String, Integer> readMap = result.get(0).getJavaMap(0);
    assertEquals(2, readMap.size());
    assertEquals(95, readMap.get("math"));
    assertEquals(88, readMap.get("science"));
  }
}
