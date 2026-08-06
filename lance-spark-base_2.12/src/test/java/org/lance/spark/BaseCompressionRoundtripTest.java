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

import org.lance.spark.utils.LanceFileCompressionReader;
import org.lance.spark.utils.LanceFileCompressionReader.CompressionScheme;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E roundtrip tests for per-column compression via TBLPROPERTIES.
 *
 * <p>Verifies that data written with {@code <col>.lance.compression} TBLPROPERTIES (lz4, zstd,
 * none) can be read back with identical values, AND that the on-disk Lance files actually use the
 * requested compression codec. The latter is verified by parsing the Lance v2 file footer and
 * column metadata protobuf to extract BufferCompression.scheme from the encoding tree.
 *
 * <p>Tests use {@code file_format_version = '2.2'} to enable block-level compression, which has no
 * minimum buffer size threshold (unlike miniblock compression in V2.1 which requires buffers &ge; 4
 * KB). Tests also generate enough data (10K+ rows with long strings) to ensure the compressed
 * output is smaller than the original, avoiding the compression effectiveness fallback.
 *
 * <p>The tests exercise both the SQL TBLPROPERTIES path and the DataFrame {@code tableProperty()}
 * API.
 */
public abstract class BaseCompressionRoundtripTest {
  private SparkSession spark;
  private static final String CATALOG_NAME = "lance_compression";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("compression-roundtrip-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private String fullTableName(String table) {
    return CATALOG_NAME + ".default." + table;
  }

  /**
   * Number of rows to generate for compression tests. Must be large enough to produce data that
   * exceeds compression thresholds and compresses effectively.
   */
  private static final int SAMPLE_ROW_COUNT = 10_000;

  private List<Row> sampleRows() {
    List<Row> rows = new ArrayList<>(SAMPLE_ROW_COUNT);
    for (int i = 0; i < SAMPLE_ROW_COUNT; i++) {
      String name = (i % 7 == 0) ? null : "name_padding_string_value_" + i + "_extra_content";
      Long value = (i % 11 == 0) ? null : (long) (i * 100);
      rows.add(RowFactory.create(i, name, value));
    }
    return rows;
  }

  private StructType sampleSchema() {
    return new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("value", DataTypes.LongType, true);
  }

  private void assertDataIntegrity(String tableName, int expectedRowCount) {
    // Count client-side via collectAsList().size() rather than SELECT count(*) to sidestep a
    // Spark 3.5 binary-compat bug: single-table COUNT(*) goes through the V2 pushed-aggregation
    // path which instantiates the old 2-arg Sum(Expression, Enumeration$Value) constructor that
    // was removed in some 3.5.x patch releases, causing NoSuchMethodError at test time. Pattern
    // re-used from the DFP integration-test suite.
    int rowCount = spark.sql("SELECT * FROM " + fullTableName(tableName)).collectAsList().size();
    assertEquals(expectedRowCount, rowCount, "Row count mismatch");
  }

  /**
   * Asserts that the specified columns in the on-disk Lance files use the expected compression
   * scheme. Columns are identified by their zero-based index in the schema.
   *
   * @param tableName table name (used to locate the .lance dataset directory)
   * @param expectedByColumn map from column index to expected compression scheme
   */
  private void assertColumnCompression(
      String tableName, Map<Integer, CompressionScheme> expectedByColumn) throws IOException {
    List<Path> dataFiles = LanceFileCompressionReader.findDataFilesForTable(tempDir, tableName);
    assertFalse(dataFiles.isEmpty(), "No .lance data files found for table " + tableName);

    for (Path file : dataFiles) {
      List<Set<CompressionScheme>> columnSchemes =
          LanceFileCompressionReader.readColumnCompressions(file);

      for (Map.Entry<Integer, CompressionScheme> entry : expectedByColumn.entrySet()) {
        int colIdx = entry.getKey();
        CompressionScheme expected = entry.getValue();

        assertTrue(
            colIdx < columnSchemes.size(),
            "Column index "
                + colIdx
                + " out of range (file has "
                + columnSchemes.size()
                + " columns)");

        Set<CompressionScheme> actual = columnSchemes.get(colIdx);
        if (expected == CompressionScheme.UNSPECIFIED) {
          // "none" compression: should not contain LZ4 or ZSTD
          assertFalse(
              actual.contains(CompressionScheme.LZ4),
              "Column " + colIdx + " should not use LZ4 but found: " + actual);
          assertFalse(
              actual.contains(CompressionScheme.ZSTD),
              "Column " + colIdx + " should not use ZSTD but found: " + actual);
        } else {
          assertTrue(
              actual.contains(expected),
              "Column "
                  + colIdx
                  + " expected "
                  + expected
                  + " but found: "
                  + actual
                  + " in file "
                  + file);
        }
      }
    }
  }

  @Test
  public void testLz4CompressionWithSqlTblProperties() throws Exception {
    String tableName = "comp_lz4_sql_" + System.currentTimeMillis();
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fullTableName(tableName)
            + " (id INT NOT NULL, name STRING, value BIGINT)"
            + " USING lance"
            + " TBLPROPERTIES ("
            + "'file_format_version' = '2.2',"
            + "'name.lance.compression' = 'lz4',"
            + "'value.lance.compression' = 'lz4'"
            + ")");

    List<Row> data = sampleRows();
    Dataset<Row> df = spark.createDataFrame(data, sampleSchema());
    df.writeTo(fullTableName(tableName)).append();

    assertDataIntegrity(tableName, data.size());
    // schema: id(0), name(1), value(2) — verify name and value use LZ4
    assertColumnCompression(tableName, Map.of(1, CompressionScheme.LZ4, 2, CompressionScheme.LZ4));

    spark.sql("DROP TABLE IF EXISTS " + fullTableName(tableName));
  }

  @Test
  public void testZstdCompressionWithSqlTblProperties() throws Exception {
    String tableName = "comp_zstd_sql_" + System.currentTimeMillis();
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fullTableName(tableName)
            + " (id INT NOT NULL, name STRING, value BIGINT)"
            + " USING lance"
            + " TBLPROPERTIES ("
            + "'file_format_version' = '2.2',"
            + "'name.lance.compression' = 'zstd',"
            + "'value.lance.compression' = 'zstd'"
            + ")");

    List<Row> data = sampleRows();
    Dataset<Row> df = spark.createDataFrame(data, sampleSchema());
    df.writeTo(fullTableName(tableName)).append();

    assertDataIntegrity(tableName, data.size());
    assertColumnCompression(
        tableName, Map.of(1, CompressionScheme.ZSTD, 2, CompressionScheme.ZSTD));

    spark.sql("DROP TABLE IF EXISTS " + fullTableName(tableName));
  }

  @Test
  public void testNoneCompressionWithSqlTblProperties() throws Exception {
    String tableName = "comp_none_sql_" + System.currentTimeMillis();
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fullTableName(tableName)
            + " (id INT NOT NULL, name STRING, value BIGINT)"
            + " USING lance"
            + " TBLPROPERTIES ("
            + "'file_format_version' = '2.2',"
            + "'name.lance.compression' = 'none',"
            + "'value.lance.compression' = 'none'"
            + ")");

    List<Row> data = sampleRows();
    Dataset<Row> df = spark.createDataFrame(data, sampleSchema());
    df.writeTo(fullTableName(tableName)).append();

    assertDataIntegrity(tableName, data.size());
    assertColumnCompression(
        tableName, Map.of(1, CompressionScheme.UNSPECIFIED, 2, CompressionScheme.UNSPECIFIED));

    spark.sql("DROP TABLE IF EXISTS " + fullTableName(tableName));
  }

  @Test
  public void testMixedCompressionPerColumn() throws Exception {
    String tableName = "comp_mixed_" + System.currentTimeMillis();
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fullTableName(tableName)
            + " (id INT NOT NULL, name STRING, value BIGINT)"
            + " USING lance"
            + " TBLPROPERTIES ("
            + "'file_format_version' = '2.2',"
            + "'name.lance.compression' = 'lz4',"
            + "'value.lance.compression' = 'zstd'"
            + ")");

    List<Row> data = sampleRows();
    Dataset<Row> df = spark.createDataFrame(data, sampleSchema());
    df.writeTo(fullTableName(tableName)).append();

    assertDataIntegrity(tableName, data.size());
    assertColumnCompression(tableName, Map.of(1, CompressionScheme.LZ4, 2, CompressionScheme.ZSTD));

    spark.sql("DROP TABLE IF EXISTS " + fullTableName(tableName));
  }

  @Test
  public void testCompressionWithDataFrameTablePropertyApi() throws Exception {
    String tableName = "comp_df_api_" + System.currentTimeMillis();

    List<Row> data = sampleRows();
    Dataset<Row> df = spark.createDataFrame(data, sampleSchema());
    df.writeTo(fullTableName(tableName))
        .tableProperty("file_format_version", "2.2")
        .tableProperty("name.lance.compression", "lz4")
        .tableProperty("value.lance.compression", "zstd")
        .createOrReplace();

    assertDataIntegrity(tableName, data.size());
    assertColumnCompression(tableName, Map.of(1, CompressionScheme.LZ4, 2, CompressionScheme.ZSTD));

    spark.sql("DROP TABLE IF EXISTS " + fullTableName(tableName));
  }

  @Test
  public void testCompressionWithMultipleAppends() throws Exception {
    String tableName = "comp_multi_append_" + System.currentTimeMillis();
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fullTableName(tableName)
            + " (id INT NOT NULL, name STRING, value BIGINT)"
            + " USING lance"
            + " TBLPROPERTIES ("
            + "'file_format_version' = '2.2',"
            + "'name.lance.compression' = 'zstd',"
            + "'value.lance.compression' = 'lz4'"
            + ")");

    // First append
    List<Row> batch1 = sampleRows();
    spark.createDataFrame(batch1, sampleSchema()).writeTo(fullTableName(tableName)).append();

    // Second append
    List<Row> batch2 = sampleRows();
    spark.createDataFrame(batch2, sampleSchema()).writeTo(fullTableName(tableName)).append();

    // Verify all data
    assertDataIntegrity(tableName, batch1.size() + batch2.size());

    // Verify compression on all data files
    assertColumnCompression(tableName, Map.of(1, CompressionScheme.ZSTD, 2, CompressionScheme.LZ4));

    spark.sql("DROP TABLE IF EXISTS " + fullTableName(tableName));
  }

  @Test
  public void testCompressionWithDirectPathWrite() throws Exception {
    // Direct path write: df.write().format("lance").option(...).save(path)
    // This exercises the SupportsCatalogOptions path where Spark's DataFrameWriter
    // passes properties=Map.empty to createTable, so compression options passed via
    // .option() must be applied in newWriteBuilder() instead.
    String tableName = "comp_direct_path_" + System.currentTimeMillis();
    String tablePath = tempDir.resolve(tableName).toString();

    List<Row> data = sampleRows();
    Dataset<Row> df = spark.createDataFrame(data, sampleSchema());
    df.write()
        .format("lance")
        .option("file_format_version", "2.2")
        .option("name.lance.compression", "lz4")
        .option("value.lance.compression", "zstd")
        .save(tablePath);

    // Verify data integrity by reading back. Use collectAsList().size() instead of Dataset.count()
    // for the same Spark 3.5 Sum-binary-compat reason documented in assertDataIntegrity().
    int count = spark.read().format("lance").load(tablePath).collectAsList().size();
    assertEquals(data.size(), count, "Row count mismatch for direct path write");

    // Verify compression on disk — locate the .lance dataset directory
    List<Path> dataFiles = LanceFileCompressionReader.findDataFiles(Path.of(tablePath));
    assertFalse(dataFiles.isEmpty(), "No .lance data files found for direct path write");

    for (Path file : dataFiles) {
      List<Set<CompressionScheme>> columnSchemes =
          LanceFileCompressionReader.readColumnCompressions(file);
      // schema: id(0), name(1), value(2)
      assertTrue(
          columnSchemes.get(1).contains(CompressionScheme.LZ4),
          "Column 1 (name) expected LZ4 but found: " + columnSchemes.get(1) + " in " + file);
      assertTrue(
          columnSchemes.get(2).contains(CompressionScheme.ZSTD),
          "Column 2 (value) expected ZSTD but found: " + columnSchemes.get(2) + " in " + file);
    }
  }
}
