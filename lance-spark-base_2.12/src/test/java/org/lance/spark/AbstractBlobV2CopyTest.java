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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractBlobV2CopyTest {

  protected static final String CATALOG_NAME = "lance_blob_v2";

  protected SparkSession spark;

  @TempDir protected Path tempDir;

  @BeforeEach
  void baseSetup() {
    spark =
        SparkSession.builder()
            .appName("blob-v2-late-materialization")
            .master("local[2]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void baseTearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  protected String fq(String table) {
    return CATALOG_NAME + ".default." + table;
  }

  protected static StructType idDataBinarySchema() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("data", DataTypes.BinaryType, true)
        });
  }

  protected void createV2BlobTable(String fqTable, String... extraProps) {
    StringBuilder props =
        new StringBuilder("'data.lance.encoding' = 'blob', 'file_format_version' = '2.2'");
    for (String p : extraProps) {
      props.append(", ").append(p);
    }
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTable
            + " (id INT NOT NULL, data BINARY) USING lance TBLPROPERTIES ("
            + props
            + ")");
  }

  protected void createV2BlobSource(String fqTable, Row... rows) throws Exception {
    createV2BlobTable(fqTable);
    spark
        .createDataFrame(Arrays.asList(rows), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqTable)
        .append();
  }

  protected static StructType tagSchema() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("tag", DataTypes.StringType, true)
        });
  }

  protected void createTwoBlobTarget(String fqTable) {
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTable
            + " (id INT NOT NULL, data_a BINARY, data_b BINARY) USING lance TBLPROPERTIES ("
            + "'data_a.lance.encoding' = 'blob', 'data_b.lance.encoding' = 'blob', "
            + "'file_format_version' = '2.2')");
  }

  protected void createV2BlobTagTarget(String fqTable) {
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTable
            + " (id INT NOT NULL, data BINARY, tag STRING) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = '2.2')");
  }

  protected void createBlobAndPlainBinaryTarget(String fqTable) {
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTable
            + " (id INT NOT NULL, data BINARY, note BINARY) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = '2.2')");
  }

  protected void createV1BlobSource(String fqTable, Row... rows) throws Exception {
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTable
            + " (id INT NOT NULL, data BINARY) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = '2.0')");
    spark
        .createDataFrame(Arrays.asList(rows), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqTable)
        .append();
  }

  protected static byte[] deterministicBlob(long seed, int length) {
    byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  protected LogicalPlan analyzePlan(String sql) throws Exception {
    return spark.sessionState().analyzer().execute(spark.sessionState().sqlParser().parsePlan(sql));
  }

  protected int countCopyRefs(LogicalPlan plan) {
    return BlobPlanProbe.countCopyRefs(plan);
  }

  protected List<String> rowAddressColumnNames(LogicalPlan plan) {
    return BlobPlanProbe.rowAddressColumnNames(plan);
  }

  protected long datasetVersionOf(String table) throws Exception {
    try (org.lance.Dataset ds =
        org.lance.Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(datasetUriOf(table))
            .build()) {
      return ds.version();
    }
  }

  protected String datasetUriOf(String table) throws Exception {
    return ((LanceDataset)
            ((TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG_NAME))
                .loadTable(Identifier.of(new String[] {"default"}, table)))
        .readOptions()
        .getDatasetUri();
  }

  protected List<Long> rowAddressesOf(String fqTable) {
    List<Row> addrRows =
        spark
            .sql("SELECT " + LanceConstant.ROW_ADDRESS + " FROM " + fqTable + " ORDER BY id")
            .collectAsList();
    List<Long> addrs = new ArrayList<>();
    for (Row r : addrRows) {
      addrs.add(r.getLong(0));
    }
    return addrs;
  }

  protected void assertTargetBlobBytes(String datasetUri, List<Long> rowAddrs, byte[][] expected)
      throws Exception {
    assertTargetBlobBytes(datasetUri, rowAddrs, "data", expected);
  }

  protected void assertTargetBlobBytes(
      String datasetUri, List<Long> rowAddrs, String column, byte[][] expected) throws Exception {
    try (org.lance.Dataset ds =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(datasetUri).build()) {
      List<org.lance.BlobFile> blobs = ds.takeBlobs(rowAddrs, column);
      assertEquals(expected.length, blobs.size(), "blob count mismatch");
      for (int i = 0; i < expected.length; i++) {
        try (org.lance.BlobFile bf = blobs.get(i)) {
          assertArrayEquals(expected[i], bf.read(), "blob bytes mismatch at row " + i);
        }
      }
    }
  }

  protected static Row row(int id, byte[] data) {
    return RowFactory.create(id, data);
  }
}
