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

import org.lance.spark.utils.BlobReferenceResolver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify blob data is preserved when blob columns flow through Spark operations like
 * JOIN and INSERT INTO SELECT.
 *
 * <p>Blob references (compact ~100 byte descriptors containing the source dataset URI and row
 * address) are serialized through Spark's shuffle instead of the actual blob bytes. On the write
 * side, the blob references are resolved by opening the source dataset and fetching the actual blob
 * content via {@code Dataset.takeBlobs()}.
 */
public abstract class BaseBlobJoinTest {
  private SparkSession spark;
  private static final String catalogName = "lance_blob_join";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("blob-join-test")
            .master("local[*]")
            // Enable the Lance extensions so LanceBlobSourceContextRule runs and propagates the
            // source dataset's credentials/open context to the write side for blob resolution.
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Verifies that blob data is preserved when selecting from a single blob table and inserting into
   * another table.
   */
  @Test
  public void testBlobPreservedDuringInsertIntoSelect() throws Exception {
    String sourceTable = "blob_source_" + System.currentTimeMillis();
    String targetTable = "blob_target_" + System.currentTimeMillis();
    String fqSource = catalogName + ".default." + sourceTable;
    String fqTarget = catalogName + ".default." + targetTable;

    // Create source table with blob column
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqSource
            + " (id INT NOT NULL, data BINARY) USING lance "
            + "TBLPROPERTIES ('data.lance.encoding' = 'blob')");

    // Insert known data into the source
    byte[] blobContent1 = "hello-blob-world-12345".getBytes(StandardCharsets.UTF_8);
    byte[] blobContent2 = "another-blob-value".getBytes(StandardCharsets.UTF_8);
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, blobContent1));
    rows.add(RowFactory.create(2, blobContent2));

    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("data", DataTypes.BinaryType, true)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    try {
      df.coalesce(1).writeTo(fqSource).append();
    } catch (Exception e) {
      fail("Failed to write to source table: " + e.getMessage());
    }

    // Create target table with blob column
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTarget
            + " (id INT NOT NULL, data BINARY) USING lance "
            + "TBLPROPERTIES ('data.lance.encoding' = 'blob')");

    // INSERT INTO target SELECT FROM source
    spark.sql("INSERT INTO " + fqTarget + " SELECT id, data FROM " + fqSource);

    // Verify row count
    Dataset<Row> result = spark.sql("SELECT COUNT(*) FROM " + fqTarget);
    assertEquals(2L, result.collectAsList().get(0).getLong(0), "Target should have 2 rows");

    // Verify blob data is preserved in target via virtual columns
    Dataset<Row> targetBlobs =
        spark.sql(
            "SELECT id, data, data"
                + LanceConstant.BLOB_SIZE_SUFFIX
                + " FROM "
                + fqTarget
                + " ORDER BY id");
    List<Row> targetRows = targetBlobs.collectAsList();
    assertEquals(2, targetRows.size());

    // Row 1: blob size should match the original content
    long blobSize1 = targetRows.get(0).getLong(2);
    assertEquals(
        blobContent1.length, blobSize1, "Blob size should match the original content length");

    // Row 2: blob size should match the original content
    long blobSize2 = targetRows.get(1).getLong(2);
    assertEquals(
        blobContent2.length, blobSize2, "Blob size should match the original content length");

    // Verify actual blob content by resolving the blob references from the target table
    try (BlobReferenceResolver resolver = new BlobReferenceResolver()) {
      byte[] targetBlob1 = (byte[]) targetRows.get(0).get(1);
      byte[] resolved1 = resolver.resolveIfNeeded(targetBlob1);
      assertArrayEquals(blobContent1, resolved1, "Row 1: blob content should match original");

      byte[] targetBlob2 = (byte[]) targetRows.get(1).get(1);
      byte[] resolved2 = resolver.resolveIfNeeded(targetBlob2);
      assertArrayEquals(blobContent2, resolved2, "Row 2: blob content should match original");
    }

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + fqSource);
    spark.sql("DROP TABLE IF EXISTS " + fqTarget);
  }

  /**
   * Verifies that blob data from two source tables is preserved when joining and inserting the
   * result into a third table.
   */
  @Test
  public void testBlobPreservedDuringJoinAndInsert() throws Exception {
    String tableA = "blob_join_a_" + System.currentTimeMillis();
    String tableB = "blob_join_b_" + System.currentTimeMillis();
    String targetTable = "blob_join_target_" + System.currentTimeMillis();
    String fqA = catalogName + ".default." + tableA;
    String fqB = catalogName + ".default." + tableB;
    String fqTarget = catalogName + ".default." + targetTable;

    // Create table A with blob column
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqA
            + " (id INT NOT NULL, blob_a BINARY) USING lance "
            + "TBLPROPERTIES ('blob_a.lance.encoding' = 'blob')");

    // Create table B with blob column
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqB
            + " (id INT NOT NULL, blob_b BINARY) USING lance "
            + "TBLPROPERTIES ('blob_b.lance.encoding' = 'blob')");

    // Insert data into table A
    List<Row> rowsA = new ArrayList<>();
    Random rng = new Random(42);
    byte[][] blobAData = new byte[5][];
    for (int i = 0; i < 5; i++) {
      byte[] data = new byte[1000];
      rng.nextBytes(data);
      data[0] = (byte) (i + 1);
      blobAData[i] = data;
      rowsA.add(RowFactory.create(i + 1, data));
    }

    StructType schemaA =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("blob_a", DataTypes.BinaryType, true)
            });

    try {
      spark.createDataFrame(rowsA, schemaA).coalesce(1).writeTo(fqA).append();
    } catch (Exception e) {
      fail("Failed to write to table A: " + e.getMessage());
    }

    // Insert data into table B
    List<Row> rowsB = new ArrayList<>();
    byte[][] blobBData = new byte[5][];
    for (int i = 0; i < 5; i++) {
      byte[] data = new byte[2000];
      rng.nextBytes(data);
      data[0] = (byte) (i + 101);
      blobBData[i] = data;
      rowsB.add(RowFactory.create(i + 1, data));
    }

    StructType schemaB =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("blob_b", DataTypes.BinaryType, true)
            });

    try {
      spark.createDataFrame(rowsB, schemaB).coalesce(1).writeTo(fqB).append();
    } catch (Exception e) {
      fail("Failed to write to table B: " + e.getMessage());
    }

    // Create target table with both blob columns
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTarget
            + " (id INT NOT NULL, blob_a BINARY, blob_b BINARY) USING lance "
            + "TBLPROPERTIES ("
            + "'blob_a.lance.encoding' = 'blob', "
            + "'blob_b.lance.encoding' = 'blob')");

    // JOIN and INSERT
    spark.sql(
        "INSERT INTO "
            + fqTarget
            + " SELECT a.id, a.blob_a, b.blob_b FROM "
            + fqA
            + " a JOIN "
            + fqB
            + " b ON a.id = b.id");

    // Verify row count
    Dataset<Row> countResult = spark.sql("SELECT COUNT(*) FROM " + fqTarget);
    assertEquals(5L, countResult.collectAsList().get(0).getLong(0), "Target should have 5 rows");

    // Verify blob data sizes are preserved via virtual columns
    Dataset<Row> targetBlobs =
        spark.sql(
            "SELECT id, blob_a, blob_b, "
                + "blob_a"
                + LanceConstant.BLOB_SIZE_SUFFIX
                + ", "
                + "blob_b"
                + LanceConstant.BLOB_SIZE_SUFFIX
                + " FROM "
                + fqTarget
                + " ORDER BY id");
    List<Row> targetRows = targetBlobs.collectAsList();
    assertEquals(5, targetRows.size());

    for (Row row : targetRows) {
      int id = row.getInt(0);
      long blobASize = row.getLong(3);
      long blobBSize = row.getLong(4);

      // Blob sizes should match the original data sizes
      assertEquals(
          1000L, blobASize, "Row " + id + ": blob_a size should match original data (1000 bytes)");
      assertEquals(
          2000L, blobBSize, "Row " + id + ": blob_b size should match original data (2000 bytes)");
    }

    // Verify actual blob content by resolving blob references
    try (BlobReferenceResolver resolver = new BlobReferenceResolver()) {
      for (Row row : targetRows) {
        int id = row.getInt(0);
        byte[] blobARef = (byte[]) row.get(1);
        byte[] blobBRef = (byte[]) row.get(2);
        byte[] resolvedA = resolver.resolveIfNeeded(blobARef);
        byte[] resolvedB = resolver.resolveIfNeeded(blobBRef);

        assertArrayEquals(
            blobAData[id - 1], resolvedA, "Row " + id + ": blob_a content should match original");
        assertArrayEquals(
            blobBData[id - 1], resolvedB, "Row " + id + ": blob_b content should match original");
      }
    }

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + fqA);
    spark.sql("DROP TABLE IF EXISTS " + fqB);
    spark.sql("DROP TABLE IF EXISTS " + fqTarget);
  }

  /**
   * Verifies that a one-to-many JOIN — where a single source blob row fans out to several output
   * rows — preserves the blob content for every output row. This exercises the resolver's
   * deduplication path: the same source row address is referenced by multiple shuffled rows and
   * must resolve to identical bytes in each, rather than skewing positionally.
   */
  @Test
  public void testBlobPreservedDuringOneToManyJoin() throws Exception {
    String blobTable = "blob_one_many_a_" + System.currentTimeMillis();
    String tagTable = "blob_one_many_b_" + System.currentTimeMillis();
    String targetTable = "blob_one_many_target_" + System.currentTimeMillis();
    String fqBlob = catalogName + ".default." + blobTable;
    String fqTag = catalogName + ".default." + tagTable;
    String fqTarget = catalogName + ".default." + targetTable;

    // Source blob table: one blob per id.
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqBlob
            + " (id INT NOT NULL, blob_a BINARY) USING lance "
            + "TBLPROPERTIES ('blob_a.lance.encoding' = 'blob')");

    byte[] blob1 = "blob-for-id-1".getBytes(StandardCharsets.UTF_8);
    byte[] blob2 = "blob-for-id-2".getBytes(StandardCharsets.UTF_8);
    List<Row> blobRows = new ArrayList<>();
    blobRows.add(RowFactory.create(1, blob1));
    blobRows.add(RowFactory.create(2, blob2));
    StructType blobSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("blob_a", DataTypes.BinaryType, true)
            });
    spark.createDataFrame(blobRows, blobSchema).coalesce(1).writeTo(fqBlob).append();

    // Tag table with multiple rows per id, so the join fans each blob row out to several outputs.
    spark.sql("CREATE TABLE IF NOT EXISTS " + fqTag + " (id INT NOT NULL, tag STRING) USING lance");
    List<Row> tagRows = new ArrayList<>();
    tagRows.add(RowFactory.create(1, "a"));
    tagRows.add(RowFactory.create(1, "b"));
    tagRows.add(RowFactory.create(1, "c"));
    tagRows.add(RowFactory.create(2, "d"));
    StructType tagSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("tag", DataTypes.StringType, true)
            });
    spark.createDataFrame(tagRows, tagSchema).coalesce(1).writeTo(fqTag).append();

    // Target carries the (duplicated) blob plus the tag that made it duplicate.
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTarget
            + " (id INT NOT NULL, blob_a BINARY, tag STRING) USING lance "
            + "TBLPROPERTIES ('blob_a.lance.encoding' = 'blob')");

    spark.sql(
        "INSERT INTO "
            + fqTarget
            + " SELECT a.id, a.blob_a, b.tag FROM "
            + fqBlob
            + " a JOIN "
            + fqTag
            + " b ON a.id = b.id");

    Dataset<Row> result =
        spark.sql("SELECT id, blob_a, tag FROM " + fqTarget + " ORDER BY id, tag");
    List<Row> rows = result.collectAsList();
    assertEquals(4, rows.size(), "one-to-many join should produce 4 rows");

    // id=1 fans out to 3 rows (tags a, b, c), id=2 to 1 row (tag d); blob content must match the
    // source blob for the row's id in every case.
    try (BlobReferenceResolver resolver = new BlobReferenceResolver()) {
      for (Row row : rows) {
        int id = row.getInt(0);
        byte[] expected = id == 1 ? blob1 : blob2;
        byte[] resolved = resolver.resolveIfNeeded((byte[]) row.get(1));
        assertArrayEquals(
            expected, resolved, "id=" + id + " tag=" + row.getString(2) + " blob mismatch");
      }
    }

    String[] tags = rows.stream().map(r -> r.getString(2)).toArray(String[]::new);
    assertArrayEquals(new String[] {"a", "b", "c", "d"}, tags, "tags should be preserved");

    spark.sql("DROP TABLE IF EXISTS " + fqBlob);
    spark.sql("DROP TABLE IF EXISTS " + fqTag);
    spark.sql("DROP TABLE IF EXISTS " + fqTarget);
  }

  /**
   * Verifies that non-blob columns are preserved correctly during JOIN + INSERT when blob columns
   * are also present.
   */
  @Test
  public void testNonBlobColumnsPreservedDuringJoinWithBlobs() throws Exception {
    String tableA = "blob_join_nonblob_a_" + System.currentTimeMillis();
    String tableB = "blob_join_nonblob_b_" + System.currentTimeMillis();
    String targetTable = "blob_join_nonblob_target_" + System.currentTimeMillis();
    String fqA = catalogName + ".default." + tableA;
    String fqB = catalogName + ".default." + tableB;
    String fqTarget = catalogName + ".default." + targetTable;

    // Create table A with blob + text columns
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqA
            + " (id INT NOT NULL, name STRING, blob_a BINARY) USING lance "
            + "TBLPROPERTIES ('blob_a.lance.encoding' = 'blob')");

    // Create table B with a score column
    spark.sql("CREATE TABLE IF NOT EXISTS " + fqB + " (id INT NOT NULL, score DOUBLE) USING lance");

    // Insert data into table A
    byte[] aliceBlob = "alice-blob-content".getBytes(StandardCharsets.UTF_8);
    byte[] bobBlob = "bob-blob-content".getBytes(StandardCharsets.UTF_8);
    List<Row> rowsA = new ArrayList<>();
    rowsA.add(RowFactory.create(1, "alice", aliceBlob));
    rowsA.add(RowFactory.create(2, "bob", bobBlob));

    StructType schemaA =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("blob_a", DataTypes.BinaryType, true)
            });

    try {
      spark.createDataFrame(rowsA, schemaA).coalesce(1).writeTo(fqA).append();
    } catch (Exception e) {
      fail("Failed to write to table A: " + e.getMessage());
    }

    // Insert data into table B
    List<Row> rowsB = new ArrayList<>();
    rowsB.add(RowFactory.create(1, 99.5));
    rowsB.add(RowFactory.create(2, 87.3));

    StructType schemaB =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("score", DataTypes.DoubleType, true)
            });

    try {
      spark.createDataFrame(rowsB, schemaB).coalesce(1).writeTo(fqB).append();
    } catch (Exception e) {
      fail("Failed to write to table B: " + e.getMessage());
    }

    // Create target table
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTarget
            + " (id INT NOT NULL, name STRING, score DOUBLE, blob_a BINARY) USING lance "
            + "TBLPROPERTIES ('blob_a.lance.encoding' = 'blob')");

    // JOIN and INSERT — non-blob columns should survive, blob data should be preserved
    spark.sql(
        "INSERT INTO "
            + fqTarget
            + " SELECT a.id, a.name, b.score, a.blob_a FROM "
            + fqA
            + " a JOIN "
            + fqB
            + " b ON a.id = b.id");

    // Verify non-blob data is preserved
    Dataset<Row> result = spark.sql("SELECT id, name, score FROM " + fqTarget + " ORDER BY id");
    List<Row> resultRows = result.collectAsList();
    assertEquals(2, resultRows.size());

    assertEquals(1, resultRows.get(0).getInt(0));
    assertEquals("alice", resultRows.get(0).getString(1));
    assertEquals(99.5, resultRows.get(0).getDouble(2), 0.01);

    assertEquals(2, resultRows.get(1).getInt(0));
    assertEquals("bob", resultRows.get(1).getString(1));
    assertEquals(87.3, resultRows.get(1).getDouble(2), 0.01);

    // Verify blob data is preserved via direct blob column read.
    // NOTE: We query the blob column itself (not the virtual __blob_size column alone)
    // because there is a pre-existing bug where querying only virtual blob columns
    // without the base blob column causes an ArrayIndexOutOfBoundsException.
    Dataset<Row> blobResult =
        spark.sql(
            "SELECT id, blob_a, blob_a"
                + LanceConstant.BLOB_SIZE_SUFFIX
                + " FROM "
                + fqTarget
                + " ORDER BY id");
    List<Row> blobRows = blobResult.collectAsList();
    assertEquals(2, blobRows.size());

    assertEquals(
        aliceBlob.length, blobRows.get(0).getLong(2), "alice's blob size should match original");
    assertEquals(
        bobBlob.length, blobRows.get(1).getLong(2), "bob's blob size should match original");

    // Verify actual blob content
    try (BlobReferenceResolver resolver = new BlobReferenceResolver()) {
      byte[] aliceResolved = resolver.resolveIfNeeded((byte[]) blobRows.get(0).get(1));
      assertArrayEquals(aliceBlob, aliceResolved, "alice's blob content should match original");

      byte[] bobResolved = resolver.resolveIfNeeded((byte[]) blobRows.get(1).get(1));
      assertArrayEquals(bobBlob, bobResolved, "bob's blob content should match original");
    }

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + fqA);
    spark.sql("DROP TABLE IF EXISTS " + fqB);
    spark.sql("DROP TABLE IF EXISTS " + fqTarget);
  }
}
