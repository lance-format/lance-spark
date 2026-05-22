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
package org.lance.spark.write;

import org.lance.memwal.MemWalIndexDetails;
import org.lance.memwal.ShardingField;
import org.lance.schema.LanceField;
import org.lance.spark.LanceRuntime;
import org.lance.spark.utils.BucketHashUtil;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test verifying that writes with bucket sharding spec produce fragments where each
 * fragment contains rows from exactly one bucket.
 */
public abstract class BaseBucketWriteTest {
  protected String catalogName = "lance_bucket_write_test";
  protected SparkSession spark;
  protected Path rootPath;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-bucket-write-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testBucketShardingDoesNotRequirePrimaryKey() {
    String tableName = "bucket_no_pk_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance "
                + "PARTITIONED BY (bucket(4, region))",
            fullTable));

    try (org.lance.Dataset dataset =
        org.lance.Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(tableUri(tableName))
            .build()) {
      assertFalse(
          dataset.getLanceSchema().fields().stream().anyMatch(BaseBucketWriteTest::isPrimaryKey),
          "Bucket sharding should not require an unenforced primary key");

      MemWalIndexDetails details =
          dataset
              .memWalIndexDetails()
              .orElseThrow(() -> new AssertionError("MemWAL index should be initialized"));
      assertFalse(details.shardingSpecs().isEmpty(), "MemWAL sharding spec should be persisted");

      LanceField regionField = field(dataset, "region");
      ShardingField shardingField = details.shardingSpecs().get(0).fields().get(0);
      assertEquals(Optional.of("bucket"), shardingField.transform());
      assertEquals(Collections.singletonList(regionField.getId()), shardingField.sourceIds());
    }
  }

  @Test
  public void testBucketWriteProducesOneBucketPerFragment() {
    String tableName = "bucket_write_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance "
                + "PARTITIONED BY (bucket(4, region))",
            fullTable));

    // Insert data with many distinct region values across 4 buckets
    StringBuilder values = new StringBuilder();
    int numRegions = 20;
    int rowsPerRegion = 3;
    for (int r = 0; r < numRegions; r++) {
      String region = "region_" + r;
      for (int i = 0; i < rowsPerRegion; i++) {
        if (values.length() > 0) {
          values.append(",");
        }
        int id = r * rowsPerRegion + i;
        values.append(String.format("(%d, '%s', %f)", id, region, id * 1.5));
      }
    }
    spark.sql(String.format("INSERT INTO %s (id, region, value) VALUES %s", fullTable, values));

    // Verify all data was written
    Dataset<Row> result = spark.sql("SELECT * FROM " + fullTable);
    assertEquals(numRegions * rowsPerRegion, result.count());

    // Verify that fragments are produced and data is bucketed.
    // Multiple fragments may map to the same bucket ID when different
    // column values that hash to the same bucket are not contiguous
    // in the sort order. The key invariant is: each fragment contains
    // rows from exactly one bucket ID.
    Dataset<Row> fragCount =
        spark.sql(String.format("SELECT COUNT(DISTINCT _fragid) as num_frags FROM %s", fullTable));
    long numFrags = fragCount.collectAsList().get(0).getLong(0);
    assertTrue(numFrags > 0, "Should have at least one fragment");

    Dataset<Row> fragmentRows =
        spark.sql(String.format("SELECT _fragid, region FROM %s ORDER BY _fragid", fullTable));
    Map<Integer, Integer> bucketByFragment = new HashMap<>();
    for (Row row : fragmentRows.collectAsList()) {
      int fragId = row.getInt(0);
      int bucketId = BucketHashUtil.computeBucketIdFromValue(row.getString(1), 4);
      Integer previous = bucketByFragment.putIfAbsent(fragId, bucketId);
      if (previous != null) {
        assertEquals(previous, bucketId, "Fragment " + fragId + " spans multiple bucket IDs");
      }
    }
  }

  @Test
  public void testBucketWriteDataIntegrity() {
    String tableName = "bucket_integrity_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, score DOUBLE) USING lance "
                + "PARTITIONED BY (bucket(8, name))",
            fullTable));

    String values =
        IntStream.range(0, 50)
            .mapToObj(
                i -> {
                  String name = "user_" + (i % 10);
                  return String.format("(%d, '%s', %f)", i, name, i * 0.5);
                })
            .collect(Collectors.joining(","));
    spark.sql(String.format("INSERT INTO %s (id, name, score) VALUES %s", fullTable, values));

    // Verify data integrity: all 50 rows present and correct
    Dataset<Row> result = spark.sql("SELECT * FROM " + fullTable + " ORDER BY id");
    assertEquals(50, result.count());

    List<Row> rows = result.collectAsList();
    for (int i = 0; i < 50; i++) {
      Row row = rows.get(i);
      assertEquals(i, row.getInt(0));
      assertEquals("user_" + (i % 10), row.getString(1));
    }
  }

  @Test
  public void testBucketWriteAllowsNullBucketColumn() {
    String tableName = "bucket_nulls_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, value DOUBLE) USING lance "
                + "PARTITIONED BY (bucket(4, region))",
            fullTable));

    spark.sql(
        String.format(
            "INSERT INTO %s (id, region, value) VALUES "
                + "(1, NULL, 1.0), (2, 'west', 2.0), (3, NULL, 3.0)",
            fullTable));

    assertTrue(
        spark.table(fullTable).schema().apply("region").nullable(),
        "Bucket column should remain nullable in Spark schema");

    Dataset<Row> nullRows =
        spark.sql(String.format("SELECT * FROM %s WHERE region IS NULL", fullTable));
    assertEquals(2, nullRows.count());
  }

  private String tableUri(String tableName) {
    return rootPath.resolve(tableName + ".lance").toString();
  }

  private static LanceField field(org.lance.Dataset dataset, String name) {
    return dataset.getLanceSchema().fields().stream()
        .filter(field -> field.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing Lance field: " + name));
  }

  private static boolean isPrimaryKey(LanceField field) {
    if (field.isUnenforcedPrimaryKey()) {
      return true;
    }
    Map<String, String> metadata = field.getMetadata();
    return metadata != null
        && "true".equalsIgnoreCase(metadata.get("lance-schema:unenforced-primary-key"));
  }
}
