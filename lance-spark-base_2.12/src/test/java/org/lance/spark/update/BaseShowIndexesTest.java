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
package org.lance.spark.update;

import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Base test for SHOW INDEXES command. */
public abstract class BaseShowIndexesTest {
  protected String catalogName = "lance_test";
  protected String tableName = "show_indexes_test";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;
  protected String tableDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-show-indexes-test")
            .master("local[10]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "show_indexes_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
    this.tableDir =
        FileSystems.getDefault().getPath(testRoot, this.tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  private void prepareDataset() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    // First insert to create initial fragments
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
    // Second insert to ensure multiple fragments
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  @Test
  public void testShowIndexes() {
    prepareDataset();

    // Create a B-tree index on id
    spark.sql(String.format("alter table %s create index test_index using btree (id)", fullTable));

    Dataset<Row> result = spark.sql(String.format("show indexes from %s", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(name,StringType,true),StructField(fields,ArrayType(StringType,true),true),StructField(index_type,StringType,true),StructField(num_indexed_fragments,LongType,true),StructField(num_indexed_rows,LongType,true),StructField(num_unindexed_fragments,LongType,true),StructField(num_unindexed_rows,LongType,true))",
        result.schema().toString());

    List<Row> rows = result.collectAsList();
    Assertions.assertFalse(rows.isEmpty(), "Expected at least one index row");

    Row row = rows.get(0);

    // name should match created index
    Assertions.assertEquals("test_index", row.getString(0));

    // fields should contain column name "id"
    @SuppressWarnings("unchecked")
    List<String> fieldNames = row.getList(1);
    Assertions.assertTrue(fieldNames.contains("id"), "fields should contain column name 'id'");

    // index_type should be btree
    Assertions.assertEquals("btree", row.getString(2));

    // num_indexed_fragments should be at least 1
    long numIndexedFragments = row.getLong(3);
    Assertions.assertTrue(numIndexedFragments >= 1L, "num_indexed_fragments should be at least 1");

    // num_indexed_rows should be at least 1
    long numIndexedRows = row.getLong(4);
    Assertions.assertTrue(numIndexedRows >= 1L, "num_indexed_rows should be at least 1");
  }

  @Test
  public void testShowIndexesFiltersMemWalIndex() {
    spark.sql(
        String.format(
            "create table %s (id int, region string) using lance "
                + "partitioned by (bucket(4, region))",
            fullTable));
    spark.sql(
        String.format(
            "insert into %s values (1, 'east'), (2, 'west'), (3, 'north'), (4, 'south')",
            fullTable));

    List<Row> systemOnly =
        spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Assertions.assertTrue(systemOnly.isEmpty(), "MemWAL must not appear in SHOW INDEXES");

    spark.sql(String.format("alter table %s create index test_index using btree (id)", fullTable));
    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();

    Assertions.assertEquals(1, rows.size());
    Row row = rows.get(0);
    Assertions.assertEquals("test_index", row.getString(0));
    Assertions.assertEquals("btree", row.getString(2));
    Assertions.assertTrue(row.getLong(3) >= 1L);
    Assertions.assertTrue(row.getLong(4) >= 1L);
  }

  @Test
  public void testShowIndexesFiltersFragmentReuseIndex() {
    prepareDataset();

    // Use one index segment over all fragments so compaction rewrites indexed data. Distributed
    // CREATE INDEX produces per-task segments that the compaction planner keeps in separate bins.
    IndexParams indexParams =
        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("BTREE")).build();
    try (var dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(tableDir).build())
            .build()) {
      dataset.createIndex(
          IndexOptions.builder(List.of("id"), IndexType.BTREE, indexParams)
              .replace(true)
              .train(true)
              .withIndexName("test_index")
              .build());
    }

    spark.sql(
        String.format(
            "optimize %s with (target_rows_per_fragment=20000, defer_index_remap=true)",
            fullTable));

    try (var dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(tableDir).build())
            .build()) {
      Assertions.assertTrue(
          dataset.getIndexes().stream()
              .anyMatch(index -> "__lance_frag_reuse".equalsIgnoreCase(index.name())),
          "OPTIMIZE with deferred index remapping must create a fragment-reuse system index");
    }

    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("test_index", rows.get(0).getString(0));
    Assertions.assertEquals("btree", rows.get(0).getString(2));
  }
}
