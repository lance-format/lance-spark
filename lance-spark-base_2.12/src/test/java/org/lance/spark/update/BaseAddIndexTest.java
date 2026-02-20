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

import org.lance.index.Index;

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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Base test for distributed CREATE INDEX. */
public abstract class BaseAddIndexTest {
  protected String catalogName = "lance_test";
  protected String tableName = "create_index_test";
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
            .appName("lance-create-index-test")
            .master("local[10]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "create_index_test_" + UUID.randomUUID().toString().replace("-", "");
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
  public void testCreateIndexDistributed() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format("alter table %s create index test_index using btree (id)", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index", indexName);

    // Verify query using the indexed field
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=5", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(5, r.getInt(0));
    Assertions.assertEquals("text_5", r.getString(1));

    // Check index is created successfully
    checkIndex("test_index");
  }

  @Test
  public void testRepeatedCreateIndex() {
    prepareDataset();

    Dataset<Row> result1 =
        spark.sql(
            String.format(
                "alter table %s create index test_index_repeat using btree (id)", fullTable));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result1.schema().toString());
    Row row1 = result1.collectAsList().get(0);
    long fragmentsIndexed1 = row1.getLong(0);
    String indexName1 = row1.getString(1);
    Assertions.assertTrue(fragmentsIndexed1 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_repeat", indexName1);

    // Check index is created successfully
    checkIndex("test_index_repeat");

    Dataset<Row> result2 =
        spark.sql(
            String.format(
                "alter table %s create index test_index_repeat using btree (id)", fullTable));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result2.schema().toString());
    Row row2 = result2.collectAsList().get(0);
    long fragmentsIndexed2 = row2.getLong(0);
    String indexName2 = row2.getString(1);
    Assertions.assertTrue(fragmentsIndexed2 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_repeat", indexName2);

    // Check index is created successfully
    checkIndex("test_index_repeat");
  }

  @Test
  public void testCreateBTreeIndexWithZoneSize() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_btree_param using btree (id) with (zone_size=2048)",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_btree_param", indexName);

    checkIndex("test_index_btree_param");

    // Verify query using the indexed field with zone_size parameter
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=15", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(15, r.getInt(0));
    Assertions.assertEquals("text_15", r.getString(1));
  }

  @Test
  public void testCreateFtsIndex() {
    prepareDataset();

    // FTS requires all InvertedIndexDetails fields to be specified
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_index using fts (text) with ("
                    + "base_tokenizer='simple', "
                    + "language='English', "
                    + "max_token_length=40, "
                    + "lower_case=true, "
                    + "stem=false, "
                    + "remove_stop_words=false, "
                    + "ascii_folding=false, "
                    + "with_position=true"
                    + ")",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    // Verify distributed execution across multiple fragments
    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_index", indexName);

    // Check index is created successfully
    checkIndex("test_fts_index");

    // Verify query using the text column
    Dataset<Row> query =
        spark.sql(String.format("select * from %s where text='text_5'", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(5, r.getInt(0));
    Assertions.assertEquals("text_5", r.getString(1));
  }

  @Test
  public void testCreateFtsIndexWithStemming() {
    prepareDataset();

    // Test with stemming enabled
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_stem using fts (text) with ("
                    + "base_tokenizer='simple', "
                    + "language='English', "
                    + "max_token_length=40, "
                    + "lower_case=true, "
                    + "stem=true, "
                    + "remove_stop_words=false, "
                    + "ascii_folding=false, "
                    + "with_position=true"
                    + ")",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_stem", indexName);

    checkIndex("test_fts_stem");
  }

  @Test
  public void testRepeatedCreateFtsIndex() {
    prepareDataset();

    String ftsOptions =
        "base_tokenizer='simple', "
            + "language='English', "
            + "max_token_length=40, "
            + "lower_case=true, "
            + "stem=false, "
            + "remove_stop_words=false, "
            + "ascii_folding=false, "
            + "with_position=true";

    // First FTS index creation
    Dataset<Row> result1 =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_repeat using fts (text) with (%s)",
                fullTable, ftsOptions));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result1.schema().toString());
    Row row1 = result1.collectAsList().get(0);
    long fragmentsIndexed1 = row1.getLong(0);
    String indexName1 = row1.getString(1);
    Assertions.assertTrue(fragmentsIndexed1 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_repeat", indexName1);

    // Check index is created successfully
    checkIndex("test_fts_repeat");

    // Second FTS index creation with same name (should replace)
    Dataset<Row> result2 =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_repeat using fts (text) with (%s)",
                fullTable, ftsOptions));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result2.schema().toString());
    Row row2 = result2.collectAsList().get(0);
    long fragmentsIndexed2 = row2.getLong(0);
    String indexName2 = row2.getString(1);
    Assertions.assertTrue(fragmentsIndexed2 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_repeat", indexName2);

    // Check index still exists after replacement
    checkIndex("test_fts_repeat");
  }

  private void checkIndex(String indexName) {
    // Check index is created successfully
    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      List<Index> indexList = lanceDataset.getIndexes();
      Assertions.assertTrue(indexList.size() >= 1);
      Set<String> indexNames = indexList.stream().map(Index::name).collect(Collectors.toSet());
      Assertions.assertTrue(indexNames.contains(indexName));
    } finally {
      lanceDataset.close();
    }
  }
}
