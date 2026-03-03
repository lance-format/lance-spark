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
}
