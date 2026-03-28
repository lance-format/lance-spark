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
import java.util.*;
import java.util.stream.Collectors;

public class BaseVectorIndexTest {
  protected String tableName = "create_index_test2";
  protected String catalogName = "lance_test";
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

  @Test
  public void testCreateIvfFlatIndexDistributed() {
    prepareVectorDataset();
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index my_ivf_flat_index using ivf_flat (vector_column) "
                    + "with (numPartitions=128, distanceType='L2')",
                fullTable));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());
    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);
    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("my_ivf_flat_index", indexName);
    // Check index is created successfully
    checkIndex("my_ivf_flat_index");
  }

  @Test
  public void testCreateIvfSqIndexDistributed() {
    prepareVectorDataset();
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index my_ivf_sq_index using ivf_sq (vector_column) "
                    + "with (numPartitions=128, numBits=8, sampleRate=256, distanceType='L2')",
                fullTable));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());
    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);
    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("my_ivf_sq_index", indexName);
    // Check index is created successfully
    checkIndex("my_ivf_sq_index");
  }

  @Test
  public void testCreateIvfPqIndexDistributed() {
    prepareVectorDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index my_ivf_pq_index using ivf_pq (vector_column) "
                    + "with (numPartitions=128, numSubVectors=16, numBits=8, distanceType='L2')",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("my_ivf_pq_index", indexName);

    // Check index is created successfully
    checkIndex("my_ivf_pq_index");
  }

  @Test
  public void testCreateIvfPqIndexNoWithDistributed1() {
    prepareVectorDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index my_ivf_pq_index using ivf_pq (vector_column) ",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("my_ivf_pq_index", indexName);

    // Check index is created successfully
    checkIndex("my_ivf_pq_index");
  }

  /**
   * Prepare a dataset with vector column for testing. This method should be implemented in the base
   * class or here.
   */
  private void prepareVectorDataset() {
    String createTableSql =
        String.format(
            "create table %s (id int, vector_column ARRAY<FLOAT> NOT NULL) using lance "
                + "TBLPROPERTIES ("
                + " 'vector_column.arrow.fixed-size-list.size' = '128')",
            fullTable);
    // Create a table with vector column
    spark.sql(createTableSql);

    Random random = new Random(42);

    int batchSize = 64;
    int totalRows = 512;
    int numBatches = totalRows / batchSize;

    for (int batch = 0; batch < numBatches; batch++) {
      StringBuilder insertSql = new StringBuilder();
      insertSql.append(String.format("insert into %s values ", fullTable));

      List<String> valuesList = new ArrayList<>();

      for (int i = 0; i < batchSize; i++) {
        int rowId = batch * batchSize + i + 1;
        StringBuilder vectorArray = new StringBuilder("array(");

        for (int dim = 0; dim < 128; dim++) {
          if (dim > 0) {
            vectorArray.append(", ");
          }
          float value = Math.round(random.nextFloat() * 1000.0f) / 1000.0f;
          vectorArray.append(String.format("%.3f", value));
        }
        vectorArray.append(")");

        valuesList.add(String.format("(%d, %s)", rowId, vectorArray.toString()));
      }

      insertSql.append(String.join(", ", valuesList));

      spark.sql(insertSql.toString());
    }
    Dataset<Row> countResult = spark.sql(String.format("select count(*) from %s", fullTable));
  }

  public void checkIndex(String indexName) {
    // Check index is created successfully
    try (org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build()) {
      List<Index> indexList = lanceDataset.getIndexes();
      Assertions.assertFalse(indexList.isEmpty());
      Set<String> indexNames = indexList.stream().map(Index::name).collect(Collectors.toSet());
      Assertions.assertTrue(indexNames.contains(indexName));
    }
  }
}
