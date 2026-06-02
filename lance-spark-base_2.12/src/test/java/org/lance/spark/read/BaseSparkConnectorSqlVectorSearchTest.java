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
package org.lance.spark.read;

import org.lance.spark.TestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseSparkConnectorSqlVectorSearchTest {
  private static final String CATALOG = "lance_sql_vec";

  private SparkSession spark;

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("spark-lance-sql-vector-search-test")
            .master("local[*]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + CATALOG + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testCatalogTableVectorSearchReturnsGlobalTopK() {
    String tableName = "vectors_" + System.currentTimeMillis();
    String table = CATALOG + ".default." + tableName;
    createCatalogVectorTable(table);

    Dataset<Row> result =
        spark.sql("SELECT id FROM vector_search('" + table + "', 'vec', array(0.0f, 0.0f), 2)");

    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(0, rows.get(0).getInt(0));
    assertEquals(1, rows.get(1).getInt(0));
  }

  @Test
  public void testVectorSearchKeepsMultiplePartitions() {
    String tableName = "multi_partition_" + System.currentTimeMillis();
    String table = CATALOG + ".default." + tableName;
    createCatalogVectorTable(table);

    Dataset<Row> result =
        spark.sql("SELECT id FROM vector_search('" + table + "', 'vec', array(0.0f, 0.0f), 2)");

    assertTrue(
        scanPartitionCount(result) > 1,
        "vector_search should keep fragment scan partitions and apply the final TopK in Spark");
    assertEquals(2, result.collectAsList().size());
  }

  @Test
  public void testVectorSearchDoesNotExposeDistanceColumn() {
    String tableName = "distance_hidden_" + System.currentTimeMillis();
    String table = CATALOG + ".default." + tableName;
    createCatalogVectorTable(table);

    Dataset<Row> result =
        spark.sql("SELECT * FROM vector_search('" + table + "', 'vec', array(0.0f, 0.0f), 1)");

    assertFalse(
        java.util.Arrays.asList(result.schema().fieldNames()).contains("_distance"),
        "vector_search should use _distance internally but not expose it");
  }

  @Test
  public void testDirectPathVectorSearchReturnsGlobalLimit() {
    String datasetUri = TestUtils.getDatasetUri(TestUtils.TestTable1Config.dbPath, "test_dataset5");

    Dataset<Row> result =
        spark.sql(
            "SELECT i FROM vector_search('"
                + datasetUri
                + "', 'vec', array("
                + queryVector32()
                + "), 1)");

    assertEquals(1, result.collectAsList().size());
    assertFalse(
        java.util.Arrays.asList(result.schema().fieldNames()).contains("_distance"),
        "vector_search should not expose _distance for path-based reads");
  }

  @Test
  public void testVectorSearchRejectsInvalidLimit() {
    String tableName = "invalid_limit_" + System.currentTimeMillis();
    String table = CATALOG + ".default." + tableName;
    createCatalogVectorTable(table);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            spark
                .sql("SELECT * FROM vector_search('" + table + "', 'vec', array(0.0f, 0.0f), 0)")
                .collect());
  }

  private void createCatalogVectorTable(String table) {
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT NOT NULL, vec ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '2')");
    spark.sql("INSERT INTO " + table + " VALUES (0, array(0.0f, 0.0f)), (10, array(10.0f, 10.0f))");
    spark.sql("INSERT INTO " + table + " VALUES (1, array(1.0f, 0.0f)), (2, array(2.0f, 0.0f))");
  }

  private static int scanPartitionCount(Dataset<Row> result) {
    scala.collection.Iterator<SparkPlan> leaves =
        result.queryExecution().executedPlan().collectLeaves().iterator();
    while (leaves.hasNext()) {
      SparkPlan node = leaves.next();
      if (node instanceof BatchScanExec) {
        return ((BatchScanExec) node).inputPartitions().size();
      }
    }
    return 0;
  }

  private static String queryVector32() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 32; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(i + 32).append(".0f");
    }
    return builder.toString();
  }
}
