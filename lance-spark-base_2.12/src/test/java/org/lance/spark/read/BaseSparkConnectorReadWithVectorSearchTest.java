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

import org.lance.index.DistanceType;
import org.lance.ipc.Query;
import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.QueryUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 *The test logic is same with org.lance.VectorSearchTest.test_knn
 */

public abstract class BaseSparkConnectorReadWithVectorSearchTest {
  private static final String DATASET_NAME = "test_dataset5";

  private static SparkSession spark;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("spark-lance-vector-search-test")
            .master("local[*]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog.lance_default", "org.lance.spark.LanceNamespaceSparkCatalog")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void validateData() {
    Dataset<Row> data =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_NEAREST, QueryUtils.queryToString(knnQuery()))
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, rawTestDataset5Uri())
            .load();

    Set<Integer> expectedI = new HashSet<>(Arrays.asList(1, 81, 161, 241, 321));
    Set<Integer> actualI = new HashSet<>();
    List<Row> rows = data.collectAsList();
    for (int i = 0; i < rows.size(); i++) {
      actualI.add(rows.get(i).getInt(0));
    }
    assertEquals(expectedI, actualI, "Unexpected values in 'i' column");
  }

  @Test
  public void testVectorSearchReturnsExpectedTopK() {
    Dataset<Row> result = vectorSearch("i", nearestQuery(5));

    List<Row> rows = result.collectAsList();
    Set<Integer> expectedI = new HashSet<>(Arrays.asList(1, 81, 161, 241, 321));
    Set<Integer> actualI = new HashSet<>();
    for (int i = 0; i < rows.size(); i++) {
      actualI.add(rows.get(i).getInt(0));
    }
    assertEquals(5, rows.size());
    assertEquals(expectedI, actualI, "Unexpected vector_search top-k values in 'i' column");
  }

  @Test
  public void testVectorSearchKeepsMultiplePartitions() {
    Dataset<Row> result = vectorSearch("i", nearestQuery(5));

    assertTrue(
        scanPartitionCount(result) > 1,
        "vector_search should keep fragment scan partitions and apply the final TopK in Spark");
    assertEquals(5, result.collectAsList().size());
  }

  @Test
  public void testVectorSearchDoesNotExposeDistanceColumn() {
    Dataset<Row> result = vectorSearch("*", nearestQuery(1));

    assertFalse(
        Arrays.asList(result.schema().fieldNames()).contains("_distance"),
        "vector_search should use _distance internally but not expose it");
  }

  @Test
  public void testVectorSearchReturnsGlobalLimit() {
    Dataset<Row> result = vectorSearch("i", nearestQuery(1));

    assertEquals(1, result.collectAsList().size());
    assertFalse(
        Arrays.asList(result.schema().fieldNames()).contains("_distance"),
        "vector_search should not expose _distance for path-based reads");
  }

  @Test
  public void testVectorSearchDefaultsToExactL2Query() throws Exception {
    Query query = scanNearestQuery(vectorSearch("i", nearestQuery(5)));

    assertFalse(query.isUseIndex(), "vector_search should default to exact search");
    assertTrue(query.getDistanceType().isPresent(), "vector_search should set distance type");
    assertEquals(DistanceType.L2, query.getDistanceType().get());
  }

  @Test
  public void testVectorSearchAcceptsDistanceTypeWithoutUseIndex() throws Exception {
    Query.Builder builder = nearestQueryBuilder(5);
    builder.setDistanceType(DistanceType.Dot);
    Query query = scanNearestQuery(vectorSearch("i", builder.build()));

    assertFalse(query.isUseIndex(), "vector_search should default use_index to false");
    assertTrue(query.getDistanceType().isPresent(), "vector_search should set distance type");
    assertEquals(DistanceType.Dot, query.getDistanceType().get());
  }

  @Test
  public void testVectorSearchAcceptsExplicitUseIndex() throws Exception {
    Query.Builder builder = nearestQueryBuilder(5);
    builder.setDistanceType(DistanceType.Cosine);
    builder.setUseIndex(true);
    Query query = scanNearestQuery(vectorSearch("i", builder.build()));

    assertTrue(query.isUseIndex(), "vector_search should allow ANN search when requested");
    assertTrue(query.getDistanceType().isPresent(), "vector_search should set distance type");
    assertEquals(DistanceType.Cosine, query.getDistanceType().get());
  }

  @Test
  public void testVectorSearchRejectsInvalidNearestJson() {
    Exception ex =
        assertThrows(
            Exception.class,
            () -> spark.sql("SELECT * FROM vector_search('" + testDataset5Uri() + "', '{')"));

    assertTrue(
        ex.getMessage().contains("nearest"),
        "Expected nearest json error, got: " + ex.getMessage());
  }

  @Test
  public void testVectorSearchRejectsInvalidLimit() {
    String nearestJson =
        "{\"column\":\"vec\",\"k\":0,\"key\":["
            + queryVector32()
            + "],\"distanceType\":\"L2\",\"useIndex\":false}";

    assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    "SELECT * FROM vector_search('"
                        + testDataset5Uri()
                        + "', '"
                        + nearestJson
                        + "')")
                .collect());
  }

  private static Dataset<Row> vectorSearch(String selectList, Query query) {
    String nearestJson = QueryUtils.queryToString(query);
    StringBuilder sql =
        new StringBuilder()
            .append("SELECT ")
            .append(selectList)
            .append(" FROM vector_search('")
            .append(testDataset5Uri())
            .append("', '")
            .append(nearestJson)
            .append("'");
    sql.append(")");
    return spark.sql(sql.toString());
  }

  private static Query scanNearestQuery(Dataset<Row> result) throws Exception {
    LanceScan scan = lanceScan(result);
    Field readOptionsField = LanceScan.class.getDeclaredField("readOptions");
    readOptionsField.setAccessible(true);
    LanceSparkReadOptions readOptions = (LanceSparkReadOptions) readOptionsField.get(scan);
    Query query = readOptions.getNearest();
    assertNotNull(query, "Expected vector_search to configure nearest query");
    return query;
  }

  private static LanceScan lanceScan(Dataset<Row> result) {
    scala.collection.Iterator<SparkPlan> leaves =
        result.queryExecution().executedPlan().collectLeaves().iterator();
    while (leaves.hasNext()) {
      SparkPlan node = leaves.next();
      if (node instanceof BatchScanExec) {
        return (LanceScan) ((BatchScanExec) node).scan();
      }
    }
    throw new IllegalStateException("No Lance BatchScanExec found in vector_search plan");
  }

  private static Query knnQuery() {
    Query.Builder builder = nearestQueryBuilder(1);
    builder.setUseIndex(true);
    return builder.build();
  }

  private static Query nearestQuery(int k) {
    return nearestQueryBuilder(k).build();
  }

  private static Query.Builder nearestQueryBuilder(int k) {
    Query.Builder builder = new Query.Builder();
    float[] key = new float[32];
    for (int i = 0; i < 32; i++) {
      key[i] = (float) (i + 32);
    }
    builder.setK(k);
    builder.setColumn("vec");
    builder.setKey(key);
    builder.setUseIndex(false);
    builder.setDistanceType(DistanceType.L2);
    return builder;
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

  private static String testDataset5Uri() {
    return "`lance_default`.`" + escapeSqlIdentifier(rawTestDataset5Uri()) + "`";
  }

  private static String rawTestDataset5Uri() {
    return TestUtils.getDatasetUri(TestUtils.TestTable1Config.dbPath, DATASET_NAME);
  }

  private static String escapeSqlIdentifier(String identifier) {
    return identifier.replace("`", "``");
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
