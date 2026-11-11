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

import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ColumnarToRowExec;
import org.apache.spark.sql.execution.FilterExec;
import org.apache.spark.sql.execution.InputAdapter;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Joins whose filtering side is a Lance table with a pushed filter, which {@code
 * LanceDynamicPruningRule} keeps for Spark's dynamic partition pruning. Runtime filters reach Lance
 * scans on Spark 4.1+ only; on older versions the rule must leave every plan as it was.
 */
public abstract class BaseLanceDynamicPruningRuleTest {
  private static final int ROWS_PER_KEY = 3;
  private static final int FACT_FRAGMENTS = 5;

  @TempDir Path tempDir;

  protected SparkSession spark;
  private String catalog;

  @BeforeEach
  public void setup() {
    catalog = "dpp_" + suffix();
    spark =
        SparkSession.builder()
            .appName("lance-dynamic-pruning-rule-test")
            .master("local[4]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
            .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")
            .config("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "10")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.catalog." + catalog, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalog + ".impl", "dir")
            .config("spark.sql.catalog." + catalog + ".root", tempDir.toString())
            .config("spark.sql.catalog." + catalog + ".single_level_ns", "true")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static boolean runtimeFilteringSupported() {
    return LanceScan.supportsRuntimeFiltering(
        LanceSparkReadOptions.builder().datasetUri("/tmp/unused.lance").build());
  }

  @Test
  public void testPrunesByAPushedFilterOnALanceTable() {
    String fact = createFact(true);
    String dim = createDimension();
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id, d.label FROM %s f JOIN %s d ON f.k = d.k WHERE d.label = 'three'",
                fact, dim));

    assertEquals(ROWS_PER_KEY, joined.collectAsList().size());
    assertPrunedOnlyWhereSupported(joined, dim);
  }

  @Test
  public void testPrunesByAFilteredLanceDerivedTableInASemiJoin() {
    String fact = createFact(true);
    String dim = createDimension();
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f LEFT SEMI JOIN"
                    + " (SELECT * FROM %s WHERE label IN ('one_a', 'three')) d ON f.k = d.k",
                fact, dim));

    assertEquals(2 * ROWS_PER_KEY, joined.collectAsList().size());
    assertPrunedOnlyWhereSupported(joined, dim);
  }

  @Test
  public void testPrunesUnderAdaptiveExecutionWithABroadcastLanceTable() {
    spark.conf().set("spark.sql.adaptive.enabled", "true");
    spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "true");
    String fact = createFact(true);
    String dim = createDimension();
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT /*+ BROADCAST(d) */ f.id FROM %s f JOIN %s d ON f.k = d.k"
                    + " WHERE d.label = 'three'",
                fact, dim));

    assertEquals(ROWS_PER_KEY, joined.collectAsList().size());
    assertPrunedOnlyWhereSupported(joined, dim);
  }

  /**
   * Lance orders string bounds by code point, so U+1F600 is the fact zone's max, while Java's
   * UTF-16 order puts it below U+FFFD: a runtime filter on '\uFFFD' must keep the fragment.
   */
  @Test
  public void testKeepsRowsOfStringKeysWithSupplementaryCharacters() {
    spark.conf().set("spark.sql.adaptive.enabled", "true");
    spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "true");
    String fact = catalog + ".default.fact_" + suffix();
    spark.sql("CREATE TABLE " + fact + " (id INT, k STRING) USING lance");
    // One fragment, so that one zone holds both values.
    spark.sql(
        "INSERT INTO "
            + fact
            + " SELECT /*+ COALESCE(1) */ * FROM VALUES (1, '\uFFFD'), (2, '\uD83D\uDE00')"
            + " AS v(id, k)");
    spark.sql("ALTER TABLE " + fact + " CREATE INDEX k_zm USING zonemap (k)");
    String dim = catalog + ".default.dim_" + suffix();
    spark.sql("CREATE TABLE " + dim + " (k STRING, label STRING) USING lance");
    spark.sql("INSERT INTO " + dim + " VALUES ('\uFFFD', 'keep'), ('other', 'drop')");

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT /*+ BROADCAST(d) */ f.id FROM %s f JOIN %s d ON f.k = d.k"
                    + " WHERE d.label = 'keep'",
                fact, dim));

    assertEquals(Collections.singletonList(RowFactory.create(1)), joined.collectAsList());
    assertEquals(runtimeFilteringSupported(), LanceScanTestHelper.hasLanceRuntimeFilters(joined));
  }

  @Test
  public void testKeepsNoFilterForAnUnfilteredLanceTable() {
    String fact = createFact(true);
    String dim = createDimension();
    Dataset<Row> joined =
        spark.sql(String.format("SELECT f.id FROM %s f JOIN %s d ON f.k = d.k", fact, dim));

    assertEquals(4 * ROWS_PER_KEY, joined.collectAsList().size());
    assertFalse(LanceScanTestHelper.hasLanceRuntimeFilters(joined));
    // Its inferred k IS NOT NULL is not selective, so it stays pushed down only.
    assertFalse(hasFilterAbove(joined, dim));
  }

  @Test
  public void testKeepsNoFilterWhenTheOtherSideTakesNoRuntimeFilter() {
    // Without an index on k, the fact table's scan takes no runtime filter on it.
    String fact = createFact(false);
    String dim = createDimension();
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k WHERE d.label = 'three'", fact, dim));

    assertEquals(ROWS_PER_KEY, joined.collectAsList().size());
    assertFalse(LanceScanTestHelper.hasLanceRuntimeFilters(joined));
    assertFalse(hasFilterAbove(joined, dim));
  }

  @Test
  public void testKeepsThePushDownsOfOtherScans() {
    String fact = createFact(true);
    Dataset<Row> limited = spark.sql("SELECT * FROM " + fact + " WHERE k > 1 LIMIT 2");
    assertEquals(2, limited.collectAsList().size());
    assertEquals("Optional[2]", metadataOf(limited, fact).get("limit"));

    Dataset<Row> counted = spark.sql("SELECT count(*) FROM " + fact + " WHERE k = 3");
    assertEquals(ROWS_PER_KEY, counted.collectAsList().get(0).getLong(0));
    assertFalse(metadataOf(counted, fact).get("pushedAggregation").contains("empty"));

    // In a join, the side that is pruned keeps its own filter pushed down only.
    String dim = createDimension();
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k"
                    + " WHERE d.label = 'three' AND f.id >= 0",
                fact, dim));
    assertEquals(ROWS_PER_KEY, joined.collectAsList().size());
    assertTrue(metadataOf(joined, fact).get("whereConditions").contains("id"));
    assertFalse(hasFilterAbove(joined, fact));
  }

  /**
   * On Spark 4.1+, the join prunes the fact table's fragments, and the dimension table keeps its
   * filter for that. On older versions no runtime filter reaches the scan, and the dimension table
   * keeps its filter pushed down only.
   */
  private void assertPrunedOnlyWhereSupported(Dataset<Row> joined, String dim) {
    if (runtimeFilteringSupported()) {
      assertTrue(
          LanceScanTestHelper.hasLanceRuntimeFilters(joined),
          joined.queryExecution().executedPlan().toString());
      Map<String, String> metadata =
          LanceScanTestHelper.extractRuntimeFilteredLanceScanMetadata(joined);
      assertTrue(
          Integer.parseInt(metadata.get("numPartitions"))
              < Integer.parseInt(metadata.get("originalFragmentCount")),
          metadata.toString());
      assertTrue(hasFilterAbove(joined, dim));
    } else {
      assertFalse(LanceScanTestHelper.hasLanceRuntimeFilters(joined));
      assertFalse(hasFilterAbove(joined, dim));
    }
  }

  /** One fragment per key 0 to 4, with {@code ROWS_PER_KEY} rows each. */
  private String createFact(boolean indexed) {
    String table = catalog + ".default.fact_" + suffix();
    spark.sql("CREATE TABLE " + table + " (id INT, k INT, payload STRING) USING lance");
    for (int key = 0; key < FACT_FRAGMENTS; key++) {
      int k = key;
      String values =
          IntStream.range(0, ROWS_PER_KEY)
              .mapToObj(i -> String.format("(%d, %d, 'v_%d_%d')", k * 10 + i, k, k, i))
              .collect(Collectors.joining(","));
      spark.sql(String.format("INSERT INTO %s VALUES %s", table, values));
    }
    if (indexed) {
      spark.sql(String.format("ALTER TABLE %s CREATE INDEX k_idx USING zonemap (k)", table));
    }
    return table;
  }

  private String createDimension() {
    String table = catalog + ".default.dim_" + suffix();
    spark.sql("CREATE TABLE " + table + " (k INT, keep BOOLEAN, label STRING) USING lance");
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES (1, true, 'one_a'), (1, true, 'one_b'), (3, true, 'three'),"
                + " (4, false, 'four'), (NULL, true, 'null')",
            table));
    return table;
  }

  private static Map<String, String> metadataOf(Dataset<Row> df, String table) {
    return scala.collection.JavaConverters.mapAsJavaMap(
        ((LanceScan) scanOf(df, table).scan()).getMetaData());
  }

  private static BatchScanExec scanOf(Dataset<Row> df, String table) {
    String name = table.substring(table.lastIndexOf('.') + 1);
    List<SparkPlan> nodes = new ArrayList<>();
    collectAll(df.queryExecution().executedPlan(), nodes);
    for (SparkPlan node : nodes) {
      if (node instanceof BatchScanExec
          && ((BatchScanExec) node).scan() instanceof LanceScan
          && ((LanceScan) ((BatchScanExec) node).scan())
              .readOptions()
              .getDatasetUri()
              .contains(name)) {
        return (BatchScanExec) node;
      }
    }
    throw new AssertionError("No scan of " + table + " in " + df.queryExecution().executedPlan());
  }

  /** Whether Spark evaluates a Filter right above the scan of {@code table}. */
  private static boolean hasFilterAbove(Dataset<Row> df, String table) {
    BatchScanExec scan = scanOf(df, table);
    List<SparkPlan> nodes = new ArrayList<>();
    collectAll(df.queryExecution().executedPlan(), nodes);
    for (SparkPlan node : nodes) {
      if (node instanceof FilterExec && scanBelow(((FilterExec) node).child()) == scan) {
        return true;
      }
    }
    return false;
  }

  private static SparkPlan scanBelow(SparkPlan plan) {
    if (plan instanceof BatchScanExec) {
      return plan;
    }
    if (plan instanceof ColumnarToRowExec
        || plan instanceof InputAdapter
        || plan instanceof WholeStageCodegenExec) {
      return scanBelow((SparkPlan) plan.children().apply(0));
    }
    return null;
  }

  private static void collectAll(SparkPlan plan, List<SparkPlan> found) {
    if (plan instanceof AdaptiveSparkPlanExec) {
      collectAll(((AdaptiveSparkPlanExec) plan).executedPlan(), found);
      return;
    }
    if (plan instanceof QueryStageExec) {
      collectAll(((QueryStageExec) plan).plan(), found);
      return;
    }
    found.add(plan);
    for (int i = 0; i < plan.children().size(); i++) {
      collectAll((SparkPlan) plan.children().apply(i), found);
    }
  }

  private static String suffix() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
