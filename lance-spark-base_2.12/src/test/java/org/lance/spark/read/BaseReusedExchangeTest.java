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

import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;
import org.apache.spark.sql.execution.exchange.Exchange;
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that Spark's ReusedExchange optimization takes effect for Lance scans. Requires {@code
 * LanceScan} to implement {@code equals}/{@code hashCode} so that {@code BatchScanExec.equals()}
 * recognizes equivalent scans.
 */
public abstract class BaseReusedExchangeTest {
  private static SparkSession spark;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("reused-exchange-test")
            .master("local[2]")
            .config("spark.sql.exchange.reuse", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .getOrCreate();

    String dbPath = TestUtils.TestTable1Config.dbPath;
    spark
        .read()
        .format(LanceDataSource.name)
        .option(
            LanceSparkReadOptions.CONFIG_DATASET_URI,
            TestUtils.getDatasetUri(dbPath, TestUtils.TestTable1Config.datasetName))
        .load()
        .createOrReplaceTempView("lance_t");
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testSelfJoinReusesExchange() {
    // Both sides project the same columns — exchanges should be reused.
    Dataset<Row> result =
        spark.sql("SELECT a.x, a.y, b.x, b.y FROM lance_t a JOIN lance_t b ON a.x = b.x");
    result.collectAsList();

    SparkPlan plan = result.queryExecution().executedPlan();
    assertReusedExchangesReferenceExistingExchanges(plan);
  }

  @Test
  public void testMultiWayJoinReusesExchange() {
    // Three-way self-join — the exchange from the first scan should be reused for the other two.
    Dataset<Row> result =
        spark.sql(
            "SELECT a.x, b.x, c.x "
                + "FROM lance_t a "
                + "JOIN lance_t b ON a.x = b.x "
                + "JOIN lance_t c ON a.x = c.x");
    result.collectAsList();

    SparkPlan plan = result.queryExecution().executedPlan();
    assertReusedExchangesReferenceExistingExchanges(plan);
  }

  @Test
  public void testSelfJoinWithDifferentFiltersDoesNotReuseExchange() {
    // Different WHERE clauses produce different scans — no exchange reuse.
    Dataset<Row> result =
        spark.sql(
            "SELECT a.x, a.y, b.x, b.y "
                + "FROM (SELECT * FROM lance_t WHERE x > 0) a "
                + "JOIN (SELECT * FROM lance_t WHERE x <= 0) b "
                + "ON a.y = b.y");
    result.collectAsList();

    SparkPlan plan = result.queryExecution().executedPlan();
    List<ReusedExchangeExec> reusedExchanges = collectNodes(plan, ReusedExchangeExec.class);

    assertTrue(
        reusedExchanges.isEmpty(),
        "Self-join with different filters should NOT produce ReusedExchange. Plan:\n" + plan);
  }

  /**
   * Asserts that the plan contains at least one ReusedExchangeExec and that every
   * ReusedExchangeExec references an existing Exchange node. This follows the assertion pattern in
   * Spark's {@code ReuseExchangeAndSubquerySuite} and {@code ExchangeSuite}.
   */
  private void assertReusedExchangesReferenceExistingExchanges(SparkPlan plan) {
    Set<Integer> exchangeIds = collectExchangeIds(plan);
    List<ReusedExchangeExec> reusedExchanges = collectNodes(plan, ReusedExchangeExec.class);

    assertFalse(reusedExchanges.isEmpty(), "Plan should contain ReusedExchange. Plan:\n" + plan);

    for (ReusedExchangeExec reused : reusedExchanges) {
      int reusedChildId = reused.child().id();
      assertTrue(
          exchangeIds.contains(reusedChildId),
          "ReusedExchangeExec should reference an existing Exchange (id="
              + reusedChildId
              + "). Exchange IDs: "
              + exchangeIds
              + ". Plan:\n"
              + plan);
    }
  }

  /** Collects IDs of all Exchange nodes in the plan tree, traversing into AQE wrappers. */
  private static Set<Integer> collectExchangeIds(SparkPlan plan) {
    Set<Integer> ids = new java.util.HashSet<>();
    collectExchangeIdsRecursive(plan, ids);
    return ids;
  }

  private static void collectExchangeIdsRecursive(SparkPlan plan, Set<Integer> ids) {
    if (plan instanceof Exchange) {
      ids.add(plan.id());
    }
    for (SparkPlan child : allChildren(plan)) {
      collectExchangeIdsRecursive(child, ids);
    }
  }

  /**
   * Collects all nodes of the given type from the plan tree, traversing into AQE stages and query
   * stages. This mirrors the traversal logic of Spark's {@code AdaptiveSparkPlanHelper.collect}.
   */
  private static <T> List<T> collectNodes(SparkPlan plan, Class<T> nodeType) {
    List<T> result = new ArrayList<>();
    collectNodesRecursive(plan, nodeType, result);
    return result;
  }

  private static <T> void collectNodesRecursive(SparkPlan plan, Class<T> nodeType, List<T> result) {
    if (nodeType.isInstance(plan)) {
      result.add(nodeType.cast(plan));
    }
    for (SparkPlan child : allChildren(plan)) {
      collectNodesRecursive(child, nodeType, result);
    }
  }

  /**
   * Returns children of a plan node, traversing into AQE wrappers. Mirrors {@code
   * AdaptiveSparkPlanHelper.allChildren}.
   */
  private static List<SparkPlan> allChildren(SparkPlan plan) {
    List<SparkPlan> children = new ArrayList<>();
    if (plan instanceof AdaptiveSparkPlanExec) {
      children.add(((AdaptiveSparkPlanExec) plan).executedPlan());
    } else if (plan instanceof QueryStageExec) {
      children.add(((QueryStageExec) plan).plan());
    } else {
      scala.collection.Iterator<SparkPlan> it = plan.children().iterator();
      while (it.hasNext()) {
        children.add(it.next());
      }
    }
    return children;
  }
}
