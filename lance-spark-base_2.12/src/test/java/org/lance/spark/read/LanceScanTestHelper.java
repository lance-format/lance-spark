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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.internal.connector.SupportsMetadata;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Shared test utilities for inspecting the executed Lance scan plan. */
public final class LanceScanTestHelper {

  private LanceScanTestHelper() {}

  /**
   * Walks the executed plan tree to find a {@code BatchScanExec} containing a {@code LanceScan},
   * then returns its {@code getMetaData()} map as a Java map. Returns {@code null} if no such node
   * is found.
   */
  public static Map<String, String> extractLanceScanMetadata(Dataset<Row> ds) {
    return findLanceScanMetadata(ds.queryExecution().executedPlan());
  }

  public static Map<String, String> extractRuntimeFilteredLanceScanMetadata(Dataset<Row> ds) {
    LanceScan scan = extractRuntimeFilteredLanceScan(ds);
    return scan == null ? null : JavaConverters.mapAsJavaMap(scan.getMetaData());
  }

  /** Returns the Lance scan that Spark supplied runtime filters to, or null when there is none. */
  public static LanceScan extractRuntimeFilteredLanceScan(Dataset<Row> ds) {
    return findLanceScan(ds.queryExecution().executedPlan(), true);
  }

  public static boolean hasLanceRuntimeFilters(Dataset<Row> ds) {
    return extractRuntimeFilteredLanceScan(ds) != null;
  }

  public static LanceScan extractLanceScan(Dataset<Row> ds) {
    return findLanceScan(ds.queryExecution().executedPlan(), false);
  }

  private static Map<String, String> findLanceScanMetadata(SparkPlan plan) {
    if (plan instanceof BatchScanExec) {
      BatchScanExec bse = (BatchScanExec) plan;
      if (bse.scan() instanceof SupportsMetadata) {
        scala.collection.immutable.Map<String, String> scalaMeta =
            ((SupportsMetadata) bse.scan()).getMetaData();
        return JavaConverters.mapAsJavaMap(scalaMeta);
      }
    }
    for (SparkPlan child : childrenOf(plan)) {
      Map<String, String> result = findLanceScanMetadata(child);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  private static LanceScan findLanceScan(SparkPlan plan, boolean withRuntimeFilters) {
    if (plan instanceof BatchScanExec) {
      BatchScanExec bse = (BatchScanExec) plan;
      if (bse.scan() instanceof LanceScan
          && (!withRuntimeFilters || !bse.runtimeFilters().isEmpty())) {
        return (LanceScan) bse.scan();
      }
    }
    for (SparkPlan child : childrenOf(plan)) {
      LanceScan result = findLanceScan(child, withRuntimeFilters);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  /** Children of a plan node, looking through AQE wrappers and query stages. */
  private static List<SparkPlan> childrenOf(SparkPlan plan) {
    if (plan instanceof AdaptiveSparkPlanExec) {
      return Collections.singletonList(((AdaptiveSparkPlanExec) plan).executedPlan());
    }
    if (plan instanceof QueryStageExec) {
      return Collections.singletonList(((QueryStageExec) plan).plan());
    }
    return JavaConverters.seqAsJavaList(plan.children());
  }
}
