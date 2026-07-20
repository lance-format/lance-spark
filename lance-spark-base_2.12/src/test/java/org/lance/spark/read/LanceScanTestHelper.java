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
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.internal.connector.SupportsMetadata;

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

  private static Map<String, String> findLanceScanMetadata(SparkPlan plan) {
    if (plan instanceof BatchScanExec) {
      BatchScanExec bse = (BatchScanExec) plan;
      if (bse.scan() instanceof SupportsMetadata) {
        scala.collection.immutable.Map<String, String> scalaMeta =
            ((SupportsMetadata) bse.scan()).getMetaData();
        return scala.collection.JavaConverters.mapAsJavaMap(scalaMeta);
      }
    }
    for (int i = 0; i < plan.children().size(); i++) {
      Map<String, String> result = findLanceScanMetadata((SparkPlan) plan.children().apply(i));
      if (result != null) {
        return result;
      }
    }
    return null;
  }
}
