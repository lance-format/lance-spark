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

import org.lance.Dataset;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Utils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LanceSplitTest {

  @Test
  public void testPlanScanReturnsNonEmptySplits() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    assertFalse(result.getSplits().isEmpty());
    assertTrue(result.getResolvedVersion() > 0);
  }

  @Test
  public void testPlanScanEachSplitHasSingleFragment() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    for (LanceSplit split : result.getSplits()) {
      assertEquals(1, split.getFragments().size());
    }
  }

  @Test
  public void testPlanScanReturnsFragmentRowCounts() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    assertFalse(result.getFragmentRowCounts().isEmpty());
    // Every fragment in splits should have a row count entry
    for (LanceSplit split : result.getSplits()) {
      for (int fragmentId : split.getFragments()) {
        assertTrue(
            result.getFragmentRowCounts().containsKey(fragmentId),
            "Missing row count for fragment " + fragmentId);
        assertTrue(
            result.getFragmentRowCounts().get(fragmentId) >= 0,
            "Row count should be non-negative for fragment " + fragmentId);
      }
    }
  }

  /**
   * Contract test: {@link LanceSplit#planScan(Dataset)} must not close the externally-owned dataset
   * handle. {@link LanceScanBuilder#build()} relies on this so it can keep using the single open
   * handle for both manifest/zonemap loading and split planning.
   */
  @Test
  public void testPlanScanWithDatasetDoesNotCloseExternalDataset() {
    try (Dataset dataset =
        Utils.openDatasetBuilder(TestUtils.TestTable1Config.readOptions).build()) {
      LanceSplit.ScanPlanResult result = LanceSplit.planScan(dataset);
      assertFalse(result.getSplits().isEmpty());

      // If planScan(Dataset) accidentally closed the handle, subsequent native calls would
      // throw. Re-issue native calls to assert the dataset is still usable.
      assertFalse(dataset.getFragments().isEmpty());
      assertTrue(dataset.getVersion().getId() > 0);
    }
  }

  /**
   * Contract test: the long-typed resolved version returned by {@link LanceSplit#planScan(Dataset)}
   * must round-trip through {@link org.lance.spark.LanceSparkReadOptions#withVersion(long)} without
   * truncation. This guards against silently casting to {@code int}, which would corrupt the
   * snapshot-isolation guarantee for long-lived high-write-frequency datasets.
   */
  @Test
  public void testResolvedVersionRoundTripsAsLong() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    long resolved = result.getResolvedVersion();
    assertEquals(
        resolved,
        TestUtils.TestTable1Config.readOptions.withVersion(resolved).getVersion().longValue());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGenerateLanceSplitsDeprecated() {
    List<LanceSplit> splits =
        LanceSplit.generateLanceSplits(TestUtils.TestTable1Config.readOptions);
    assertFalse(splits.isEmpty());
  }
}
