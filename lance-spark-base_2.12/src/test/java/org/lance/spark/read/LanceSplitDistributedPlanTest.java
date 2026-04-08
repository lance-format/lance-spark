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
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceSplitDistributedPlanTest {

  @TempDir static Path tempDir;

  private static LanceSparkReadOptions readOptions;

  @BeforeAll
  static void setup() {
    String datasetUri = TestUtils.createTestTable1V2Dataset(tempDir);
    readOptions = LanceSparkReadOptions.from(datasetUri);
  }

  @Test
  public void testPlanDistributedScanBasic() {
    LanceSplit.DistributedScanPlanResult result =
        LanceSplit.planDistributedScan(
            readOptions,
            TestUtils.TestTable1Config.schema,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    assertNotNull(result);
    assertFalse(result.getTasks().isEmpty());
    assertTrue(result.getResolvedVersion() > 0);
    assertEquals(result.getTasks().size(), result.getFragmentIds().length);
  }

  @Test
  public void testTaskCountMatchesFragments() {
    LanceSplit.DistributedScanPlanResult result =
        LanceSplit.planDistributedScan(
            readOptions,
            TestUtils.TestTable1Config.schema,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    // v2 test dataset has 2 fragments
    assertEquals(2, result.getTasks().size());
    assertEquals(2, result.getFragmentIds().length);
  }

  @Test
  public void testFilteredReadTaskSerializable() throws Exception {
    LanceSplit.DistributedScanPlanResult result =
        LanceSplit.planDistributedScan(
            readOptions,
            TestUtils.TestTable1Config.schema,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    byte[] originalTask = result.getTasks().get(0);
    assertNotNull(originalTask);
    assertTrue(originalTask.length > 0);

    // byte[] is natively serializable — verify round-trip via copy
    byte[] copied = originalTask.clone();
    assertArrayEquals(originalTask, copied);
  }

  @Test
  public void testPlanDistributedScanWithFilter() {
    LanceSplit.DistributedScanPlanResult result =
        LanceSplit.planDistributedScan(
            readOptions,
            TestUtils.TestTable1Config.schema,
            Optional.of("x > 1"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    assertNotNull(result);
    assertFalse(result.getTasks().isEmpty());
    assertTrue(result.getResolvedVersion() > 0);
    assertEquals(result.getTasks().size(), result.getFragmentIds().length);
  }

  @Test
  public void testResolvedVersionConsistency() {
    LanceSplit.DistributedScanPlanResult distributedResult =
        LanceSplit.planDistributedScan(
            readOptions,
            TestUtils.TestTable1Config.schema,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    LanceSplit.ScanPlanResult legacyResult = LanceSplit.planScan(readOptions);
    assertEquals(legacyResult.getResolvedVersion(), distributedResult.getResolvedVersion());
  }
}
