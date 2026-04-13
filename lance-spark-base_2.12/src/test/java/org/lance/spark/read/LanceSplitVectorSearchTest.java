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
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LanceSplitVectorSearchTest {

  private static final int TEST_DATASET5_FRAGMENT_COUNT = 5;

  private static LanceSparkReadOptions optionsWithNearest(boolean useIndex) {
    Query.Builder builder = new Query.Builder();
    builder.setK(10);
    builder.setColumn("vector");
    builder.setKey(new float[] {1.0f, 2.0f, 3.0f});
    builder.setDistanceType(DistanceType.L2);
    builder.setUseIndex(useIndex);
    return LanceSparkReadOptions.builder()
        .datasetUri("s3://bucket/path")
        .nearest(builder.build())
        .build();
  }

  private static LanceSparkReadOptions optionsWithoutNearest() {
    return LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").build();
  }

  /** Builds read options backed by test_dataset5 (5 fragments, has 'vec' column). */
  private static LanceSparkReadOptions dataset5OptionsWithNearest(boolean useIndex) {
    float[] key = new float[32];
    for (int i = 0; i < 32; i++) {
      key[i] = (float) (i + 32);
    }
    Query.Builder builder = new Query.Builder();
    builder.setK(1);
    builder.setColumn("vec");
    builder.setKey(key);
    builder.setDistanceType(DistanceType.L2);
    builder.setUseIndex(useIndex);
    String datasetUri = TestUtils.getDatasetUri(TestUtils.TestTable1Config.dbPath, "test_dataset5");
    return LanceSparkReadOptions.builder().datasetUri(datasetUri).nearest(builder.build()).build();
  }

  // --- isIndexedVectorSearch ---

  @Test
  public void testIsIndexedVectorSearchWithUseIndexTrue() {
    LanceSparkReadOptions options = optionsWithNearest(true);
    assertTrue(LanceSplit.isIndexedVectorSearch(options));
  }

  @Test
  public void testIsIndexedVectorSearchWithUseIndexFalse() {
    LanceSparkReadOptions options = optionsWithNearest(false);
    assertFalse(LanceSplit.isIndexedVectorSearch(options));
  }

  @Test
  public void testIsIndexedVectorSearchWithoutNearest() {
    LanceSparkReadOptions options = optionsWithoutNearest();
    assertFalse(LanceSplit.isIndexedVectorSearch(options));
  }

  // --- planScan split strategy ---

  @Test
  public void testIndexedVectorSearchProducesSingleSplit() {
    // Indexed vector search (useIndex=true) must produce exactly one split so that
    // LanceFragmentScanner runs a single dataset-level scan instead of N redundant
    // per-fragment global index searches.
    LanceSplit.ScanPlanResult result =
        LanceSplit.planScan(dataset5OptionsWithNearest(/* useIndex= */ true));
    List<LanceSplit> splits = result.getSplits();
    assertEquals(1, splits.size(), "Indexed vector search must produce exactly one split");
    assertEquals(
        1,
        splits.get(0).getFragments().size(),
        "The single split must carry exactly one representative fragment ID");
  }

  @Test
  public void testBruteForceKNNProducesPerFragmentSplits() {
    // Brute-force KNN (useIndex=false) must keep per-fragment splits so that
    // each executor can scan its fragment in parallel.
    LanceSplit.ScanPlanResult result =
        LanceSplit.planScan(dataset5OptionsWithNearest(/* useIndex= */ false));
    List<LanceSplit> splits = result.getSplits();
    assertEquals(
        TEST_DATASET5_FRAGMENT_COUNT,
        splits.size(),
        "Brute-force KNN must produce one split per fragment");
    for (LanceSplit split : splits) {
      assertEquals(
          1, split.getFragments().size(), "Each brute-force KNN split must map to one fragment");
    }
  }
}
