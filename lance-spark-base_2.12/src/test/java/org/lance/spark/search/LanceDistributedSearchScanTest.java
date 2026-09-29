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
package org.lance.spark.search;

import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.spark.search.LanceSearchQuery.SearchType;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceDistributedSearchScanTest {
  @Test
  void legacyStatisticsUseSegmentMetricInsteadOfNestedStorageMetric() {
    Map<String, Object> storage = new HashMap<>();
    storage.put("metric_type", "L2");

    Map<String, Object> first = new HashMap<>();
    first.put("metric_type", "COSINE");
    first.put("sub_index", storage);

    Map<String, Object> second = new HashMap<>();
    second.put("metric_type", "cosine");

    Map<String, Object> statistics = new HashMap<>();
    statistics.put("indices", Arrays.asList(first, second));

    assertEquals(
        Collections.singleton("cosine"), LanceDistributedSearchScan.indexMetricTypes(statistics));
  }

  @Test
  void blankIndexMetricsAreIgnored() {
    Map<String, Object> empty = new HashMap<>();
    empty.put("metric_type", "");
    Map<String, Object> whitespace = new HashMap<>();
    whitespace.put("metric_type", "   ");
    Map<String, Object> statistics = new HashMap<>();
    statistics.put("indices", Arrays.asList(empty, whitespace));

    assertTrue(LanceDistributedSearchScan.indexMetricTypes(statistics).isEmpty());
  }

  @Test
  void explicitNprobesUsesFixedSemantics() {
    LanceSearchQuery query =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(Arrays.asList("ns", "table"))
            .namespaceImpl("dir")
            .vector(Collections.singletonList(0.0f))
            .vectorColumn("vector")
            .nprobes(7)
            .build();
    LanceDistributedSearchInputPartition partition =
        LanceDistributedSearchInputPartition.forFragment(new StructType(), query, 1);

    ScanOptions options = LanceDistributedSearchColumnarPartitionReader.buildScanOptions(partition);
    Query nearest = options.getNearest().get();
    assertEquals(7, nearest.getMinimumNprobes());
    assertEquals(Optional.of(7), nearest.getMaximumNprobes());
  }

  @Test
  void identifiesVectorIndexesByDetailsTypeInsteadOfConcreteIndexType() {
    assertTrue(
        LanceDistributedSearchScan.isVectorIndexTypeUrl("/lance.index.pb.VectorIndexDetails"));
    assertTrue(
        LanceDistributedSearchScan.isVectorIndexTypeUrl(
            "type.googleapis.com/lance.index.VectorIndexDetails"));
    assertFalse(
        LanceDistributedSearchScan.isVectorIndexTypeUrl(
            "type.googleapis.com/lance.index.pb.BTreeIndexDetails"));
    assertFalse(
        LanceDistributedSearchScan.isVectorIndexTypeUrl(
            "type.googleapis.com/example.MyVectorIndexDetails"));
  }

  @Test
  void computesOversampledLocalCandidateCount() {
    assertEquals(7, LanceDistributedSearchScan.localCandidateK(7, null));
    assertEquals(11, LanceDistributedSearchScan.localCandidateK(7, 1.5f));
  }

  @Test
  void rejectsInvalidOversampleFactor() {
    assertThrows(
        IllegalArgumentException.class, () -> LanceDistributedSearchScan.localCandidateK(7, 0.99f));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceDistributedSearchScan.localCandidateK(7, Float.NaN));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceDistributedSearchScan.localCandidateK(7, Float.POSITIVE_INFINITY));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceDistributedSearchScan.localCandidateK(Integer.MAX_VALUE, 2.0f));
  }

  @Test
  void segmentWithoutFragmentBitmapIsRejected() {
    assertEquals(
        new HashSet<>(Arrays.asList(1, 2)),
        LanceDistributedSearchScan.requireFragmentCoverage(
            "vec_idx", Optional.of(Arrays.asList(1, 2))));

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> LanceDistributedSearchScan.requireFragmentCoverage("vec_idx", Optional.empty()));
    assertTrue(error.getMessage().contains("without a fragment bitmap"), error.getMessage());
    assertTrue(error.getMessage().contains("vec_idx"), error.getMessage());
  }

  @Test
  void indexDiscoveryFailureIsFatalOnlyForFastSearch() {
    Exception cause = new IOException("metadata unavailable");

    LanceDistributedSearchScan.failFastSearchWhenIndexDiscoveryFails(false, cause);
    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> LanceDistributedSearchScan.failFastSearchWhenIndexDiscoveryFails(true, cause));
    assertEquals(cause, error.getCause());
    assertTrue(error.getMessage().contains("index discovery failed"));
  }
}
