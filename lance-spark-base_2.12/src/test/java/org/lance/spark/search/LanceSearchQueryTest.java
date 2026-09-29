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

import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.search.LanceSearchQuery.SearchType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LanceSearchQueryTest {
  @Test
  void toBuilderPreservesEveryVectorQueryField() {
    LanceSparkReadOptions readOptions = LanceSparkReadOptions.from("file:///tmp/query.lance");
    LanceSearchQuery original =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(Arrays.asList("ns", "table"))
            .namespaceImpl("dir")
            .namespaceProperties(Collections.singletonMap("root", "/tmp"))
            .readOptions(readOptions)
            .initialStorageOptions(Collections.singletonMap("allow_http", "true"))
            .outputColumns(Arrays.asList("id", "vector"))
            .topK(17)
            .offset(3)
            .version(41L)
            .filter("id > 2")
            .withRowId(true)
            .vector(Arrays.asList(1.0f, 2.0f))
            .vectorColumn("vector")
            .distanceType("cosine")
            .nprobes(7)
            .ef(31)
            .refineFactor(2)
            .lowerBound(0.1f)
            .upperBound(0.9f)
            .bypassVectorIndex(false)
            .fastSearch(true)
            .prefilter(true)
            .oversampleFactor(1.5f)
            .textQuery("hello")
            .searchColumns(Arrays.asList("title", "body"))
            .fullTextQueryJson("{\"match\":{\"column\":\"body\",\"terms\":\"hello\"}}")
            .build();

    LanceSearchQuery copy = original.toBuilder().build();

    assertEquals(original.getSearchType(), copy.getSearchType());
    assertEquals(original.getTableId(), copy.getTableId());
    assertEquals(original.getNamespaceImpl(), copy.getNamespaceImpl());
    assertEquals(original.getNamespaceProperties(), copy.getNamespaceProperties());
    assertSame(original.getReadOptions(), copy.getReadOptions());
    assertEquals(original.getInitialStorageOptions(), copy.getInitialStorageOptions());
    assertEquals(original.getOutputColumns(), copy.getOutputColumns());
    assertEquals(original.getK(), copy.getK());
    assertEquals(original.getOffset(), copy.getOffset());
    assertEquals(original.getVersion(), copy.getVersion());
    assertEquals(original.getFilter(), copy.getFilter());
    assertEquals(original.getWithRowId(), copy.getWithRowId());
    assertEquals(original.getVector(), copy.getVector());
    assertEquals(original.getVectorColumn(), copy.getVectorColumn());
    assertEquals(original.getDistanceType(), copy.getDistanceType());
    assertEquals(original.getNprobes(), copy.getNprobes());
    assertEquals(original.getEf(), copy.getEf());
    assertEquals(original.getRefineFactor(), copy.getRefineFactor());
    assertEquals(original.getLowerBound(), copy.getLowerBound());
    assertEquals(original.getUpperBound(), copy.getUpperBound());
    assertEquals(original.getBypassVectorIndex(), copy.getBypassVectorIndex());
    assertEquals(original.getFastSearch(), copy.getFastSearch());
    assertEquals(original.getPrefilter(), copy.getPrefilter());
    assertEquals(original.getOversampleFactor(), copy.getOversampleFactor());
    assertEquals(original.getTextQuery(), copy.getTextQuery());
    assertEquals(original.getSearchColumns(), copy.getSearchColumns());
    assertEquals(original.getFullTextQueryJson(), copy.getFullTextQueryJson());

    assertThrows(
        IllegalArgumentException.class, () -> original.toBuilder().oversampleFactor(0.5f).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> original.toBuilder().oversampleFactor(Float.NaN).build());
  }

  @Test
  void rejectsBypassVectorIndexWithFastSearch() {
    LanceSearchQuery.Builder builder =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(Arrays.asList("ns", "table"))
            .namespaceImpl("dir")
            .topK(5)
            .vector(Arrays.asList(1.0f, 2.0f))
            .bypassVectorIndex(true)
            .fastSearch(true);
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, builder::build);
    assertEquals("bypass_vector_index and fast_search cannot both be true", error.getMessage());
  }

  @Test
  void canonicalizesDistanceTypeAliases() {
    assertEquals("l2", vectorQueryWithDistanceType("euclidean").getDistanceType());
    assertEquals("dot", vectorQueryWithDistanceType("ip").getDistanceType());
    assertEquals("dot", vectorQueryWithDistanceType("inner_product").getDistanceType());
    assertEquals("dot", vectorQueryWithDistanceType("INNER_PRODUCT").getDistanceType());
    assertThrows(IllegalArgumentException.class, () -> vectorQueryWithDistanceType(""));
    assertThrows(IllegalArgumentException.class, () -> vectorQueryWithDistanceType("   "));
    assertThrows(
        IllegalArgumentException.class, () -> vectorQueryWithDistanceType("unsupported_metric"));
  }

  private static LanceSearchQuery vectorQueryWithDistanceType(String distanceType) {
    return LanceSearchQuery.builder(SearchType.VECTOR)
        .tableId(Arrays.asList("ns", "table"))
        .namespaceImpl("dir")
        .vector(Arrays.asList(1.0f, 2.0f))
        .distanceType(distanceType)
        .build();
  }
}
