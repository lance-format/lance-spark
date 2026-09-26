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

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * One unit of a distributed search, run by one Spark task: either a single segment of a vector
 * index, or a single fragment the index does not cover. The two are mutually exclusive by
 * construction - use {@link #forIndexSegment} or {@link #forFragment}.
 */
public class LanceDistributedSearchInputPartition implements InputPartition {
  private static final long serialVersionUID = -70163928476152093L;

  private final StructType schema;
  private final LanceSearchQuery query;
  private final List<Integer> fragmentIds;
  private final List<UUID> indexSegments;

  private LanceDistributedSearchInputPartition(
      StructType schema,
      LanceSearchQuery query,
      List<Integer> fragmentIds,
      List<UUID> indexSegments) {
    this.schema = schema;
    this.query = query;
    this.fragmentIds = fragmentIds;
    this.indexSegments = indexSegments;
  }

  /** An indexed unit: search one segment of a vector index through {@code indexSegments(...)}. */
  public static LanceDistributedSearchInputPartition forIndexSegment(
      StructType schema, LanceSearchQuery query, UUID segmentUuid) {
    return new LanceDistributedSearchInputPartition(
        schema,
        query,
        Collections.emptyList(),
        Collections.singletonList(Objects.requireNonNull(segmentUuid, "segmentUuid")));
  }

  /** A fallback unit: flat KNN over one fragment no index segment covers. */
  public static LanceDistributedSearchInputPartition forFragment(
      StructType schema, LanceSearchQuery query, int fragmentId) {
    return new LanceDistributedSearchInputPartition(
        schema, query, Collections.singletonList(fragmentId), Collections.emptyList());
  }

  public StructType getSchema() {
    return schema;
  }

  public LanceSearchQuery getQuery() {
    return query;
  }

  public List<Integer> getFragmentIds() {
    return fragmentIds;
  }

  public List<UUID> getIndexSegments() {
    return indexSegments;
  }
}
