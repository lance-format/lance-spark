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

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class LanceSearchInputPartition implements InputPartition {
  private static final long serialVersionUID = -38612098237192389L;

  private final StructType schema;
  private final LanceSearchQuery query;

  private final boolean distributed;
  private final List<Integer> fragmentIds;
  private final List<UUID> indexSegments;
  private final LanceSparkReadOptions readOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final Map<String, String> initialStorageOptions;

  /** Non-distributed: namespace.queryTable() path. */
  public LanceSearchInputPartition(StructType schema, LanceSearchQuery query) {
    this.schema = schema;
    this.query = query;
    this.distributed = false;
    this.fragmentIds = Collections.emptyList();
    this.indexSegments = Collections.emptyList();
    this.readOptions = null;
    this.namespaceImpl = null;
    this.namespaceProperties = Collections.emptyMap();
    this.initialStorageOptions = Collections.emptyMap();
  }

  /**
   * Distributed: one unit (= one segment OR one uncovered fragment) executed on a Spark task.
   * Exactly one of {@code fragmentIds}, {@code indexSegments} must be non-empty.
   */
  public LanceSearchInputPartition(
      StructType schema,
      LanceSearchQuery query,
      List<Integer> fragmentIds,
      List<UUID> indexSegments,
      LanceSparkReadOptions readOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      Map<String, String> initialStorageOptions) {
    if (readOptions == null) {
      throw new IllegalArgumentException("readOptions must be non-null in distributed mode");
    }
    if (namespaceImpl == null) {
      throw new IllegalArgumentException("namespaceImpl must be non-null in distributed mode");
    }
    boolean hasFragments = fragmentIds != null && !fragmentIds.isEmpty();
    boolean hasSegments = indexSegments != null && !indexSegments.isEmpty();
    if (hasFragments == hasSegments) {
      throw new IllegalArgumentException(
          "Exactly one of fragmentIds or indexSegments must be non-empty");
    }
    this.schema = schema;
    this.query = query;
    this.distributed = true;
    this.fragmentIds =
        fragmentIds == null ? Collections.emptyList() : Collections.unmodifiableList(fragmentIds);
    this.indexSegments =
        indexSegments == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(indexSegments);
    this.readOptions = readOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties =
        namespaceProperties == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(namespaceProperties));
    this.initialStorageOptions =
        initialStorageOptions == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(initialStorageOptions));
  }

  public StructType getSchema() {
    return schema;
  }

  public LanceSearchQuery getQuery() {
    return query;
  }

  public boolean isDistributed() {
    return distributed;
  }

  public List<Integer> getFragmentIds() {
    return fragmentIds;
  }

  public List<UUID> getIndexSegments() {
    return indexSegments;
  }

  public LanceSparkReadOptions getReadOptions() {
    return readOptions;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  public Map<String, String> getInitialStorageOptions() {
    return initialStorageOptions;
  }
}
