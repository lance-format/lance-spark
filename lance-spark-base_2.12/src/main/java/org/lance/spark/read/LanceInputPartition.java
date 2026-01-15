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

import org.lance.ipc.ColumnOrdering;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class LanceInputPartition implements InputPartition {
  private static final long serialVersionUID = 4723894723984723984L;

  private final StructType schema;
  private final int partitionId;
  private final LanceSplit lanceSplit;
  private final LanceSparkReadOptions readOptions;
  private final Optional<String> whereCondition;
  private final Optional<Integer> limit;
  private final Optional<Integer> offset;
  private final Optional<List<ColumnOrdering>> topNSortOrders;
  private final Optional<Aggregation> pushedAggregation;
  private final String scanId;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;

  public LanceInputPartition(
      StructType schema,
      int partitionId,
      LanceSplit lanceSplit,
      LanceSparkReadOptions readOptions,
      Optional<String> whereCondition,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders,
      Optional<Aggregation> pushedAggregation,
      String scanId,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    this.schema = schema;
    this.partitionId = partitionId;
    this.lanceSplit = lanceSplit;
    this.readOptions = readOptions;
    this.whereCondition = whereCondition;
    this.limit = limit;
    this.offset = offset;
    this.topNSortOrders = topNSortOrders;
    this.pushedAggregation = pushedAggregation;
    this.scanId = scanId;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
  }

  public StructType getSchema() {
    return schema;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public LanceSplit getLanceSplit() {
    return lanceSplit;
  }

  public LanceSparkReadOptions getReadOptions() {
    return readOptions;
  }

  public Optional<String> getWhereCondition() {
    return whereCondition;
  }

  public Optional<Integer> getLimit() {
    return limit;
  }

  public Optional<Integer> getOffset() {
    return offset;
  }

  public Optional<List<ColumnOrdering>> getTopNSortOrders() {
    return topNSortOrders;
  }

  public Optional<Aggregation> getPushedAggregation() {
    return pushedAggregation;
  }

  public String getScanId() {
    return scanId;
  }

  public Map<String, String> getInitialStorageOptions() {
    return initialStorageOptions;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }
}
