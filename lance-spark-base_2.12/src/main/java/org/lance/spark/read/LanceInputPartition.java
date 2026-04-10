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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class LanceInputPartition implements HasPartitionKey {
  private static final long serialVersionUID = 4723894723984723985L;

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

  /**
   * Partition key row for storage-partitioned joins (SPJ). When a partition-compatible column is
   * detected via zonemap stats, this holds the single partition value for this partition's
   * fragment. Null when no partition column is detected (falls back to fragment-ID-based
   * partitioning).
   */
  private final InternalRow partitionKeyRow;

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
      Map<String, String> namespaceProperties,
      InternalRow partitionKeyRow) {
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
    this.partitionKeyRow = partitionKeyRow;
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

  /**
   * Returns the partition key for this input partition.
   *
   * <p>When a partition column is declared via {@code lance.partition.columns} table property and
   * detected as partition-compatible (zonemap stats showing min==max per fragment), this returns
   * the partition value as a single-column InternalRow. This enables Spark's storage-partitioned
   * join (SPJ) protocol to co-locate partitions with the same value across different data sources.
   *
   * <p>When no partition column is configured, {@code outputPartitioning()} returns {@link
   * org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning} and Spark should not call
   * this method. Returns an empty row as a defensive fallback.
   */
  @Override
  public InternalRow partitionKey() {
    if (partitionKeyRow != null) {
      return partitionKeyRow;
    }
    return new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(new Object[] {});
  }
}
