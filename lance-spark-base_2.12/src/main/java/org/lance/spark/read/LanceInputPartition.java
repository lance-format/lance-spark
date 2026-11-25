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
import org.lance.ipc.FragmentSlice;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
   * Partition key row for storage-partitioned joins (SPJ). When a Lance sharding field is detected
   * from zonemap stats, this holds the single sharding value for this partition's fragment. Null
   * when no sharding column is detected (falls back to fragment-ID-based partitioning).
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

  /** Scan options for all selected ranges of the fragment opened by the reader. */
  public ScanOptions fragmentScanOptions(List<FragmentSplit> fragmentSplits) {
    ScanOptions.Builder options = newScanOptions(fragmentSplits);
    List<String> columns = projectedColumns();
    if (readOptions.getFullTextQuery() != null) {
      options.prefilter(true);
      if (Arrays.stream(schema.fields()).anyMatch(Utils::isScoreMetadataColumn)) {
        columns.add(LanceConstant.SCORE);
      }
    }
    options.columns(columns);
    options.batchSize(readOptions.getBatchSize());
    options.withRowId(schema.isEmpty() || schema.getFieldIndex(LanceConstant.ROW_ID).isDefined());
    // Blob references need the row address even when the user did not project it.
    options.withRowAddress(
        schema.getFieldIndex(LanceConstant.ROW_ADDRESS).isDefined()
            || Arrays.stream(schema.fields()).anyMatch(BlobUtils::isBlobReadColumn));
    limit.ifPresent(options::limit);
    offset.ifPresent(options::offset);
    topNSortOrders.ifPresent(options::setColumnOrderings);
    return options.build();
  }

  /** Scan options for counting matching rows across all fragments in this partition. */
  public ScanOptions countScanOptions() {
    return newScanOptions(lanceSplit.getFragmentSplits())
        .fragmentIds(lanceSplit.getFragments())
        .columns(Collections.emptyList())
        .withRowId(true)
        .build();
  }

  private ScanOptions.Builder newScanOptions(List<FragmentSplit> splits) {
    ScanOptions.Builder options = new ScanOptions.Builder();
    options.useScalarIndex(readOptions.isUseScalarIndex());
    whereCondition.ifPresent(options::filter);
    options.collectStats(true);
    if (readOptions.getFullTextQuery() != null) {
      options.fullTextQuery(readOptions.getFullTextQuery());
      options.disableScoringAutoprojection(true);
    }
    List<FragmentSlice> slices =
        splits.stream().flatMap(split -> split.getSlice().stream()).collect(Collectors.toList());
    if (!slices.isEmpty()) {
      options.fragmentSlices(slices);
    }
    return options;
  }

  private List<String> projectedColumns() {
    List<String> columns =
        Arrays.stream(schema.fields())
            .filter(field -> !Utils.isScoreMetadataColumn(field))
            .map(StructField::name)
            .filter(
                name ->
                    !name.equals(LanceConstant.FRAGMENT_ID)
                        && !name.equals(LanceConstant.ROW_ID)
                        && !name.equals(LanceConstant.ROW_ADDRESS)
                        && !name.equals(LanceConstant.ROW_CREATED_AT_VERSION)
                        && !name.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
                        && !name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
                        && !name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX))
            .collect(Collectors.toList());
    // Blob v1 virtual columns are derived from their base blob column, so scan the base column even
    // when the query only projects the virtual columns.
    for (StructField field : schema.fields()) {
      String baseName = BlobUtils.blobV1VirtualColumnBaseName(field.name());
      if (baseName != null && !columns.contains(baseName)) {
        columns.add(baseName);
      }
    }
    if (schema.getFieldIndex(LanceConstant.ROW_LAST_UPDATED_AT_VERSION).isDefined()) {
      columns.add(LanceConstant.ROW_LAST_UPDATED_AT_VERSION);
    }
    if (schema.getFieldIndex(LanceConstant.ROW_CREATED_AT_VERSION).isDefined()) {
      columns.add(LanceConstant.ROW_CREATED_AT_VERSION);
    }

    return columns;
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
   * <p>When a Lance sharding field is detected as partition-compatible from zonemap stats, this
   * returns the sharding value as a single-column InternalRow. This enables Spark's
   * storage-partitioned join (SPJ) protocol to co-locate partitions with the same value across
   * different data sources.
   *
   * <p>When no sharding column is configured, {@code outputPartitioning()} returns {@link
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
