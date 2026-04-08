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
import org.lance.Fragment;
import org.lance.ipc.ColumnOrdering;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;

import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.internal.connector.SupportsMetadata;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LanceScan
    implements Batch, Scan, SupportsMetadata, SupportsReportStatistics, Serializable {
  private static final long serialVersionUID = 947284762748623947L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceScan.class);

  private final StructType schema;
  private final LanceSparkReadOptions readOptions;
  private final Optional<String> whereConditions;
  private final Optional<Integer> limit;
  private final Optional<Integer> offset;
  private final Optional<List<ColumnOrdering>> topNSortOrders;
  private final Optional<Aggregation> pushedAggregation;
  private final Filter[] pushedFilters;
  private final LanceStatistics statistics;
  private final String scanId = UUID.randomUUID().toString();

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  public LanceScan(
      StructType schema,
      LanceSparkReadOptions readOptions,
      Optional<String> whereConditions,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders,
      Optional<Aggregation> pushedAggregation,
      Filter[] pushedFilters,
      LanceStatistics statistics,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties) {
    this.schema = schema;
    this.readOptions = readOptions;
    this.whereConditions = whereConditions;
    this.limit = limit;
    this.offset = offset;
    this.topNSortOrders = topNSortOrders;
    this.pushedAggregation = pushedAggregation;
    this.pushedFilters =
        pushedFilters != null ? Arrays.copyOf(pushedFilters, pushedFilters.length) : new Filter[0];
    this.statistics = statistics;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<FragmentRowRange> ranges;
    java.util.Map<Integer, Long> fragmentRowCounts;
    LanceSparkReadOptions resolvedReadOptions;
    try (Dataset dataset = openDataset(readOptions)) {
      List<Fragment> fragments = dataset.getFragments();
      ranges = new ArrayList<>(fragments.size());
      fragmentRowCounts = new HashMap<>(fragments.size());
      for (Fragment fragment : fragments) {
        int id = fragment.getId();
        ranges.add(FragmentRowRange.allRows(id));
        fragmentRowCounts.put(id, fragment.metadata().getNumRows());
      }
      // Pin the version for snapshot isolation — all workers read the same version
      resolvedReadOptions = readOptions.withVersion((int) dataset.getVersion().getId());
    }

    int maxRows = resolvedReadOptions.getMaxRowsPerPartition();
    ranges = splitRanges(ranges, fragmentRowCounts, maxRows);
    ranges = pruneByRowAddrFilters(ranges);
    ranges = pruneByLimit(ranges, fragmentRowCounts);

    List<List<FragmentRowRange>> partitions = binPackRanges(ranges, fragmentRowCounts, maxRows);

    return IntStream.range(0, partitions.size())
        .mapToObj(
            i ->
                new LanceInputPartition(
                    schema,
                    i,
                    partitions.get(i),
                    resolvedReadOptions,
                    whereConditions,
                    limit,
                    offset,
                    topNSortOrders,
                    pushedAggregation,
                    scanId,
                    initialStorageOptions,
                    namespaceImpl,
                    namespaceProperties))
        .toArray(InputPartition[]::new);
  }

  /**
   * Prunes ranges based on {@code _rowaddr} filters — dropping ranges whose fragment ID cannot
   * match the query predicate.
   *
   * <p>Fragment IDs match {@code (int)(rowAddr >>> 32)} — the same encoding used by {@link
   * org.lance.spark.join.FragmentAwareJoinUtils}.
   */
  private List<FragmentRowRange> pruneByRowAddrFilters(List<FragmentRowRange> allRanges) {
    java.util.Optional<Set<Integer>> targetFragmentIds =
        RowAddressFilterAnalyzer.extractTargetFragmentIds(pushedFilters);
    if (!targetFragmentIds.isPresent()) {
      return allRanges;
    }
    Set<Integer> allowedIds = targetFragmentIds.get();
    List<FragmentRowRange> pruned =
        allRanges.stream()
            .filter(range -> allowedIds.contains(range.getFragmentId()))
            .collect(Collectors.toList());
    if (pruned.size() < allRanges.size()) {
      LOG.debug(
          "Pruned by _rowaddr filters: {} of {} ranges retained, allowed fragment IDs: {}",
          pruned.size(),
          allRanges.size(),
          allowedIds);
    }
    return pruned;
  }

  /**
   * Prunes ranges based on pushed LIMIT using per-fragment row counts from the manifest.
   *
   * <p>When a LIMIT is pushed down without filters or TopN sort orders, we can use the per-fragment
   * logical row counts (which account for deletions) to determine how many ranges are needed to
   * satisfy the limit. This avoids scheduling hundreds of unnecessary tasks for large tables.
   *
   * <p>Correctness is guaranteed because Spark keeps a global {@code CollectLimit} on top (since
   * {@code isPartiallyPushed()} returns {@code true}).
   */
  private List<FragmentRowRange> pruneByLimit(
      List<FragmentRowRange> allRanges, java.util.Map<Integer, Long> fragmentRowCounts) {
    if (!limit.isPresent()
        || whereConditions.isPresent()
        || topNSortOrders.isPresent()
        || pushedAggregation.isPresent()
        || readOptions.getNearest() != null
        || fragmentRowCounts.isEmpty()) {
      return allRanges;
    }

    int requestedLimit = limit.get();
    long rowsAccumulated = 0;
    List<FragmentRowRange> pruned = new ArrayList<>();

    for (FragmentRowRange range : allRanges) {
      pruned.add(range);
      rowsAccumulated += getRowCount(range, fragmentRowCounts);
      if (rowsAccumulated >= requestedLimit) {
        break;
      }
    }

    if (pruned.size() < allRanges.size()) {
      LOG.debug(
          "Limit-based pruning: {} of {} ranges retained for LIMIT {} "
              + "(accumulated {} rows from selected ranges)",
          pruned.size(),
          allRanges.size(),
          requestedLimit,
          rowsAccumulated);
    }

    return pruned;
  }

  /**
   * Splits ranges whose row count exceeds {@code maxRows} into sub-ranges. Also materializes {@link
   * FragmentRowRange#ALL_ROWS} sentinels into concrete counts so bin-packing can size them.
   */
  private List<FragmentRowRange> splitRanges(
      List<FragmentRowRange> ranges, java.util.Map<Integer, Long> fragmentRowCounts, int maxRows) {
    if (maxRows <= 0) {
      return ranges;
    }
    List<FragmentRowRange> result = new ArrayList<>();
    for (FragmentRowRange range : ranges) {
      long total = getRowCount(range, fragmentRowCounts);
      if (total <= maxRows || total <= 0) {
        if (range.isFullFragment() && total > 0) {
          result.add(new FragmentRowRange(range.getFragmentId(), 0, total));
        } else {
          result.add(range);
        }
      } else {
        long offset = range.getOffset();
        long remaining = total;
        while (remaining > 0) {
          long chunk = Math.min(remaining, maxRows);
          result.add(new FragmentRowRange(range.getFragmentId(), offset, chunk));
          offset += chunk;
          remaining -= chunk;
        }
      }
    }
    return result;
  }

  /**
   * Packs ranges into partitions using first-fit decreasing bin packing. When {@code maxRows} is
   * disabled (≤ 0), falls back to one range per partition.
   */
  private List<List<FragmentRowRange>> binPackRanges(
      List<FragmentRowRange> ranges, java.util.Map<Integer, Long> fragmentRowCounts, int maxRows) {
    if (maxRows <= 0) {
      return ranges.stream().map(Collections::singletonList).collect(Collectors.toList());
    }

    List<FragmentRowRange> sorted = new ArrayList<>(ranges);
    sorted.sort(
        (a, b) ->
            Long.compare(getRowCount(b, fragmentRowCounts), getRowCount(a, fragmentRowCounts)));

    List<List<FragmentRowRange>> bins = new ArrayList<>();
    List<Long> binSizes = new ArrayList<>();
    for (FragmentRowRange range : sorted) {
      long size = getRowCount(range, fragmentRowCounts);
      int target = -1;
      for (int i = 0; i < bins.size(); i++) {
        if (binSizes.get(i) + size <= maxRows) {
          target = i;
          break;
        }
      }
      if (target >= 0) {
        bins.get(target).add(range);
        binSizes.set(target, binSizes.get(target) + size);
      } else {
        List<FragmentRowRange> bin = new ArrayList<>();
        bin.add(range);
        bins.add(bin);
        binSizes.add(size);
      }
    }
    return bins;
  }

  private long getRowCount(FragmentRowRange range, java.util.Map<Integer, Long> fragmentRowCounts) {
    if (!range.isFullFragment() && range.getNumRows() > 0) {
      return range.getNumRows();
    }
    Long count = fragmentRowCounts.get(range.getFragmentId());
    return count != null ? count : 0;
  }

  private static Dataset openDataset(LanceSparkReadOptions readOptions) {
    if (readOptions.hasNamespace()) {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .namespaceClient(readOptions.getNamespace())
          .tableId(readOptions.getTableId())
          .readOptions(readOptions.toReadOptions())
          .build();
    } else {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(readOptions.getDatasetUri())
          .readOptions(readOptions.toReadOptions())
          .build();
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceReaderFactory();
  }

  @Override
  public StructType readSchema() {
    if (pushedAggregation.isPresent()) {
      return new StructType().add("count", org.apache.spark.sql.types.DataTypes.LongType);
    }
    return schema;
  }

  @Override
  public Map<String, String> getMetaData() {
    scala.collection.immutable.Map<String, String> empty =
        scala.collection.immutable.Map$.MODULE$.empty();
    scala.collection.immutable.Map<String, String> result = empty;
    result = result.$plus(scala.Tuple2.apply("whereConditions", whereConditions.toString()));
    result = result.$plus(scala.Tuple2.apply("limit", limit.toString()));
    result = result.$plus(scala.Tuple2.apply("offset", offset.toString()));
    result = result.$plus(scala.Tuple2.apply("topNSortOrders", topNSortOrders.toString()));
    result = result.$plus(scala.Tuple2.apply("pushedAggregation", pushedAggregation.toString()));
    return result;
  }

  @Override
  public Statistics estimateStatistics() {
    return statistics;
  }

  private static class LanceReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      Preconditions.checkArgument(
          partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");
      return LanceRowPartitionReader.create((LanceInputPartition) partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      Preconditions.checkArgument(
          partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");

      LanceInputPartition lancePartition = (LanceInputPartition) partition;
      if (lancePartition.getPushedAggregation().isPresent()) {
        AggregateFunc[] aggFunc =
            lancePartition.getPushedAggregation().get().aggregateExpressions();
        if (aggFunc.length == 1 && aggFunc[0] instanceof CountStar) {
          return new LanceCountStarPartitionReader(lancePartition);
        }
      }

      return new LanceColumnarPartitionReader(lancePartition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }
  }
}
