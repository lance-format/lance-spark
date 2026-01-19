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

import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceDataset;
import org.lance.spark.partition.LancePartitionConfig;
import org.lance.spark.partition.Partition;
import org.lance.spark.partition.PartitionDescriptor;
import org.lance.spark.partition.PartitionUtils;
import org.lance.spark.utils.Optional;

import com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** ScanBuilder for partitioned Lance dataset. */
public class PartitionedLanceScanBuilder
    implements SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit,
        SupportsPushDownOffset,
        SupportsPushDownTopN,
        SupportsPushDownAggregates {
  private StructType schema;
  private final List<PartitionDescriptor> partitionDescriptors;
  private final LancePartitionConfig config;

  private Filter[] nonPartitionPushedFilters = new Filter[0];
  private final List<Filter> partitionFilters = new ArrayList<>();
  private Optional<Integer> limit = Optional.empty();
  private Optional<Integer> offset = Optional.empty();
  private SortOrder[] pushedTopNOrders = null;

  public PartitionedLanceScanBuilder(
      StructType schema,
      List<PartitionDescriptor> partitionDescriptors,
      LancePartitionConfig config) {
    this.schema = schema;
    this.partitionDescriptors = partitionDescriptors;
    this.config = config;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    if (!requiredSchema.isEmpty()) {
      this.schema = requiredSchema;
    }
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    // Split filters into:
    // - partition filters.
    // - nonPartitionPushedFilters filters.
    // - evaluatedFilters = partition filters + nonPartitionNonPushedFilters.
    List<Filter> nonPartitionFilters = new ArrayList<>();
    for (Filter f : filters) {
      if (!PartitionUtils.isPruneAvailable(partitionDescriptors, f)) {
        partitionFilters.add(f);
      } else {
        nonPartitionFilters.add(f);
      }
    }

    Filter[][] processed =
        FilterPushDown.processFilters(nonPartitionFilters.toArray(new Filter[0]));
    this.nonPartitionPushedFilters = processed[0];

    List<Filter> evaluatedFilters = Lists.newArrayList(processed[1]);
    evaluatedFilters.addAll(partitionFilters);
    return evaluatedFilters.toArray(new Filter[0]);
  }

  @Override
  public Filter[] pushedFilters() {
    return nonPartitionPushedFilters;
  }

  @Override
  public boolean pushLimit(int limit) {
    this.limit = Optional.of(limit);
    return true;
  }

  @Override
  public boolean pushOffset(int offset) {
    // Offset is not guaranteed to be fully pushed at partitioned level.
    this.offset = Optional.of(offset);
    return false;
  }

  @Override
  public boolean isPartiallyPushed() {
    return true;
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    if (orders == null || orders.length == 0) {
      return false;
    }
    for (SortOrder sortOrder : orders) {
      if (!(sortOrder.expression() instanceof FieldReference)) {
        return false;
      }
    }

    this.limit = Optional.of(limit);
    this.pushedTopNOrders = orders;
    return true;
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    return false;
  }

  @Override
  public Scan build() {
    Partition root = loadPartition();

    // Compute pruned leaves
    Set<LanceDataset> prunedLeaves = new HashSet<>(root.leaves());
    for (Filter pf : partitionFilters) {
      List<LanceDataset> matched = root.filter(pf);
      prunedLeaves.retainAll(matched);
    }

    // Construct child scans
    List<LanceScan> childScans = new ArrayList<>();
    for (LanceDataset ds : prunedLeaves) {
      LanceScanBuilder child = new LanceScanBuilder(schema, ds.readOptions());
      if (nonPartitionPushedFilters.length > 0) {
        child.pushFilters(nonPartitionPushedFilters);
      }
      if (limit.isPresent()) {
        child.pushLimit(limit.get());
      }
      if (offset.isPresent()) {
        child.pushOffset(offset.get());
      }
      if (pushedTopNOrders != null) {
        child.pushTopN(pushedTopNOrders, (limit.isPresent() ? limit.get() : Integer.MAX_VALUE));
      }
      childScans.add((LanceScan) child.build());
    }

    return new PartitionedLanceScan(schema, childScans);
  }

  private Partition loadPartition() {
    try {
      LanceNamespace namespace = PartitionUtils.createNamespace(config.options());
      return PartitionUtils.loadPartition(
          namespace, partitionDescriptors, schema, config.options());
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Failed to load physical partition", e);
    }
  }
}
