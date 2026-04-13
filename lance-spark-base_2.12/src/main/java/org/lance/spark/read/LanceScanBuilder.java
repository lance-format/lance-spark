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
import org.lance.ManifestSummary;
import org.lance.index.IndexDescription;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.schema.LanceField;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LanceScanBuilder
    implements SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit,
        SupportsPushDownOffset,
        SupportsPushDownTopN,
        SupportsPushDownAggregates {
  private static final Logger LOG = LoggerFactory.getLogger(LanceScanBuilder.class);

  private final LanceSparkReadOptions readOptions;
  private StructType schema;

  private Filter[] pushedFilters = new Filter[0];
  private Optional<Integer> limit = Optional.empty();
  private Optional<Integer> offset = Optional.empty();
  private Optional<List<ColumnOrdering>> topNSortOrders = Optional.empty();
  private Optional<Aggregation> pushedAggregation = Optional.empty();
  private LanceLocalScan localScan = null;

  // Lazily opened dataset for reuse during scan building
  private Dataset lazyDataset = null;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  private final java.util.Map<String, String> tableProperties;

  static final String TABLE_OPT_PARTITION_COLUMNS = "lance.partition.columns";

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      java.util.Map<String, String> tableProperties) {
    this.schema = schema;
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableProperties = tableProperties != null ? tableProperties : Collections.emptyMap();
  }

  /**
   * Gets or opens a dataset for reuse during scan building. The dataset is lazily opened on first
   * access and reused for subsequent calls.
   */
  private Dataset getOrOpenDataset() {
    if (lazyDataset == null) {
      lazyDataset = Utils.openDatasetBuilder(readOptions).build();
    }
    return lazyDataset;
  }

  /** Closes the lazily opened dataset if it was opened. */
  private void closeLazyDataset() {
    if (lazyDataset != null) {
      lazyDataset.close();
      lazyDataset = null;
    }
  }

  @Override
  public Scan build() {
    // Return LocalScan if we have a metadata-only aggregation result
    if (localScan != null) {
      closeLazyDataset();
      return localScan;
    }

    // Get statistics from manifest summary before closing dataset
    ManifestSummary summary = getOrOpenDataset().getVersion().getManifestSummary();

    // Collect all columns that need zonemap stats: filter columns + partition column (if declared).
    Set<String> columnsToLoad = extractReferencedColumns(pushedFilters);
    String partitionColumn = tableProperties.get(TABLE_OPT_PARTITION_COLUMNS);
    if (partitionColumn != null && !partitionColumn.trim().isEmpty()) {
      partitionColumn = partitionColumn.trim();
      columnsToLoad.add(partitionColumn);
    } else {
      partitionColumn = null;
    }

    // Load zonemap stats for all requested columns in one pass.
    Map<String, List<ZoneStats>> zonemapStats = loadZonemapStats(getOrOpenDataset(), columnsToLoad);

    // Detect partition-compatible columns, gated on lance.partition.columns table property.
    // Currently a partitioned column is only valid if each fragment contains only a single
    // value for that column (i.e., all zonemap zones have min == max with the same value).
    ZonemapFragmentPruner.PartitionInfo partitionInfo = null;
    if (partitionColumn != null) {
      if (!zonemapStats.containsKey(partitionColumn)) {
        LOG.warn(
            "Partition column '{}' declared in {} has no zonemap index or stats;"
                + " partition detection disabled",
            partitionColumn,
            TABLE_OPT_PARTITION_COLUMNS);
      } else {
        Map<Integer, Comparable<?>> partValues =
            ZonemapFragmentPruner.computeFragmentPartitionValues(zonemapStats.get(partitionColumn))
                .orElse(null);
        if (partValues != null) {
          partitionInfo = new ZonemapFragmentPruner.PartitionInfo(partitionColumn, partValues);
          LOG.info(
              "Detected partition-compatible column '{}' with {} fragments",
              partitionColumn,
              partValues.size());
        }
      }
    }

    // Pre-compute fragment pruning so we can (a) estimate post-pruning statistics for
    // JoinSelection (BroadcastHashJoin vs SortMergeJoin) and (b) pass the cached result
    // to LanceScan to avoid re-computing during planInputPartitions().
    Set<Integer> survivingFragmentIds = null;
    if (pushedFilters.length > 0 && !zonemapStats.isEmpty()) {
      survivingFragmentIds =
          ZonemapFragmentPruner.pruneFragments(pushedFilters, zonemapStats).orElse(null);
    }

    LanceStatistics statistics;
    if (survivingFragmentIds != null) {
      statistics =
          LanceStatistics.estimatePostPruning(
              summary.getTotalRows(),
              summary.getTotalFilesSize(),
              summary.getTotalFragments(),
              survivingFragmentIds.size());
      LOG.debug(
          "Estimated post-pruning statistics: {} of {} fragments survive,"
              + " estimatedSize={}, estimatedRows={} (full: size={}, rows={})",
          survivingFragmentIds.size(),
          summary.getTotalFragments(),
          statistics.sizeInBytes(),
          statistics.numRows(),
          summary.getTotalFilesSize(),
          summary.getTotalRows());
    } else {
      statistics = new LanceStatistics(summary);
    }

    // Close the lazily opened dataset - it's no longer needed after build
    closeLazyDataset();

    Optional<String> whereCondition = FilterPushDown.compileFiltersToSqlWhereClause(pushedFilters);
    return new LanceScan(
        schema,
        readOptions,
        whereCondition,
        limit,
        offset,
        topNSortOrders,
        pushedAggregation,
        pushedFilters,
        statistics,
        zonemapStats,
        survivingFragmentIds,
        partitionInfo,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties);
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    if (!readOptions.isPushDownFilters()) {
      return filters;
    }
    // remove the code after fix this issue https://github.com/lance-format/lance/issues/3578
    boolean hasNestedField = false;
    for (StructField field : this.schema.fields()) {
      if (field.dataType() instanceof ArrayType) {
        ArrayType fieldType = (ArrayType) field.dataType();
        if (fieldType.elementType() instanceof StructType) {
          hasNestedField = true;
          break;
        }
      }
    }
    if (hasNestedField) {
      return filters;
    }
    Filter[][] processFilters = FilterPushDown.processFilters(filters);
    pushedFilters = processFilters[0];
    return processFilters[1];
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean pushLimit(int limit) {
    this.limit = Optional.of(limit);
    return true;
  }

  @Override
  public boolean pushOffset(int offset) {
    // Only one data file can be pushed down the offset.
    List<Integer> fragmentIds =
        getOrOpenDataset().getFragments().stream()
            .map(Fragment::getId)
            .collect(Collectors.toList());
    if (fragmentIds.size() == 1) {
      this.offset = Optional.of(offset);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isPartiallyPushed() {
    return true;
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    // The Order by operator will use compute thread in lance.
    // So it's better to have an option to enable it.
    if (!readOptions.isTopNPushDown()) {
      return false;
    }
    this.limit = Optional.of(limit);
    List<ColumnOrdering> topNSortOrders = new ArrayList<>();
    for (SortOrder sortOrder : orders) {
      ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
      builder.setNullFirst(sortOrder.nullOrdering() == NullOrdering.NULLS_FIRST);
      builder.setAscending(sortOrder.direction() == SortDirection.ASCENDING);
      if (!(sortOrder.expression() instanceof FieldReference)) {
        return false;
      }
      FieldReference reference = (FieldReference) sortOrder.expression();
      builder.setColumnName(reference.fieldNames()[0]);
      topNSortOrders.add(builder.build());
    }
    this.topNSortOrders = Optional.of(topNSortOrders);
    return true;
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    AggregateFunc[] funcs = aggregation.aggregateExpressions();
    if (aggregation.groupByExpressions().length > 0) {
      return false;
    }
    if (funcs.length == 1 && funcs[0] instanceof CountStar) {
      // Check if we can use metadata-based count (no filters pushed)
      if (pushedFilters.length == 0) {
        Optional<Long> metadataCount = getCountFromMetadata(getOrOpenDataset());
        if (metadataCount.isPresent()) {
          // Create LocalScan with pre-computed count result
          StructType countSchema = new StructType().add("count", DataTypes.LongType);
          InternalRow[] rows = new InternalRow[1];
          rows[0] = new GenericInternalRow(new Object[] {metadataCount.get()});
          this.localScan = new LanceLocalScan(countSchema, rows, readOptions.getDatasetUri());
          return true;
        }
      }
      // Fall back to scan-based count (with filters or metadata unavailable)
      this.pushedAggregation = Optional.of(aggregation);
      return true;
    }

    return false;
  }

  private static Optional<Long> getCountFromMetadata(Dataset dataset) {
    try {
      ManifestSummary summary = dataset.getVersion().getManifestSummary();
      return Optional.of(summary.getTotalRows());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Loads zonemap statistics for the requested columns. Only loads stats for columns that have a
   * zonemap index.
   */
  private Map<String, List<ZoneStats>> loadZonemapStats(Dataset dataset, Set<String> columns) {
    if (columns.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<String> zonemapColumns = findZonemapIndexedColumns(dataset);
    if (zonemapColumns.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, List<ZoneStats>> result = new HashMap<>();
    for (String col : columns) {
      if (zonemapColumns.contains(col)) {
        try {
          List<ZoneStats> stats = dataset.getZonemapStats(col);
          if (!stats.isEmpty()) {
            result.put(col, stats);
            LOG.debug("Loaded {} zonemap zones for column '{}'", stats.size(), col);
          }
        } catch (Exception e) {
          LOG.debug("Failed to load zonemap stats for column '{}': {}", col, e.getMessage());
        }
      }
    }

    if (!result.isEmpty()) {
      LOG.debug("Loaded zonemap stats for {} columns: {}", result.size(), result.keySet());
    }

    return result;
  }

  private Set<String> findZonemapIndexedColumns(Dataset dataset) {
    Set<String> columns = new HashSet<>();
    try {
      Map<Integer, String> fieldIdToName = new HashMap<>();
      for (LanceField field : dataset.getLanceSchema().fields()) {
        fieldIdToName.put(field.getId(), field.getName());
      }

      for (IndexDescription idx : dataset.describeIndices()) {
        if ("ZONEMAP".equalsIgnoreCase(idx.getIndexType())) {
          for (int fieldId : idx.getFieldIds()) {
            String name = fieldIdToName.get(fieldId);
            if (name != null) {
              columns.add(name);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to query zonemap indexes: {}", e.getMessage());
    }
    return columns;
  }

  private static Set<String> extractReferencedColumns(Filter[] filters) {
    Set<String> columns = new HashSet<>();
    for (Filter filter : filters) {
      for (String attr : filter.references()) {
        columns.add(attr);
      }
    }
    return columns;
  }
}
