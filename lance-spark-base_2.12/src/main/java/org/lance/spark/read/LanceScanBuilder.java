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
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.schema.LanceField;
import org.lance.spark.LanceConstant;
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
import org.apache.spark.sql.types.DataTypes;
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
  private final StructType fullSchema;
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

  static final String CONF_REPORTING_ENABLED = "spark.lance.partition.reporting.enabled";
  static final String CONF_REPORTING_MAX_PARTITIONS =
      "spark.lance.partition.reporting.maxPartitions";
  static final int DEFAULT_REPORTING_MAX_PARTITIONS = 10_000;

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      java.util.Map<String, String> tableProperties) {
    this.fullSchema = schema;
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

    try {
      // Get statistics from manifest summary before closing dataset
      ManifestSummary summary = getOrOpenDataset().getVersion().getManifestSummary();

      // Parse and validate partition columns from TBLPROPERTIES. Nested paths and non-whitelisted
      // types are rejected here with a WARN; detection falls through to null PartitionInfo.
      List<String> partitionColumns =
          parsePartitionColumns(tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_COLUMNS));

      // Collect all columns that need zonemap stats: filter columns + declared partition columns.
      Set<String> columnsToLoad = extractReferencedColumns(pushedFilters);
      columnsToLoad.addAll(partitionColumns);

      // Load zonemap stats for all requested columns in one pass.
      Map<String, List<ZoneStats>> zonemapStats =
          loadZonemapStats(getOrOpenDataset(), columnsToLoad);

      // Reject-all policy: if any declared column fails detection (missing stats, non-constant
      // values, coverage mismatch), the whole scan falls back to UnknownPartitioning so SPJ
      // symmetry with the joined counterpart is preserved.
      ZonemapFragmentPruner.PartitionInfo partitionInfo =
          detectPartitioning(partitionColumns, zonemapStats);

      // Pre-compute fragment pruning so we can (a) estimate post-pruning statistics for
      // JoinSelection (BroadcastHashJoin vs SortMergeJoin) and (b) pass the cached result
      // to LanceScan to avoid re-computing during planInputPartitions().
      Set<Integer> survivingFragmentIds = null;
      if (pushedFilters.length > 0 && !zonemapStats.isEmpty()) {
        survivingFragmentIds =
            ZonemapFragmentPruner.pruneFragments(pushedFilters, zonemapStats).orElse(null);
      }

      // Filter pushdown may have narrowed the surviving fragment set; restrict PartitionInfo so
      // the partition count reported via SPJ matches the post-pushdown size. restrictTo clears
      // the softCapped flag (cap is size-dependent) — re-apply if the restricted size still
      // exceeds the cap.
      if (partitionInfo != null && survivingFragmentIds != null) {
        partitionInfo = partitionInfo.restrictTo(survivingFragmentIds);
        if (partitionInfo.size() == 0) {
          partitionInfo = null;
        } else if (partitionInfo.size() > readMaxReportedPartitionsConf()) {
          partitionInfo = partitionInfo.withSoftCapped();
        }
      }

      // Scale rows and full size by the zonemap fragment-pruning ratio first, then let
      // LanceStatistics.estimateProjected apply the column-width ratio on top
      // (when the projected schema is narrower than the full schema).
      long projectedRows = summary.getTotalRows();
      long projectedFullSize = summary.getTotalFilesSize();
      if (survivingFragmentIds != null && summary.getTotalFragments() > 0) {
        double ratio = (double) survivingFragmentIds.size() / summary.getTotalFragments();
        projectedRows = (long) (projectedRows * ratio);
        projectedFullSize = (long) (projectedFullSize * ratio);
      }
      LanceStatistics statistics =
          LanceStatistics.estimateProjected(projectedRows, projectedFullSize, fullSchema, schema);
      if (survivingFragmentIds != null) {
        LOG.debug(
            "Scan statistics after pruning: {} of {} fragments survive,"
                + " estimatedSize={}, estimatedRows={} (full: size={}, rows={})",
            survivingFragmentIds.size(),
            summary.getTotalFragments(),
            statistics.sizeInBytes(),
            statistics.numRows(),
            summary.getTotalFilesSize(),
            summary.getTotalRows());
      }

      Optional<String> whereCondition =
          FilterPushDown.compileFiltersToSqlWhereClause(pushedFilters);
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
    } finally {
      // Always close the lazily opened dataset, including on exception paths, so we don't leak
      // the JNI handle when parsing/detection/pruning helpers throw.
      closeLazyDataset();
    }
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

      // Use the criteria-based overload so that indexes missing index_details
      // (created by older versions) are silently skipped instead of causing errors.
      IndexCriteria criteria = new IndexCriteria.Builder().build();
      for (IndexDescription idx : dataset.describeIndices(criteria)) {
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

  /**
   * Tokenizes {@code lance.partition.columns} on {@code ,}, trims, drops empties, deduplicates,
   * rejects nested paths, and validates each column's Spark type against the whitelist. Returns an
   * empty list if the property is absent, empty, or any column fails validation (reject-all).
   */
  // Package-private so LanceScanBuilderTest can assert dedup / ordering directly.
  List<String> parsePartitionColumns(String raw) {
    // Treat null, empty, whitespace-only, and pure-delimiter values (",", ", ,", ...) all as
    // "property not set" — these are the no-op cases; returning quietly avoids a spurious WARN.
    if (raw == null || raw.replace(",", "").trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> tokens = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (String part : raw.split(",")) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      if (!seen.add(trimmed)) {
        LOG.warn(
            "{} contains duplicate column '{}' (dropped)",
            LanceConstant.TABLE_OPT_PARTITION_COLUMNS,
            trimmed);
        continue;
      }
      if (trimmed.contains(".")) {
        LOG.warn("partition column '{}' has nested path; nested paths not supported", trimmed);
        return Collections.emptyList();
      }
      if (!isSupportedPartitionType(trimmed)) {
        return Collections.emptyList();
      }
      tokens.add(trimmed);
    }
    return tokens;
  }

  /**
   * Looks up the column's type on the full read schema and returns true iff it is in the partition
   * whitelist. Uses {@link #fullSchema} rather than {@link #schema} so column pruning does not
   * remove partition columns from the lookup; returns false with a WARN if the column is missing or
   * has an unsupported type.
   */
  private boolean isSupportedPartitionType(String columnName) {
    int idx;
    try {
      idx = fullSchema.fieldIndex(columnName);
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "partition column '{}' is not in the table schema; partition detection disabled",
          columnName);
      return false;
    }
    org.apache.spark.sql.types.DataType type = fullSchema.fields()[idx].dataType();
    // Whitelist types that PartitionInfo.toSparkValue can encode into Spark's InternalRow.
    // ZoneStats always returns Long for integral Arrow widths (int8/16/32 too) and for
    // Date (epoch-days) / Timestamp (epoch-micros); toSparkValue narrows/wraps appropriately.
    // Use .equals() rather than == so a DataType materialized from a deserialized schema
    // (e.g. JSON/Avro round-trip) still matches the singleton constants.
    if (DataTypes.BooleanType.equals(type)
        || DataTypes.ByteType.equals(type)
        || DataTypes.ShortType.equals(type)
        || DataTypes.IntegerType.equals(type)
        || DataTypes.LongType.equals(type)
        || DataTypes.StringType.equals(type)
        || DataTypes.DateType.equals(type)
        || DataTypes.TimestampType.equals(type)) {
      return true;
    }
    LOG.warn(
        "partition column '{}' has unsupported type {}: whitelist is"
            + " Boolean/Byte/Short/Int/Long/String/Date/Timestamp",
        columnName,
        type.typeName());
    return false;
  }

  /**
   * Runs per-column zone-constancy detection, verifies that every declared column covers the same
   * fragment-id set, and assembles per-fragment partition tuples in declaration order. Returns null
   * when any column fails — reject-all, so SPJ symmetry is preserved on the joined counterpart.
   */
  // Package-private for unit-test access to the multi-column detection logic.
  ZonemapFragmentPruner.PartitionInfo detectPartitioning(
      List<String> partitionColumns, Map<String, List<ZoneStats>> zonemapStats) {
    if (partitionColumns.isEmpty()) {
      return null;
    }
    Map<String, Map<Integer, Comparable<?>>> perColumnMaps = new HashMap<>();
    for (String name : partitionColumns) {
      if (!zonemapStats.containsKey(name)) {
        LOG.warn("partition column '{}' has no zonemap stats; partition detection disabled", name);
        return null;
      }
      Map<Integer, Comparable<?>> values =
          ZonemapFragmentPruner.computeFragmentPartitionValues(zonemapStats.get(name)).orElse(null);
      if (values == null || values.isEmpty()) {
        LOG.warn(
            "partition column '{}' has non-constant or null values; partition detection disabled",
            name);
        return null;
      }
      perColumnMaps.put(name, values);
    }

    // Require every declared partition column to cover the same fragment-id set. A strict-subset
    // intersection would leave splits for uncovered fragments with a phantom null-key tuple —
    // wrong input to Spark's SPJ. Iterate in declaration order so the mismatched-column WARN is
    // deterministic across runs.
    Set<Integer> intersection = null;
    for (String name : partitionColumns) {
      Set<Integer> columnFragments = perColumnMaps.get(name).keySet();
      if (intersection == null) {
        intersection = new HashSet<>(columnFragments);
      } else if (!intersection.equals(columnFragments)) {
        LOG.warn(
            "partition columns {} have mismatched fragment-id coverage (column '{}' differs);"
                + " partition detection disabled",
            partitionColumns,
            name);
        return null;
      }
    }
    if (intersection == null || intersection.isEmpty()) {
      LOG.warn(
          "partition columns {} have no covered fragments; partition detection disabled",
          partitionColumns);
      return null;
    }

    // Assemble tuples in declaration order, and resolve per-column Spark types from the full
    // read schema so PartitionInfo can encode each value into the right InternalRow slot
    // (narrowing Long -> byte/short/int for Byte/Short/Int/Date columns, pass-through otherwise).
    int width = partitionColumns.size();
    Map<Integer, Comparable<?>[]> tuples = new HashMap<>();
    for (Integer fragId : intersection) {
      Comparable<?>[] tuple = new Comparable<?>[width];
      for (int i = 0; i < width; i++) {
        tuple[i] = perColumnMaps.get(partitionColumns.get(i)).get(fragId);
      }
      tuples.put(fragId, tuple);
    }
    List<org.apache.spark.sql.types.DataType> columnTypes = new java.util.ArrayList<>(width);
    for (String name : partitionColumns) {
      columnTypes.add(fullSchema.fields()[fullSchema.fieldIndex(name)].dataType());
    }
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(partitionColumns, columnTypes, tuples);

    // Apply soft cap based on session conf (if available) or the default. When the cap fires,
    // the scan will report UnknownPartitioning — log that branch separately so operators don't
    // see a success-looking "detected N fragments" INFO immediately after the soft-cap WARN.
    int cap = readMaxReportedPartitionsConf();
    if (info.size() > cap) {
      LOG.warn(
          "partition count {} exceeds {}={}; reporting UnknownPartitioning",
          info.size(),
          CONF_REPORTING_MAX_PARTITIONS,
          cap);
      return info.withSoftCapped();
    }
    LOG.info(
        "lance.partition.detect cols={} columnCount={} fragments={}",
        partitionColumns,
        partitionColumns.size(),
        info.size());
    return info;
  }

  private static int readMaxReportedPartitionsConf() {
    String val = null;
    try {
      org.apache.spark.sql.SparkSession session = org.apache.spark.sql.SparkSession.active();
      val = session.conf().get(CONF_REPORTING_MAX_PARTITIONS, null);
    } catch (Exception e) {
      // No active SparkSession (e.g. unit-test / offline builder usage); log at DEBUG so real
      // session-level misconfiguration is diagnosable, and fall through to the default.
      LOG.debug(
          "Could not read {}: {}; using default", CONF_REPORTING_MAX_PARTITIONS, e.toString());
      return DEFAULT_REPORTING_MAX_PARTITIONS;
    }
    if (val == null) {
      return DEFAULT_REPORTING_MAX_PARTITIONS;
    }
    try {
      return Integer.parseInt(val.trim());
    } catch (NumberFormatException e) {
      LOG.warn(
          "Could not parse {}='{}' as an integer; using default {}",
          CONF_REPORTING_MAX_PARTITIONS,
          val,
          DEFAULT_REPORTING_MAX_PARTITIONS);
      return DEFAULT_REPORTING_MAX_PARTITIONS;
    }
  }

  static boolean readReportingEnabledConf() {
    try {
      org.apache.spark.sql.SparkSession session = org.apache.spark.sql.SparkSession.active();
      String val = session.conf().get(CONF_REPORTING_ENABLED, null);
      if (val != null) {
        return !"false".equalsIgnoreCase(val.trim());
      }
    } catch (Exception e) {
      // No active SparkSession; log at DEBUG and default to enabled.
      LOG.debug("Could not read {}: {}; defaulting to true", CONF_REPORTING_ENABLED, e.toString());
    }
    return true;
  }
}
