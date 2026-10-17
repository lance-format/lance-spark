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
import org.lance.FragmentStatistics;
import org.lance.ManifestSummary;
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.index.IndexType;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceSchema;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.search.LanceSearchQuery;
import org.lance.spark.search.LanceSearchScan;
import org.lance.spark.sharding.SparkLanceShardingUtils;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.FieldPathUtils;
import org.lance.spark.utils.FullTextQueryUtils;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LanceScanBuilder
    implements SupportsPushDownRequiredColumns,
        SupportsPushDownV2Filters,
        SupportsPushDownLimit,
        SupportsPushDownOffset,
        SupportsPushDownTopN,
        SupportsPushDownAggregates {
  private static final Logger LOG = LoggerFactory.getLogger(LanceScanBuilder.class);

  private final LanceSparkReadOptions readOptions;

  /** Full table schema before column pruning; used to widen nested structs for vectorized reads. */
  private final StructType fullSchema;

  /** Blob v2 column names in the read schema. Filters on these cannot push to Lance. */
  private final Set<String> blobV2Columns;

  private StructType schema;

  private Predicate[] pushedPredicates = new Predicate[0];

  // Set when pushPredicates leaves filters for Spark. pushLimit and friends read this after
  // pushPredicates because Spark pushes filters before those operators.
  private boolean hasResidualPredicates = false;
  private Optional<Integer> limit = Optional.empty();
  private Optional<Integer> offset = Optional.empty();
  private Optional<List<ColumnOrdering>> topNSortOrders = Optional.empty();
  private Optional<Aggregation> pushedAggregation = Optional.empty();
  private LanceLocalScan localScan = null;
  private LanceIndexedCountScan indexedCountScan = null;

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

  private final ShardingSpec shardingSpec;

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties) {
    this(schema, readOptions, initialStorageOptions, namespaceImpl, namespaceProperties, null);
  }

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      ShardingSpec shardingSpec) {
    this.fullSchema = BlobUtils.applyBlobV2DescriptorSchema(schema);
    this.blobV2Columns = BlobUtils.blobV2ColumnNames(this.fullSchema);
    this.schema = this.fullSchema;
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.shardingSpec = shardingSpec;
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
    // Wrap the entire planning body in try/finally to guarantee that the lazily-opened native
    // dataset handle (lazyDataset) is always released, including when intermediate steps such as
    // zonemap loading or LanceSplit.planScan(dataset) throw.
    try {
      // Return LocalScan if we have a metadata-only aggregation result
      if (localScan != null) {
        return localScan;
      }
      if (indexedCountScan != null) {
        return indexedCountScan;
      }

      // Namespace-configured full-text search executes server-side via queryTable (single
      // partition). A full-text query without a namespace, or against a catalog-only namespace such
      // as Glue that does not implement queryTable, falls through to the local per-fragment scan
      // below. COUNT(*) is excluded because countTableRows has no full-text field.
      if (shouldNamespaceFtsScan()) {
        return buildNamespaceFtsScan();
      }

      // Get statistics from manifest summary before closing dataset
      ManifestSummary summary = getOrOpenDataset().getVersion().getManifestSummary();

      // Collect all columns that need zonemap stats: filter columns + sharding columns.
      Set<String> columnsToLoad = extractReferencedColumns(pushedPredicates);
      Dataset dataset = getOrOpenDataset();
      LanceSchema lanceSchema = dataset.getLanceSchema();
      ShardingSpec activeShardingSpec =
          SparkLanceShardingUtils.isEmpty(shardingSpec)
              ? SparkLanceShardingUtils.firstShardingSpec(dataset)
              : shardingSpec;

      // Pre-compute splits and per-fragment row counts from the same Dataset handle that we
      // already opened above. This provides the live fragment IDs needed for safe sharding
      // detection and pins the resolved version onto the read options shipped to workers, so the
      // zonemap stats and splits come from the same snapshot. The version is kept as a long
      // end-to-end so long-lived high-write-frequency datasets do not silently truncate it.
      LanceSplit.ScanPlanResult scanPlan = LanceSplit.planScan(dataset, readOptions);
      Set<Integer> liveFragmentIds = new HashSet<>(scanPlan.getFragmentRowCounts().keySet());

      for (ShardingField field : SparkLanceShardingUtils.fields(activeShardingSpec)) {
        columnsToLoad.add(SparkLanceShardingUtils.columnName(field, lanceSchema));
      }

      // Load zonemap stats for all requested columns in one pass, along with each
      // column's index coverage.
      ZonemapLoadResult zonemapLoad = loadZonemapStats(getOrOpenDataset(), columnsToLoad);
      Map<String, List<ZoneStats>> zonemapStats = zonemapLoad.stats;

      // Detect sharding-compatible fragments from zonemap stats. Each field checks its column's
      // zones; if every fragment has a single sharding value, we get a fragment-to-key map.
      Map<Integer, Object> fragmentShardingKeys = null;
      Expression activeShardingExpression = null;
      for (ShardingField field : SparkLanceShardingUtils.fields(activeShardingSpec)) {
        String column = SparkLanceShardingUtils.columnName(field, lanceSchema);
        List<ZoneStats> colStats = zonemapStats.get(column);
        if (colStats == null || colStats.isEmpty()) {
          LOG.warn(
              "Sharding column '{}' (transform={}) has no zonemap stats;"
                  + " sharding detection disabled",
              column,
              field.transform().orElse(null));
          continue;
        }
        java.util.Optional<Map<Integer, Object>> keys =
            SparkLanceShardingUtils.detectFragmentKeys(
                field, lanceSchema, colStats, liveFragmentIds);
        if (keys.isPresent()) {
          fragmentShardingKeys = keys.get();
          activeShardingExpression = SparkLanceShardingUtils.toSparkExpression(field, lanceSchema);
          LOG.info(
              "Detected Lance sharding field {}('{}') with {} fragments",
              field.transform().orElse(null),
              column,
              fragmentShardingKeys.size());
          break;
        }
      }

      // Pre-compute fragment pruning so we can (a) estimate post-pruning statistics for
      // JoinSelection (BroadcastHashJoin vs SortMergeJoin) and (b) pass the cached result
      // to LanceScan to avoid re-computing during planInputPartitions().
      Set<Integer> survivingFragmentIds = null;
      if (pushedPredicates.length > 0 && !zonemapStats.isEmpty()) {
        survivingFragmentIds =
            ZonemapFragmentPruner.pruneFragments(
                    pushedPredicates, zonemapStats, zonemapLoad.uncoveredByColumn)
                .orElse(null);
      }

      // Scale rows and full size by the surviving-row ratio first, then let
      // LanceStatistics.estimateProjected apply the column-width ratio on top
      // (when the projected schema is narrower than the full schema).
      long projectedRows = summary.getTotalRows();
      long projectedFullSize = summary.getTotalFilesSize();
      if (survivingFragmentIds != null && !scanPlan.getFragmentRowCounts().isEmpty()) {
        long survivingRows =
            survivingFragmentIds.stream()
                .mapToLong(
                    fragmentId -> scanPlan.getFragmentRowCounts().getOrDefault(fragmentId, 0L))
                .sum();
        LanceStatistics postPruning =
            LanceStatistics.estimatePostPruningByRows(
                summary.getTotalRows(), summary.getTotalFilesSize(), survivingRows);
        projectedRows = postPruning.numRows().getAsLong();
        projectedFullSize = postPruning.sizeInBytes().getAsLong();
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

      LanceSparkReadOptions resolvedReadOptions = readOptions.withRef(scanPlan.getRef());
      Optional<String> whereCondition =
          FilterPushDown.compileFiltersToSqlWhereClause(pushedPredicates);
      return new LanceScan(
          fullSchema,
          schema,
          resolvedReadOptions,
          whereCondition,
          limit,
          offset,
          topNSortOrders,
          pushedAggregation,
          pushedPredicates,
          statistics,
          survivingFragmentIds,
          scanPlan.getSplits(),
          scanPlan.getFragmentRowCounts(),
          activeShardingExpression,
          fragmentShardingKeys,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties);
    } finally {
      closeLazyDataset();
    }
  }

  private static ExactLookupColumn exactLookupColumn(
      LanceSchema lanceSchema, Predicate[] predicates) {
    if (predicates.length == 0) {
      return null;
    }

    Integer fieldId = null;
    String columnPath = null;
    boolean hasLookup = false;
    for (Predicate predicate : predicates) {
      // IS_NOT_NULL may accompany an exact lookup but is not an exact lookup by itself.
      if ("=".equals(predicate.name()) || "IN".equals(predicate.name())) {
        hasLookup = true;
      } else if (!"IS_NOT_NULL".equals(predicate.name())) {
        return null;
      }

      NamedReference[] references = predicate.references();
      if (references.length != 1) {
        return null;
      }
      String path = FieldPathUtils.canonicalPath(Arrays.asList(references[0].fieldNames()));
      int predicateFieldId;
      try {
        predicateFieldId = FieldPathUtils.resolveLeafField(lanceSchema, path).getId();
      } catch (IllegalArgumentException e) {
        return null;
      }
      if (fieldId != null && fieldId != predicateFieldId) {
        return null;
      }
      fieldId = predicateFieldId;
      columnPath = String.join(".", references[0].fieldNames());
    }
    return hasLookup ? new ExactLookupColumn(fieldId, columnPath) : null;
  }

  static Optional<String> compileExactLookupCountFilter(Predicate[] predicates) {
    List<Predicate> lookupPredicates = new ArrayList<>(predicates.length);
    for (Predicate predicate : predicates) {
      if (!"IS_NOT_NULL".equals(predicate.name())) {
        lookupPredicates.add(predicate);
      }
    }
    return FilterPushDown.compileFiltersToSqlWhereClause(
        lookupPredicates.toArray(new Predicate[0]));
  }

  private ExactLookupCount findFullyCoveringExactLookupIndex(Dataset dataset) {
    if (!readOptions.isUseScalarIndex()
        || readOptions.getFullTextQuery() != null
        || pushedPredicates.length == 0) {
      return ExactLookupCount.miss();
    }

    ExactLookupColumn column = exactLookupColumn(dataset.getLanceSchema(), pushedPredicates);
    if (column == null) {
      return ExactLookupCount.miss();
    }

    // Avoid exporting unrelated indexes, including vector indexes, as full segment metadata.
    IndexCriteria criteria =
        new IndexCriteria.Builder()
            .forColumn(column.columnPath)
            .mustSupportExactEquality(true)
            .build();
    Map<String, List<Index>> candidateSegments = new HashMap<>();
    for (IndexDescription description : dataset.describeIndices(criteria)) {
      List<Index> segments = scalarLookupSegments(description, column.fieldId);
      if (segments != null) {
        candidateSegments.put(description.getName(), segments);
      }
    }
    if (candidateSegments.isEmpty()) {
      return ExactLookupCount.miss();
    }

    FragmentStatistics fragmentStatistics = dataset.getFragmentStatistics();
    Set<Integer> nonemptyFragmentIds = nonemptyFragmentIds(fragmentStatistics);
    if (nonemptyFragmentIds == null) {
      return ExactLookupCount.empty();
    }

    String selectedName = null;
    int selectedSegments = Integer.MAX_VALUE;
    for (Map.Entry<String, List<Index>> candidate : candidateSegments.entrySet()) {
      Set<Integer> coveredFragments = new HashSet<>();
      boolean hasCompleteCoverageMetadata = true;
      for (Index segment : candidate.getValue()) {
        if (!segment.fragments().isPresent()) {
          hasCompleteCoverageMetadata = false;
          break;
        }
        coveredFragments.addAll(segment.fragments().get());
      }
      if (!hasCompleteCoverageMetadata || !coveredFragments.containsAll(nonemptyFragmentIds)) {
        continue;
      }
      int segments = candidate.getValue().size();
      String name = candidate.getKey();
      if (selectedName == null
          || segments < selectedSegments
          || (segments == selectedSegments && name.compareTo(selectedName) < 0)) {
        selectedName = name;
        selectedSegments = segments;
      }
    }
    if (selectedName == null) {
      return ExactLookupCount.miss();
    }
    LOG.debug(
        "Using fully covering index '{}' for filtered COUNT(*)"
            + " ({} segments, {} fragments; fewest segments, then name)",
        selectedName,
        selectedSegments,
        nonemptyFragmentIds.size());
    return ExactLookupCount.index(selectedName);
  }

  private static List<Index> scalarLookupSegments(IndexDescription description, int fieldId) {
    List<Index> segments = description.getSegments();
    if (segments == null || segments.isEmpty()) {
      return null;
    }
    for (Index segment : segments) {
      List<Integer> fields = segment.fields();
      if ((segment.indexType() != IndexType.BTREE && segment.indexType() != IndexType.BITMAP)
          || fields == null
          || fields.size() != 1
          || !fields.get(0).equals(fieldId)) {
        return null;
      }
    }
    return segments;
  }

  // Returns null for no fragments or when every aligned row count is zero. Missing or unaligned
  // row counts conservatively retain every fragment id.
  private static Set<Integer> nonemptyFragmentIds(FragmentStatistics fragmentStatistics) {
    int[] fragmentIds = fragmentStatistics.getIds();
    if (fragmentIds.length == 0) {
      return null;
    }
    long[] rowCounts = fragmentStatistics.getRowCounts();
    boolean rowCountsAligned = rowCounts != null && rowCounts.length == fragmentIds.length;
    Set<Integer> nonempty = new HashSet<>();
    for (int i = 0; i < fragmentIds.length; i++) {
      if (!rowCountsAligned || rowCounts[i] > 0) {
        nonempty.add(fragmentIds[i]);
      }
    }
    return nonempty.isEmpty() ? null : nonempty;
  }

  private static final class ExactLookupCount {
    enum Kind {
      MISS,
      EMPTY,
      INDEX
    }

    final Kind kind;
    final String indexName;

    private ExactLookupCount(Kind kind, String indexName) {
      this.kind = kind;
      this.indexName = indexName;
    }

    static ExactLookupCount miss() {
      return new ExactLookupCount(Kind.MISS, null);
    }

    static ExactLookupCount empty() {
      return new ExactLookupCount(Kind.EMPTY, null);
    }

    static ExactLookupCount index(String indexName) {
      return new ExactLookupCount(Kind.INDEX, indexName);
    }
  }

  private static final class ExactLookupColumn {
    final int fieldId;
    final String columnPath;

    private ExactLookupColumn(int fieldId, String columnPath) {
      this.fieldId = fieldId;
      this.columnPath = columnPath;
    }
  }

  boolean shouldNamespaceFtsScan() {
    LanceRef ref = readOptions.getRef();
    if (ref != null && ref.isBranchOrTag()) {
      return false;
    }

    return readOptions.getFullTextQuery() != null
        && LanceRuntime.supportsQueryTable(namespaceImpl)
        && !pushedAggregation.isPresent();
  }

  /**
   * Builds a single-partition scan that runs the full-text query server-side through the namespace
   * {@code queryTable} endpoint. Reuses the search-package namespace scan/reader, driven from the
   * shared scan spec rather than a bespoke table function.
   */
  private Scan buildNamespaceFtsScan() {
    // Request every projected field except the row-id metadata column (fetched via withRowId).
    // _score stays in the request columns because queryTable returns it as a named column.
    List<String> requestColumns = new ArrayList<>();
    boolean withRowId = false;
    for (StructField field : schema.fields()) {
      if (field.name().equals(LanceConstant.ROW_ID)) {
        withRowId = true;
      } else {
        requestColumns.add(field.name());
      }
    }

    // Push the real k/offset only when no residual predicate sits above the scan. Otherwise request
    // all matches and let Spark apply the residual filter and limit globally. queryTable requires a
    // non-null k, so "all" is Integer.MAX_VALUE rather than an omitted k.
    boolean pushLimit = limit.isPresent() && !hasResidualPredicates;
    Integer k = pushLimit ? limit.get() : Integer.MAX_VALUE;
    Integer pushedOffset = (offset.isPresent() && !hasResidualPredicates) ? offset.get() : null;

    Optional<String> whereCondition =
        FilterPushDown.compileFiltersToSqlWhereClause(pushedPredicates);

    LanceSearchQuery query =
        LanceSearchQuery.builder(LanceSearchQuery.SearchType.FULL_TEXT)
            .tableId(readOptions.getTableId())
            .namespaceImpl(namespaceImpl)
            .namespaceProperties(namespaceProperties)
            .outputColumns(requestColumns)
            .fullTextQueryJson(
                FullTextQueryUtils.fullTextQueryToString(readOptions.getFullTextQuery()))
            .topK(k)
            .offset(pushedOffset)
            .filter(whereCondition.isPresent() ? whereCondition.get() : null)
            .version(
                readOptions.getRef() == null || readOptions.getRef().getVersionNumber().isEmpty()
                    ? null
                    : readOptions.getRef().getVersionNumber().get())
            .withRowId(withRowId ? Boolean.TRUE : null)
            .build();
    return new LanceSearchScan(schema, query);
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = ReadSchemaNestedStructWidening.widenRequiredSchema(requiredSchema, fullSchema);
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    Predicate[] pushed;
    Predicate[] residual;
    if (!readOptions.isPushDownFilters()) {
      pushed = new Predicate[0];
      residual = predicates;
    } else {
      List<Predicate> pushedList = new ArrayList<>();
      List<Predicate> residualList = new ArrayList<>();
      // Push supported predicates unless they touch a blob v2 column. Those read back as descriptor
      // structs, so Lance cannot evaluate filters on them. Normal-column filters still prune.
      for (Predicate predicate : predicates) {
        if (FilterPushDown.isPredicateSupported(predicate)
            && !FilterPushDown.referencesAny(predicate, blobV2Columns)) {
          pushedList.add(predicate);
        } else {
          residualList.add(predicate);
        }
      }
      pushed = pushedList.toArray(new Predicate[0]);
      residual = residualList.toArray(new Predicate[0]);
    }
    this.pushedPredicates = pushed;
    this.hasResidualPredicates = residual.length > 0;
    return residual;
  }

  @Override
  public Predicate[] pushedPredicates() {
    return pushedPredicates;
  }

  @Override
  public boolean pushLimit(int limit) {
    if (hasResidualPredicates) {
      return false;
    }
    this.limit = Optional.of(limit);
    return true;
  }

  @Override
  public boolean pushOffset(int offset) {
    if (hasResidualPredicates) {
      return false;
    }
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
    if (!readOptions.isTopNPushDown() || hasResidualPredicates) {
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
    if (hasResidualPredicates) {
      return false;
    }
    AggregateFunc[] funcs = aggregation.aggregateExpressions();
    if (aggregation.groupByExpressions().length > 0) {
      return false;
    }
    if (funcs.length == 1 && funcs[0] instanceof CountStar) {
      // Metadata-based count is only valid when nothing restricts the rows. A full-text query is
      // carried in the read options rather than as a pushed predicate, because the FTS rule moves
      // the predicate out of the Filter and into the relation options, so it must be checked
      // separately or COUNT(*) would answer from the manifest and ignore the FTS query.
      Dataset dataset = getOrOpenDataset();
      if (pushedPredicates.length == 0 && readOptions.getFullTextQuery() == null) {
        Optional<Long> metadataCount = getCountFromMetadata(dataset);
        if (metadataCount.isPresent()) {
          setLocalCount(metadataCount.get());
          return true;
        }
      }

      // total_rows omits fragments with unknown deletion counts, so only total_fragments can prove
      // that the dataset has no fragments.
      if (hasNoFragments(dataset)) {
        setLocalCount(0L);
        return true;
      }

      ExactLookupCount lookup;
      try {
        lookup = findFullyCoveringExactLookupIndex(dataset);
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to inspect scalar indexes for filtered COUNT(*);"
                + " falling back to a distributed scan: {}",
            e.getMessage());
        lookup = ExactLookupCount.miss();
      }
      if (lookup.kind == ExactLookupCount.Kind.EMPTY) {
        setLocalCount(0L);
        return true;
      }
      if (lookup.kind == ExactLookupCount.Kind.INDEX) {
        Optional<String> filter = compileExactLookupCountFilter(pushedPredicates);
        if (filter.isPresent()) {
          try {
            LanceSparkReadOptions pinned =
                readOptions.withRef(Utils.pinOpenedRef(dataset, readOptions.getRef()));
            this.indexedCountScan =
                new LanceIndexedCountScan(
                    pinned,
                    lookup.indexName,
                    filter.get(),
                    initialStorageOptions,
                    namespaceImpl,
                    namespaceProperties);
            return true;
          } catch (RuntimeException e) {
            LOG.warn(
                "Failed to plan an indexed COUNT(*) with scalar index '{}';"
                    + " falling back to a distributed scan: {}",
                lookup.indexName,
                e.getMessage());
          }
        }
      }
      // Fall back to scan-based count (with filters, a full-text query, or metadata unavailable)
      this.pushedAggregation = Optional.of(aggregation);
      return true;
    }

    return false;
  }

  private void setLocalCount(long count) {
    StructType countSchema = new StructType().add("count", DataTypes.LongType);
    InternalRow[] rows = new InternalRow[1];
    rows[0] = new GenericInternalRow(new Object[] {count});
    this.localScan = new LanceLocalScan(countSchema, rows, readOptions.getDatasetUri());
  }

  private static Optional<Long> getCountFromMetadata(Dataset dataset) {
    try {
      ManifestSummary summary = dataset.getVersion().getManifestSummary();
      return Optional.of(summary.getTotalRows());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private static boolean hasNoFragments(Dataset dataset) {
    try {
      return dataset.getVersion().getManifestSummary().getTotalFragments() == 0L;
    } catch (Exception e) {
      return false;
    }
  }

  /** Loads zone stats for every requested column, without consulting index coverage. */
  private Map<String, List<ZoneStats>> loadStatsForColumns(Dataset dataset, Set<String> columns) {
    Map<String, List<ZoneStats>> result = new HashMap<>();
    for (String col : columns) {
      try {
        List<ZoneStats> stats = dataset.getZonemapStats(col);
        if (!stats.isEmpty()) {
          result.put(col, stats);
        }
      } catch (Exception e) {
        LOG.debug("Failed to load zonemap stats for column '{}': {}", col, e.getMessage());
      }
    }
    return result;
  }

  /** Zone stats plus, per column, the dataset fragments those stats do not describe. */
  private static final class ZonemapLoadResult {
    final Map<String, List<ZoneStats>> stats;
    final Map<String, Set<Integer>> uncoveredByColumn;

    ZonemapLoadResult(Map<String, List<ZoneStats>> stats, Map<String, Set<Integer>> uncovered) {
      this.stats = stats;
      this.uncoveredByColumn = uncovered;
    }
  }

  /**
   * Loads zone stats for the requested columns, plus the fragments each column's zones do not
   * describe.
   *
   * <p>Coverage comes from the zones themselves: a column can carry several zonemap indexes and
   * {@code getZonemapStats} returns the zones of only one, so index metadata would claim coverage
   * for fragments those zones never saw.
   */
  private ZonemapLoadResult loadZonemapStats(Dataset dataset, Set<String> columns) {
    if (columns.isEmpty()) {
      return new ZonemapLoadResult(Collections.emptyMap(), Collections.emptyMap());
    }

    Map<String, List<ZoneStats>> stats = loadStatsForColumns(dataset, columns);
    if (stats.isEmpty()) {
      return new ZonemapLoadResult(stats, Collections.emptyMap());
    }

    Set<Integer> allFragments = new HashSet<>();
    for (Fragment fragment : dataset.getFragments()) {
      allFragments.add(fragment.getId());
    }

    Map<String, Set<Integer>> uncoveredByColumn = new HashMap<>();
    for (Map.Entry<String, List<ZoneStats>> entry : stats.entrySet()) {
      Set<Integer> described = new HashSet<>();
      for (ZoneStats zone : entry.getValue()) {
        described.add(zone.getFragmentId());
      }
      Set<Integer> uncovered = new HashSet<>(allFragments);
      uncovered.removeAll(described);
      uncoveredByColumn.put(entry.getKey(), uncovered);
      if (!uncovered.isEmpty()) {
        LOG.info(
            "Zonemap zones for '{}' describe {} of {} fragments;"
                + " retaining {} unindexed fragment(s) in the scan",
            entry.getKey(),
            allFragments.size() - uncovered.size(),
            allFragments.size(),
            uncovered.size());
      }
    }

    return new ZonemapLoadResult(stats, uncoveredByColumn);
  }

  private static Set<String> extractReferencedColumns(Predicate[] predicates) {
    Set<String> columns = new HashSet<>();
    for (Predicate predicate : predicates) {
      for (NamedReference ref : predicate.references()) {
        String[] names = ref.fieldNames();
        columns.add(names.length == 1 ? names[0] : String.join(".", names));
      }
    }
    return columns;
  }
}
