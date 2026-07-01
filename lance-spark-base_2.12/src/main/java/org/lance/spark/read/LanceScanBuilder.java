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
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.sharding.SparkLanceShardingUtils;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.SparkSession;
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
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

      // Get statistics from manifest summary before closing dataset
      org.lance.Version datasetVersion = getOrOpenDataset().getVersion();
      ManifestSummary summary = datasetVersion.getManifestSummary();

      // Collect all columns that need zonemap stats: filter columns + sharding columns.
      Set<String> columnsToLoad = extractReferencedColumns(pushedPredicates);
      Dataset dataset = getOrOpenDataset();
      LanceSchema lanceSchema = dataset.getLanceSchema();
      ShardingSpec activeShardingSpec =
          SparkLanceShardingUtils.isEmpty(shardingSpec)
              ? SparkLanceShardingUtils.firstShardingSpec(dataset)
              : shardingSpec;
      for (ShardingField field : SparkLanceShardingUtils.fields(activeShardingSpec)) {
        columnsToLoad.add(SparkLanceShardingUtils.columnName(field, lanceSchema));
      }

      // Load zonemap stats for all requested columns in one pass.
      Map<String, List<ZoneStats>> zonemapStats =
          loadZonemapStats(getOrOpenDataset(), columnsToLoad);

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
            SparkLanceShardingUtils.detectFragmentKeys(field, lanceSchema, colStats);
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
            ZonemapFragmentPruner.pruneFragments(pushedPredicates, zonemapStats).orElse(null);
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
      // CBO column statistics: when ANALYZE TABLE has persisted lance.stats.* into the manifest
      // config and the manifest version + schema fingerprint still match, surface them to Spark's
      // optimizer via Statistics.columnStats(). There is intentionally NO zonemap-derived fallback
      // here — when persisted stats are absent or stale we report none and let Spark fall back to
      // its own row-count heuristics, exactly as native ANALYZE TABLE behaves.
      //
      // Skip the persisted stats entirely when zonemap fragment pruning has shrunk the reported row
      // count below the live total: the persisted stats describe the FULL table, so pairing their
      // full-table distinctCount / nullCount / min / max with a pruned (smaller) numRows would be
      // internally inconsistent — e.g. distinctCount could exceed numRows — and mislead Spark's
      // join/filter estimation worse than having no column stats. The full-table stats simply do
      // not describe the pruned subset, so we drop them and let the (accurate) pruned row count
      // drive estimation.
      SparkSession session = activeSparkSessionOrNull();
      Map<NamedReference, ColumnStatistics> columnStats = Collections.emptyMap();
      boolean fragmentPruningReducedRows = projectedRows < summary.getTotalRows();
      if (!fragmentPruningReducedRows && resolveCboColumnStatsEnabled(session, readOptions)) {
        Map<String, String> tableProperties = getOrOpenDataset().getConfig();
        Map<NamedReference, ColumnStatistics> persisted =
            loadPersistedColumnStats(
                session, tableProperties, fullSchema, schema, datasetVersion.getId());
        if (persisted != null) {
          columnStats = persisted;
          LOG.info(
              "Using persisted column stats (ANALYZE TABLE fast path) for {} columns",
              persisted.size());
        }
      }
      LanceStatistics statistics =
          LanceStatistics.estimateProjected(
              projectedRows, projectedFullSize, fullSchema, schema, columnStats);
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

      // Pre-compute splits and per-fragment row counts from the same Dataset handle that we
      // already opened above. This consolidates two driver-side opens into one and lets us pin
      // the resolved version onto the read options shipped to workers, providing snapshot
      // isolation across all tasks of this query. The version is kept as a long end-to-end so
      // long-lived high-write-frequency datasets do not silently truncate to a wrong version.
      LanceSplit.ScanPlanResult scanPlan = LanceSplit.planScan(dataset);
      LanceSparkReadOptions resolvedReadOptions =
          readOptions.withVersion(scanPlan.getResolvedVersion());

      Optional<String> whereCondition =
          FilterPushDown.compileFiltersToSqlWhereClause(pushedPredicates);
      return new LanceScan(
          schema,
          resolvedReadOptions,
          whereCondition,
          limit,
          offset,
          topNSortOrders,
          pushedAggregation,
          pushedPredicates,
          statistics,
          zonemapStats,
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
      // Check if we can use metadata-based count (no filters pushed)
      if (pushedPredicates.length == 0) {
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
    LOG.debug("zonemapColumns={}, requested columns={}", zonemapColumns, columns);

    Map<String, List<ZoneStats>> result = new HashMap<>();
    for (String col : columns) {
      if (zonemapColumns.isEmpty() || zonemapColumns.contains(col)) {
        try {
          List<ZoneStats> stats = dataset.getZonemapStats(col);
          LOG.debug("getZonemapStats('{}') returned {} zones", col, stats.size());
          if (!stats.isEmpty()) {
            result.put(col, stats);
            LOG.debug("Loaded {} zonemap zones for column '{}'", stats.size(), col);
          }
        } catch (Exception e) {
          LOG.debug("Failed to load zonemap stats for column" + " '{}': {}", col, e.getMessage());
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

      IndexCriteria criteria = new IndexCriteria.Builder().build();
      for (IndexDescription idx : dataset.describeIndices(criteria)) {
        LOG.debug(
            "Index '{}' type='{}' fields={}", idx.getName(), idx.getIndexType(), idx.getFieldIds());
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

  private static SparkSession activeSparkSessionOrNull() {
    try {
      return SparkSession.active();
    } catch (Exception ignored) {
      return null;
    }
  }

  /**
   * Resolve the column-stats kill-switch using two-level lookup. The SparkConf key {@link
   * LanceStatsKeys#SPARK_CONF_CBO_COLUMN_STATS_ENABLED} acts as a global kill-switch: when set, it
   * overrides the per-scan {@link LanceSparkReadOptions#isCboColumnStatsEnabled()} value. When
   * unset, the per-scan option (default {@code true}) wins. This makes a single session-level
   * config able to disable the feature everywhere for safe rollback.
   */
  private static boolean resolveCboColumnStatsEnabled(
      SparkSession session, LanceSparkReadOptions readOptions) {
    if (session != null) {
      try {
        String key = LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ENABLED;
        if (session.conf().contains(key)) {
          return Boolean.parseBoolean(session.conf().get(key));
        }
      } catch (Exception ignored) {
        // Conf access can throw if the session was closed mid-scan — fall through.
      }
    }
    return readOptions.isCboColumnStatsEnabled();
  }

  /**
   * Read ANALYZE-TABLE-persisted column statistics from the Lance manifest config ({@code
   * lance.stats.*} keys), or {@code null} when none are usable. The returned map feeds Spark's CBO
   * via {@link LanceStatistics#columnStats()}. This path is fully decoupled from zonemap indexes —
   * it reads only persisted properties. When it returns {@code null}, the caller reports no column
   * stats (it does NOT fall back to zonemap aggregation), matching native ANALYZE TABLE semantics.
   */
  static Map<NamedReference, ColumnStatistics> loadPersistedColumnStats(
      SparkSession session,
      java.util.Map<String, String> tableProperties,
      StructType fullSchema,
      StructType projectedSchema,
      long currentManifestVersion) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return null;
    }
    // No completeness sentinel is consulted: ANALYZE commits the entire stats payload in one atomic
    // alterTable (BaseLanceNamespaceSparkCatalog issues a single Dataset.updateConfig), so a reader
    // never observes a half-written set. Validity is gated below by the format version,
    // computedAtVersion, and schemaHash tags instead.
    // Format-version gate: only consume payloads we know how to decode. Missing version is
    // fail-safe (skip); unrecognized version warns and skips so a future v2 writer can ship
    // before the readers catch up.
    String formatVersion = tableProperties.get(LanceStatsKeys.VERSION);
    if (formatVersion == null) {
      LOG.debug(
          "lance.stats.version key absent — reporting no column stats (no ANALYZE-written "
              + "stats payload present, or a hand-edited / partially-written payload)");
      return null;
    }
    if (!LanceStatsKeys.SUPPORTED_VERSION.equals(formatVersion.trim())) {
      // Sanitize: TBLPROPERTIES values are user-controllable; SLF4J's {} passes CR/LF verbatim.
      LOG.warn(
          "Unrecognized lance.stats.version='{}' — reporting no column stats. "
              + "This usually means the table was analyzed with a newer connector; upgrade the "
              + "connector or re-run ANALYZE TABLE with the current connector to restore the fast "
              + "path.",
          sanitizeForLog(formatVersion));
      return null;
    }
    String versionStr = tableProperties.get(LanceStatsKeys.COMPUTED_AT_VERSION);
    if (versionStr == null) {
      return null;
    }
    long computedAtVersion;
    try {
      computedAtVersion = Long.parseLong(versionStr.trim());
    } catch (NumberFormatException e) {
      LOG.warn(
          "Malformed lance.stats.computedAtVersion='{}' — ignoring persisted stats",
          sanitizeForLog(versionStr));
      return null;
    }
    if (computedAtVersion < 0) {
      LOG.warn(
          "Negative lance.stats.computedAtVersion={} — ignoring persisted stats",
          computedAtVersion);
      return null;
    }
    boolean allowStale = false;
    if (session != null) {
      try {
        String key = LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE;
        if (session.conf().contains(key)) {
          allowStale = Boolean.parseBoolean(session.conf().get(key));
        }
      } catch (Exception ignored) {
        // Conf access can throw if the session was closed mid-scan — keep allowStale=false.
      }
    }
    // Schema fingerprint guard: a hash mismatch means columns were renamed/retyped/dropped/
    // reordered, and persisted stats may attribute values to the wrong column. Missing hash is
    // also fail-safe — a hand-edited or pre-hash payload can't be verified.
    String recordedSchemaHash = tableProperties.get(LanceStatsKeys.SCHEMA_HASH);
    if (recordedSchemaHash == null) {
      LOG.debug(
          "lance.stats.schemaHash key absent — reporting no column stats. Persisted stats "
              + "predate the schema-drift guard; re-run ANALYZE TABLE to populate the hash.");
      return null;
    }
    String currentSchemaHash = LanceStatsKeys.computeSchemaHash(fullSchema);
    if (!recordedSchemaHash.equals(currentSchemaHash)) {
      // Sanitize the recorded hash — a poisoned property could violate the SHA-256-hex shape.
      LOG.info(
          "lance.stats.schemaHash mismatch (recorded={}, current={}); schema has changed since "
              + "ANALYZE — reporting no column stats. Re-run ANALYZE TABLE to refresh.",
          sanitizeForLog(recordedSchemaHash.substring(0, Math.min(8, recordedSchemaHash.length()))),
          currentSchemaHash.substring(0, Math.min(8, currentSchemaHash.length())));
      return null;
    }
    if (computedAtVersion != currentManifestVersion && !allowStale) {
      LOG.debug(
          "Persisted column stats are stale (computedAtVersion={}, currentVersion={}); "
              + "reporting no column stats. Re-run ANALYZE TABLE to refresh, or set {}=true.",
          computedAtVersion,
          currentManifestVersion,
          LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE);
      return null;
    }

    Map<NamedReference, ColumnStatistics> result = new LinkedHashMap<>();
    for (org.apache.spark.sql.types.StructField f : projectedSchema.fields()) {
      // Spark matches V2 column stats via AttributeReference.name().equals(ref.describe()).
      // FieldReference.describe() backtick-quotes any name needing SQL quoting, so a stat for such
      // a column would be reported but never matched/used by the optimizer. Skip it so
      // columnStats() advertises only stats Spark can actually consume.
      if (!FieldReference.column(f.name()).describe().equals(f.name())) {
        LOG.debug(
            "Skipping persisted stats for column {} — its name requires SQL quoting, so Spark's "
                + "optimizer cannot match it to a plan attribute.",
            sanitizeForLog(f.name()));
        continue;
      }
      // TimestampNTZ min/max crash Spark's CBO FilterEstimation (no toDouble case → MatchError).
      // ANALYZE never writes them, but defend against a hand-edited/poisoned payload too.
      if (org.apache.spark.sql.types.DataTypes.TimestampNTZType.equals(f.dataType())) {
        continue;
      }
      // Decode this column's persisted keys via Spark's own CatalogColumnStat codec (see
      // LanceColumnStatCodec): fromMap + toPlanStat yields min/max in the internal catalyst
      // representation that DataSourceV2Relation.transformV2Stats copies verbatim into a catalyst
      // ColumnStat (no CatalystTypeConverters round-trip on the V2 min/max path). Every decode
      // failure — absent keys, Spark codec rejection, or a poisoned min/max that fromExternalString
      // can't parse — is fail-safe (empty), so the column is simply omitted from the stats map.
      java.util.Optional<ColumnStatistics> decoded =
          org.apache.spark.sql.execution.datasources.v2.LanceColumnStatCodec.decode(
              STATS_DECODE_TABLE_LABEL, f.name(), f.dataType(), tableProperties);
      if (decoded.isPresent()) {
        result.put(FieldReference.column(f.name()), decoded.get());
      }
    }
    return result.isEmpty() ? null : result;
  }

  /**
   * Placeholder table label passed to {@link
   * org.apache.spark.sql.execution.datasources.v2.LanceColumnStatCodec#decode}. Spark's {@code
   * CatalogColumnStat.fromMap} uses it only in an internal warn message on malformed input; no real
   * table identifier is in scope here, so a constant suffices.
   */
  private static final String STATS_DECODE_TABLE_LABEL = "lance";

  private static String sanitizeForLog(String value) {
    return LanceStatsKeys.sanitizeForLog(value);
  }
}
