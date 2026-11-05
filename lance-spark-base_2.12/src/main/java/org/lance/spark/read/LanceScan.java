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

import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.ArrowBatchAdapter;
import org.lance.spark.read.metric.LanceCustomMetrics;
import org.lance.spark.sharding.SparkLanceShardingUtils;
import org.lance.spark.utils.BucketHashUtil;
import org.lance.spark.utils.FullTextQueryUtils;
import org.lance.spark.utils.Optional;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.internal.connector.SupportsMetadata;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LanceScan
    implements Batch,
        Scan,
        SupportsMetadata,
        SupportsReportStatistics,
        SupportsReportPartitioning,
        SupportsRuntimeV2Filtering,
        Serializable {
  private static final long serialVersionUID = 947284762748623947L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceScan.class);

  private final StructType schema;
  private final LanceSparkReadOptions readOptions;
  private final Optional<String> whereConditions;
  private final Optional<Integer> limit;
  private final Optional<Integer> offset;
  private final Optional<List<ColumnOrdering>> topNSortOrders;
  private final Optional<Aggregation> pushedAggregation;
  private final Predicate[] pushedPredicates;
  private final LanceStatistics statistics;
  private final String scanId = UUID.randomUUID().toString();
  private final java.util.Map<String, List<ZoneStats>> zonemapStats;
  private final java.util.Map<String, Set<Integer>> uncoveredFragmentsByColumn;
  private final NamedReference[] runtimeFilterAttributes;
  private final List<Predicate> runtimePredicates = new ArrayList<>();
  private final Set<String> runtimePredicateFingerprints = new LinkedHashSet<>();
  private int runtimeFilterKeyCount;
  private int runtimeFilterSerializedBytes;
  private int rejectedRuntimeFilterCount;
  private transient Object runtimeFilterLock = new Object();

  /**
   * Pre-computed surviving fragment IDs from zonemap pruning in LanceScanBuilder. When non-null,
   * {@link #pruneByZonemapStats} skips re-computing and uses these directly.
   */
  private final Set<Integer> cachedSurvivingFragmentIds;

  /**
   * Splits pre-computed on the driver during {@link LanceScanBuilder#build()}. Each entry is one
   * fragment. Built from a single {@code Dataset} handle that was already opened for manifest /
   * schema / zonemap loading, so no second {@code Dataset.open()} is needed at {@link
   * #planInputPartitions()} time.
   */
  private final List<LanceSplit> precomputedSplits;

  /**
   * Per-fragment logical row counts (after deletions), captured together with {@link
   * #precomputedSplits} on the driver. Consumed by {@link #pruneByLimit}. Not declared {@code
   * transient} because Java deserialization would skip the constructor and leave the field {@code
   * null}, which would NPE inside {@link #pruneByLimit}.
   */
  private final java.util.Map<Integer, Long> precomputedFragmentRowCounts;

  /** Number of partitions after pruning, set during {@link #planInputPartitions()}. */
  private transient int numPartitions = -1;

  private transient int survivingFragmentCount = -1;

  /** Active Spark partition expression detected from sharding zonemap stats. */
  private final Expression activeShardingExpression;

  private final String activeShardingColumn;

  /** Map from fragment ID to sharding key value. Null when no sharding is detected. */
  private final java.util.Map<Integer, Object> fragmentShardingKeys;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  /** The Arrow batches the scan can emit; see {@link #arrowBatchSchema()}. */
  private final ArrowBatchAdapter.Plan arrowBatchPlan;

  /** True when the dataset has no stable row IDs, so {@code _rowid} equals {@code _rowaddr}. */
  private final boolean rowIdIsRowAddress;

  /**
   * Largest stored value of each runtime filter column that Lance stores unsigned. Spark reads
   * these columns as a wider signed type, so a join key can be negative or above this value. Such a
   * key matches no row, and Lance rejects it as a literal for the column.
   */
  private final java.util.Map<String, Long> unsignedRuntimeKeyMax;

  /**
   * The constructor from before runtime filtering, kept so existing callers still compile. The scan
   * advertises no runtime filter columns and treats {@code _rowid} as distinct from {@code
   * _rowaddr}.
   */
  public LanceScan(
      StructType schema,
      LanceSparkReadOptions readOptions,
      Optional<String> whereConditions,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders,
      Optional<Aggregation> pushedAggregation,
      Predicate[] pushedPredicates,
      LanceStatistics statistics,
      Set<Integer> survivingFragmentIds,
      List<LanceSplit> precomputedSplits,
      java.util.Map<Integer, Long> precomputedFragmentRowCounts,
      Expression activeShardingExpression,
      java.util.Map<Integer, Object> fragmentShardingKeys,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties) {
    this(
        schema,
        readOptions,
        whereConditions,
        limit,
        offset,
        topNSortOrders,
        pushedAggregation,
        pushedPredicates,
        statistics,
        survivingFragmentIds,
        null,
        null,
        null,
        precomputedSplits,
        precomputedFragmentRowCounts,
        activeShardingExpression,
        activeShardingExpression instanceof NamedReference
            ? String.join(".", ((NamedReference) activeShardingExpression).fieldNames())
            : null,
        fragmentShardingKeys,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        false,
        null);
  }

  public LanceScan(
      StructType schema,
      LanceSparkReadOptions readOptions,
      Optional<String> whereConditions,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders,
      Optional<Aggregation> pushedAggregation,
      Predicate[] pushedPredicates,
      LanceStatistics statistics,
      Set<Integer> survivingFragmentIds,
      java.util.Map<String, List<ZoneStats>> zonemapStats,
      java.util.Map<String, Set<Integer>> uncoveredFragmentsByColumn,
      NamedReference[] runtimeFilterAttributes,
      List<LanceSplit> precomputedSplits,
      java.util.Map<Integer, Long> precomputedFragmentRowCounts,
      Expression activeShardingExpression,
      String activeShardingColumn,
      java.util.Map<Integer, Object> fragmentShardingKeys,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      boolean rowIdIsRowAddress,
      java.util.Map<String, Long> unsignedRuntimeKeyMax,
      ArrowBatchAdapter.Plan arrowBatchPlan) {
    this.schema = schema;
    this.arrowBatchPlan =
        arrowBatchPlan != null
            ? arrowBatchPlan
            : ArrowBatchAdapter.Plan.unsupported("the scan was built without an Arrow batch plan");
    // The reader emits Arrow batches whenever the read option asks for them, so a scan that
    // cannot emit them reads as usual whatever its read options say.
    if (readOptions != null
        && readOptions.isArrowBatchReadsEnabled()
        && !this.arrowBatchPlan.isSupported()) {
      LOG.info(
          "Reading {} without Arrow batches: {}",
          readOptions.getDatasetUri(),
          this.arrowBatchPlan.unsupportedReason());
      this.readOptions = readOptions.withArrowBatchReads(false);
    } else {
      this.readOptions = readOptions;
    }
    this.whereConditions = whereConditions;
    this.limit = limit;
    this.offset = offset;
    this.topNSortOrders = topNSortOrders;
    this.pushedAggregation = pushedAggregation;
    this.pushedPredicates =
        pushedPredicates != null
            ? Arrays.copyOf(pushedPredicates, pushedPredicates.length)
            : new Predicate[0];
    this.statistics = statistics;
    this.cachedSurvivingFragmentIds = survivingFragmentIds;
    this.zonemapStats = zonemapStats != null ? zonemapStats : Collections.emptyMap();
    this.uncoveredFragmentsByColumn =
        uncoveredFragmentsByColumn != null ? uncoveredFragmentsByColumn : Collections.emptyMap();
    this.runtimeFilterAttributes =
        runtimeFilterAttributes != null
            ? Arrays.copyOf(runtimeFilterAttributes, runtimeFilterAttributes.length)
            : new NamedReference[0];
    this.precomputedSplits = precomputedSplits;
    this.precomputedFragmentRowCounts =
        precomputedFragmentRowCounts != null
            ? precomputedFragmentRowCounts
            : Collections.emptyMap();
    this.activeShardingExpression = activeShardingExpression;
    this.activeShardingColumn = activeShardingColumn;
    this.fragmentShardingKeys = fragmentShardingKeys;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.rowIdIsRowAddress = rowIdIsRowAddress;
    this.unsignedRuntimeKeyMax =
        unsignedRuntimeKeyMax != null ? unsignedRuntimeKeyMax : Collections.emptyMap();
  }

  /**
   * The read options this scan executes with, already pinned to the resolved dataset version by
   * {@link LanceScanBuilder} for snapshot isolation.
   */
  public LanceSparkReadOptions readOptions() {
    return readOptions;
  }

  /**
   * The Arrow schema of the batches the columnar reader emits with Arrow batch reads ({@link
   * LanceSparkReadOptions#CONFIG_ARROW_BATCH_READS}, {@link #withArrowBatches()}), or empty if this
   * scan cannot emit Arrow batches; {@link #arrowBatchUnsupportedReason()} then says why. A
   * consumer that takes the batches as Arrow can check here that it reads every type.
   *
   * <p>Every column of an Arrow batch is a Spark {@link
   * org.apache.spark.sql.vectorized.ArrowColumnVector}, in {@link #readSchema()} order. A batch is
   * valid until the reader's next {@code next()} or {@code close()} call, which reuses or releases
   * its vectors, so a consumer that keeps a batch longer transfers its vectors first.
   */
  public Optional<Schema> arrowBatchSchema() {
    return arrowBatchPlan.isSupported() ? Optional.of(arrowBatchPlan.schema()) : Optional.empty();
  }

  /** Why this scan cannot emit Arrow batches, if it cannot. See {@link #arrowBatchSchema()}. */
  public Optional<String> arrowBatchUnsupportedReason() {
    return arrowBatchPlan.isSupported()
        ? Optional.empty()
        : Optional.of(arrowBatchPlan.unsupportedReason());
  }

  /**
   * A copy of this scan whose columnar reader emits Arrow batches, as with the {@link
   * LanceSparkReadOptions#CONFIG_ARROW_BATCH_READS} read option.
   *
   * @throws IllegalStateException if this scan cannot emit Arrow batches (see {@link
   *     #arrowBatchUnsupportedReason()})
   */
  public LanceScan withArrowBatches() {
    if (!arrowBatchPlan.isSupported()) {
      throw new IllegalStateException(
          "Lance scan cannot emit Arrow batches: " + arrowBatchPlan.unsupportedReason());
    }
    if (readOptions.isArrowBatchReadsEnabled()) {
      return this;
    }
    LanceScan copy =
        new LanceScan(
            schema,
            readOptions.withArrowBatchReads(true),
            whereConditions,
            limit,
            offset,
            topNSortOrders,
            pushedAggregation,
            pushedPredicates,
            statistics,
            cachedSurvivingFragmentIds,
            precomputedSplits,
            precomputedFragmentRowCounts,
            activeShardingExpression,
            fragmentShardingKeys,
            initialStorageOptions,
            namespaceImpl,
            namespaceProperties,
            arrowBatchPlan);
    // Planned partitions are the same, so the copy reports the same partitioning.
    copy.numPartitions = numPartitions;
    return copy;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    Predicate[] runtimePredicatesSnapshot = runtimePredicatesSnapshot();
    if (containsEmptyRuntimeIn(runtimePredicatesSnapshot)) {
      this.numPartitions = 0;
      this.survivingFragmentCount = 0;
      return new InputPartition[0];
    }
    Predicate[] effectivePredicates = concatPredicates(pushedPredicates, runtimePredicatesSnapshot);

    // Splits and per-fragment row counts are pre-computed on the driver during
    // LanceScanBuilder.build() from the same Dataset handle that loaded manifest /
    // schema / zonemap stats. This avoids a second Dataset.open() at plan time.
    List<LanceSplit> prunedSplits =
        pruneForPlanning(
            precomputedSplits, effectivePredicates, cachedSurvivingFragmentIds, rowIdIsRowAddress);

    // Sharding keys are exact per-fragment values. Equality and IN predicates on the sharding
    // source column can therefore remove fragments without consulting native index state.
    prunedSplits = pruneByShardingFilters(prunedSplits, effectivePredicates);
    prunedSplits = pruneByRuntimeZonemapStats(prunedSplits, runtimePredicatesSnapshot);

    // Limit-based split pruning: when a LIMIT is pushed down without filters or TopN sort,
    // use per-fragment row counts to plan only enough splits to satisfy the limit.
    // This avoids scheduling hundreds of unnecessary tasks. Correctness is guaranteed
    // because Spark still keeps a global CollectLimit on top (isPartiallyPushed = true).
    if (runtimePredicatesSnapshot.length == 0) {
      prunedSplits = pruneByLimit(prunedSplits, precomputedFragmentRowCounts);
    }

    // Capture as effectively final for use in lambda
    final List<LanceSplit> finalSplits = prunedSplits;
    final DataType partitionKeyType = partitionKeyDataType();
    final Optional<String> effectiveWhereCondition =
        runtimePredicatesSnapshot.length == 0
            ? whereConditions
            : FilterPushDown.compileFiltersToSqlWhereClause(effectivePredicates);

    // readOptions is already pinned to the resolved version by LanceScanBuilder for
    // snapshot isolation across all workers.
    InputPartition[] result =
        IntStream.range(0, finalSplits.size())
            .mapToObj(
                i -> {
                  LanceSplit split = finalSplits.get(i);
                  InternalRow partKeyRow = null;
                  if (activeShardingExpression != null && fragmentShardingKeys != null) {
                    int fragId = split.getFragments().get(0);
                    Object key = fragmentShardingKeys.get(fragId);
                    if (key != null) {
                      partKeyRow = SparkLanceShardingUtils.partitionKeyRow(key, partitionKeyType);
                    }
                  }
                  return new LanceInputPartition(
                      schema,
                      i,
                      split,
                      readOptions,
                      effectiveWhereCondition,
                      limit,
                      offset,
                      topNSortOrders,
                      pushedAggregation,
                      scanId,
                      initialStorageOptions,
                      namespaceImpl,
                      namespaceProperties,
                      partKeyRow);
                })
            .toArray(InputPartition[]::new);

    this.numPartitions = result.length;
    this.survivingFragmentCount = result.length;
    return result;
  }

  private DataType partitionKeyDataType() {
    if (activeShardingExpression instanceof BucketTransform) {
      return DataTypes.IntegerType;
    }
    StructField field = findField(activeShardingColumn);
    return field != null ? field.dataType() : DataTypes.StringType;
  }

  /**
   * Shared with {@link LanceScanBuilder#build()} so planning can tell, before the scan exists,
   * whether pruning leaves anything to read. An empty result means no fragment can match.
   *
   * @param survivingFragmentIds fragment IDs kept by zonemap pruning, or null when the builder did
   *     not prune
   */
  static List<LanceSplit> pruneForPlanning(
      List<LanceSplit> allSplits,
      Predicate[] pushedPredicates,
      Set<Integer> survivingFragmentIds,
      boolean rowIdIsRowAddress) {
    return pruneByZonemapStats(
        pruneByRowAddrFilters(allSplits, pushedPredicates, rowIdIsRowAddress),
        survivingFragmentIds);
  }

  /**
   * Prunes splits based on {@code _rowaddr} filters — skipping fragment opens, scan setup, and task
   * scheduling for fragments that provably cannot match the query predicate.
   *
   * <p>CONTRACT: {@link LanceSplit#getFragments()} returns Lance fragment IDs as Integer values
   * that match {@code (int)(rowAddr >>> 32)} — the same encoding used by {@link
   * org.lance.spark.join.FragmentAwareJoinUtils}. This is verified by {@link
   * LanceSplit#planScan(LanceSparkReadOptions)} which maps {@code Fragment.getId()} directly.
   *
   * <p>Note: an empty allowedIds set is valid — it means the filter is unsatisfiable (e.g. {@code
   * _rowaddr = 0 AND _rowaddr = 4294967296L}) and no fragments can match, resulting in zero rows
   * returned.
   */
  private static List<LanceSplit> pruneByRowAddrFilters(
      List<LanceSplit> allSplits, Predicate[] effectivePredicates, boolean rowIdIsRowAddress) {
    java.util.Optional<Set<Integer>> targetFragmentIds =
        RowAddressFilterAnalyzer.extractTargetFragmentIds(effectivePredicates, rowIdIsRowAddress);
    if (!targetFragmentIds.isPresent()) {
      return allSplits;
    }
    Set<Integer> allowedIds = targetFragmentIds.get();
    // Assumes each LanceSplit maps to a single fragment. If splits ever
    // bundle multiple fragments, consider sub-split level pruning.
    List<LanceSplit> pruned =
        allSplits.stream()
            .filter(
                split -> {
                  if (split.getFragments().size() > 1) {
                    LOG.warn(
                        "Split contains {} fragments;" + " sub-split pruning not implemented",
                        split.getFragments().size());
                  }
                  return split.getFragments().stream().anyMatch(allowedIds::contains);
                })
            .collect(Collectors.toList());
    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Pruned fragments by _rowaddr filters: {} of {} splits retained,"
              + " allowed fragment IDs: {}",
          pruned.size(),
          allSplits.size(),
          allowedIds);
    } else {
      LOG.debug(
          "No fragments pruned by _rowaddr filters: all {} splits retained,"
              + " allowed fragment IDs: {}",
          allSplits.size(),
          allowedIds);
    }
    return pruned;
  }

  /**
   * Prunes splits based on pushed LIMIT using per-fragment row counts from the manifest.
   *
   * <p>When a LIMIT is pushed down without filters or TopN sort orders, we can use the per-fragment
   * logical row counts (which account for deletions) to determine how many fragments are needed to
   * satisfy the limit. This avoids scheduling hundreds of unnecessary tasks for large tables.
   *
   * <p>This optimization is skipped when:
   *
   * <ul>
   *   <li>No limit is pushed
   *   <li>Filters are present (unknown selectivity makes row count estimation unreliable)
   *   <li>TopN sort orders are present (all fragments needed for global sort)
   *   <li>Aggregation is pushed (e.g., COUNT(*) LIMIT — row counts don't apply)
   *   <li>FTS query is active (needs all fragments to return complete matching results)
   *   <li>Fragment row counts are unavailable
   * </ul>
   *
   * <p>Correctness is guaranteed because Spark keeps a global {@code CollectLimit} on top (since
   * {@code isPartiallyPushed()} returns {@code true}). If we under-estimate due to concurrent
   * deletions, the query simply returns fewer rows than the limit — which is valid LIMIT semantics.
   */
  private List<LanceSplit> pruneByLimit(
      List<LanceSplit> allSplits, java.util.Map<Integer, Long> fragmentRowCounts) {
    if (!limit.isPresent()
        || whereConditions.isPresent()
        || topNSortOrders.isPresent()
        || pushedAggregation.isPresent()
        || readOptions.getFullTextQuery() != null
        || fragmentRowCounts.isEmpty()) {
      return allSplits;
    }

    int requestedLimit = limit.get();
    long rowsAccumulated = 0;
    List<LanceSplit> pruned = new java.util.ArrayList<>();

    for (LanceSplit split : allSplits) {
      pruned.add(split);
      for (int fragmentId : split.getFragments()) {
        Long rowCount = fragmentRowCounts.get(fragmentId);
        if (rowCount != null) {
          rowsAccumulated += rowCount;
        }
      }
      if (rowsAccumulated >= requestedLimit) {
        break;
      }
    }

    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Limit-based pruning: {} of {} splits retained for LIMIT {} "
              + "(accumulated {} rows from selected fragments)",
          pruned.size(),
          allSplits.size(),
          requestedLimit,
          rowsAccumulated);
    }

    return pruned;
  }

  /**
   * Prunes splits based on zonemap index statistics — using per-column min/max/null_count to
   * eliminate fragments that provably cannot match the pushed filters.
   *
   * <p>This is analogous to partition pruning in Hive/Iceberg: fragments whose zones all fail the
   * predicate are skipped entirely, avoiding fragment opens, scan setup, and task scheduling.
   */
  private static List<LanceSplit> pruneByZonemapStats(
      List<LanceSplit> allSplits, Set<Integer> survivingFragmentIds) {
    // Null means the builder did not prune. Do not recompute here.
    if (survivingFragmentIds == null) {
      return allSplits;
    }
    Set<Integer> allowedIds = survivingFragmentIds;

    List<LanceSplit> pruned =
        allSplits.stream()
            .filter(split -> split.getFragments().stream().anyMatch(allowedIds::contains))
            .collect(Collectors.toList());

    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Zonemap pruning: {} of {} splits retained," + " allowed fragment IDs: {}",
          pruned.size(),
          allSplits.size(),
          allowedIds);
    }

    return pruned;
  }

  private List<LanceSplit> pruneByRuntimeZonemapStats(
      List<LanceSplit> allSplits, Predicate[] runtimePredicatesSnapshot) {
    if (runtimePredicatesSnapshot.length == 0 || zonemapStats.isEmpty()) {
      return allSplits;
    }
    java.util.Optional<Set<Integer>> surviving =
        ZonemapFragmentPruner.pruneFragments(
            runtimePredicatesSnapshot, zonemapStats, uncoveredFragmentsByColumn);
    if (!surviving.isPresent()) {
      return allSplits;
    }
    Set<Integer> allowedIds = surviving.get();
    return allSplits.stream()
        .filter(split -> split.getFragments().stream().anyMatch(allowedIds::contains))
        .collect(Collectors.toList());
  }

  @Override
  public NamedReference[] filterAttributes() {
    if (!isSpark41OrLater() || !readOptions.isPushDownFilters()) {
      return new NamedReference[0];
    }
    return Arrays.copyOf(runtimeFilterAttributes, runtimeFilterAttributes.length);
  }

  @Override
  public void filter(Predicate[] predicates) {
    if (!isSpark41OrLater()
        || !readOptions.isPushDownFilters()
        || predicates == null
        || predicates.length == 0) {
      return;
    }

    Set<String> advertisedColumns =
        Arrays.stream(runtimeFilterAttributes)
            .map(LanceScan::columnName)
            .collect(Collectors.toSet());
    synchronized (runtimeFilterLock) {
      for (Predicate predicate : predicates) {
        CanonicalRuntimePredicate canonical =
            canonicalizeRuntimePredicate(predicate, advertisedColumns);
        if (canonical == null) {
          rejectedRuntimeFilterCount++;
          LOG.warn("Ignoring unsupported Lance runtime predicate: {}", predicate);
          continue;
        }
        if (runtimePredicateFingerprints.contains(canonical.fingerprint)) {
          continue;
        }
        if (runtimeFilterKeyCount + canonical.keyCount > readOptions.getRuntimeFilterMaxKeys()
            || runtimeFilterSerializedBytes + canonical.serializedBytes
                > readOptions.getRuntimeFilterMaxBytes()) {
          rejectedRuntimeFilterCount++;
          LOG.warn(
              "Ignoring Lance runtime predicate on '{}' because accumulated runtime filter size "
                  + "would exceed maxKeys={} or maxBytes={}",
              canonical.column,
              readOptions.getRuntimeFilterMaxKeys(),
              readOptions.getRuntimeFilterMaxBytes());
          continue;
        }
        runtimePredicates.add(canonical.predicate);
        runtimePredicateFingerprints.add(canonical.fingerprint);
        runtimeFilterKeyCount += canonical.keyCount;
        runtimeFilterSerializedBytes += canonical.serializedBytes;
      }
    }
  }

  /**
   * Spark 4.2 adds this method to {@code SupportsRuntimeV2Filtering}. It intentionally has no
   * {@code @Override} annotation so the common source also compiles against Spark 4.1 and older
   * build dependencies.
   */
  public Predicate[] pushedPredicates() {
    return runtimePredicatesSnapshot();
  }

  /**
   * Lance accepts Spark's exact translated IN predicates directly, so a second 4.2
   * PartitionPredicate pass is unnecessary.
   */
  public boolean supportsIterativePushdown() {
    return false;
  }

  private Predicate[] runtimePredicatesSnapshot() {
    synchronized (runtimeFilterLock) {
      return runtimePredicates.toArray(new Predicate[0]);
    }
  }

  private CanonicalRuntimePredicate canonicalizeRuntimePredicate(
      Predicate predicate, Set<String> advertisedColumns) {
    if (predicate == null || !"IN".equals(predicate.name())) {
      return null;
    }
    Expression[] children = predicate.children();
    if (children.length == 0 || !(children[0] instanceof NamedReference)) {
      return null;
    }
    NamedReference reference = (NamedReference) children[0];
    if (reference.fieldNames().length != 1) {
      return null;
    }
    String column = columnName(reference);
    if (!advertisedColumns.contains(column)) {
      return null;
    }

    StructField field = findField(column);
    if (field == null) {
      return null;
    }

    Long unsignedMax = unsignedRuntimeKeyMax.get(column);
    List<Literal<?>> literals = new ArrayList<>();
    Set<String> literalFingerprints = new LinkedHashSet<>();
    int serializedBytes = column.getBytes(StandardCharsets.UTF_8).length;
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        return null;
      }
      Literal<?> literal = (Literal<?>) children[i];
      if (literal.value() == null) {
        continue;
      }
      if (!runtimeLiteralMatchesField(field.dataType(), literal.dataType())) {
        return null;
      }
      if (unsignedMax != null && !withinUnsignedRange(literal.value(), unsignedMax)) {
        continue;
      }
      String literalFingerprint = runtimeLiteralFingerprint(literal);
      if (literalFingerprints.add(literalFingerprint)) {
        literals.add(literal);
        serializedBytes += literalFingerprint.getBytes(StandardCharsets.UTF_8).length;
      }
    }

    Expression[] canonicalChildren = new Expression[literals.size() + 1];
    canonicalChildren[0] = FieldReference.column(column);
    for (int i = 0; i < literals.size(); i++) {
      canonicalChildren[i + 1] = literals.get(i);
    }
    Predicate canonical = new Predicate("IN", canonicalChildren);
    String fingerprint = column + "|" + String.join("|", literalFingerprints);
    return new CanonicalRuntimePredicate(
        canonical, column, literals.size(), serializedBytes, fingerprint);
  }

  private StructField findField(String column) {
    for (StructField field : schema.fields()) {
      if (field.name().equals(column)) {
        return field;
      }
    }
    return null;
  }

  private static boolean runtimeLiteralMatchesField(DataType fieldType, DataType literalType) {
    return literalType == DataTypes.NullType || fieldType.sameType(literalType);
  }

  private static boolean withinUnsignedRange(Object value, long max) {
    if (!(value instanceof Number)) {
      return true;
    }
    long key = ((Number) value).longValue();
    return key >= 0 && key <= max;
  }

  private static String runtimeLiteralFingerprint(Literal<?> literal) {
    Object value = normalizeLiteral(literal.value());
    return literal.dataType().sql() + ":" + value.getClass().getName() + ":" + value.toString();
  }

  private static boolean containsEmptyRuntimeIn(Predicate[] predicates) {
    for (Predicate predicate : predicates) {
      if ("IN".equals(predicate.name()) && predicate.children().length == 1) {
        return true;
      }
    }
    return false;
  }

  private List<LanceSplit> pruneByShardingFilters(
      List<LanceSplit> allSplits, Predicate[] effectivePredicates) {
    if (activeShardingColumn == null
        || activeShardingExpression == null
        || fragmentShardingKeys == null) {
      return allSplits;
    }
    java.util.Optional<Set<Object>> allowedKeys =
        extractShardingKeys(effectivePredicates, activeShardingColumn);
    if (!allowedKeys.isPresent()) {
      return allSplits;
    }

    Set<Object> keys = allowedKeys.get();
    return allSplits.stream()
        .filter(
            split ->
                split.getFragments().stream()
                    .anyMatch(
                        fragmentId -> {
                          if (!fragmentShardingKeys.containsKey(fragmentId)) {
                            return true;
                          }
                          return keys.contains(fragmentShardingKeys.get(fragmentId));
                        }))
        .collect(Collectors.toList());
  }

  private java.util.Optional<Set<Object>> extractShardingKeys(
      Predicate[] predicates, String shardingColumn) {
    Set<Object> result = null;
    for (Predicate predicate : predicates) {
      java.util.Optional<Set<Object>> keys = analyzeShardingPredicate(predicate, shardingColumn);
      if (keys.isPresent()) {
        if (result == null) {
          result = new HashSet<>(keys.get());
        } else {
          result.retainAll(keys.get());
        }
      }
    }
    return result == null
        ? java.util.Optional.empty()
        : java.util.Optional.of(Collections.unmodifiableSet(result));
  }

  private java.util.Optional<Set<Object>> analyzeShardingPredicate(
      Predicate predicate, String shardingColumn) {
    if (predicate instanceof And) {
      java.util.Optional<Set<Object>> left =
          analyzeShardingPredicate(((And) predicate).left(), shardingColumn);
      java.util.Optional<Set<Object>> right =
          analyzeShardingPredicate(((And) predicate).right(), shardingColumn);
      if (left.isPresent() && right.isPresent()) {
        Set<Object> intersection = new HashSet<>(left.get());
        intersection.retainAll(right.get());
        return java.util.Optional.of(intersection);
      }
      return left.isPresent() ? left : right;
    }
    if (predicate instanceof Or) {
      java.util.Optional<Set<Object>> left =
          analyzeShardingPredicate(((Or) predicate).left(), shardingColumn);
      java.util.Optional<Set<Object>> right =
          analyzeShardingPredicate(((Or) predicate).right(), shardingColumn);
      if (left.isPresent() && right.isPresent()) {
        Set<Object> union = new HashSet<>(left.get());
        union.addAll(right.get());
        return java.util.Optional.of(union);
      }
      return java.util.Optional.empty();
    }
    if (predicate instanceof Not) {
      return java.util.Optional.empty();
    }

    Expression[] children = predicate.children();
    if ((!"=".equals(predicate.name()) && !"IN".equals(predicate.name()))
        || children.length == 0
        || !(children[0] instanceof NamedReference)
        || !shardingColumn.equals(columnName((NamedReference) children[0]))) {
      return java.util.Optional.empty();
    }

    Set<Object> keys = new HashSet<>();
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        return java.util.Optional.empty();
      }
      Object value = ((Literal<?>) children[i]).value();
      if (value != null) {
        keys.add(toShardingKey(normalizeLiteral(value)));
      }
    }
    return java.util.Optional.of(keys);
  }

  private Object toShardingKey(Object value) {
    if (activeShardingExpression instanceof BucketTransform) {
      Object buckets = ((BucketTransform) activeShardingExpression).numBuckets().value();
      return BucketHashUtil.computeBucketIdFromValue(value, ((Number) buckets).intValue());
    }
    return value;
  }

  private static Object normalizeLiteral(Object value) {
    if (value instanceof UTF8String) {
      return value.toString();
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
      return ((Number) value).longValue();
    }
    if (value instanceof Float) {
      return ((Float) value).doubleValue();
    }
    return value;
  }

  private static Predicate[] concatPredicates(Predicate[] left, Predicate[] right) {
    Predicate[] result = Arrays.copyOf(left, left.length + right.length);
    System.arraycopy(right, 0, result, left.length, right.length);
    return result;
  }

  private static String columnName(NamedReference reference) {
    return String.join(".", reference.fieldNames());
  }

  private static boolean isSpark41OrLater() {
    String[] components = org.apache.spark.package$.MODULE$.SPARK_VERSION().split("\\.");
    if (components.length < 2) {
      return false;
    }
    int major = Integer.parseInt(components[0]);
    int minor = Integer.parseInt(components[1]);
    return major > 4 || (major == 4 && minor >= 1);
  }

  private static final class CanonicalRuntimePredicate {
    final Predicate predicate;
    final String column;
    final int keyCount;
    final int serializedBytes;
    final String fingerprint;

    CanonicalRuntimePredicate(
        Predicate predicate, String column, int keyCount, int serializedBytes, String fingerprint) {
      this.predicate = predicate;
      this.column = column;
      this.keyCount = keyCount;
      this.serializedBytes = serializedBytes;
      this.fingerprint = fingerprint;
    }
  }

  /**
   * Reports the output partitioning to Spark's optimizer.
   *
   * <p>When a sharding-compatible column is detected via zonemap stats (every fragment has a single
   * distinct value for that column), we report the data column as the partition key. This enables
   * Spark's storage-partitioned join (SPJ) protocol for Lance tables and other data sources that
   * share the same sharding column.
   *
   * <p>When no sharding column is detected, returns {@link UnknownPartitioning}.
   */
  @Override
  public Partitioning outputPartitioning() {
    if (activeShardingExpression != null && fragmentShardingKeys != null) {
      Expression[] keys = new Expression[] {activeShardingExpression};
      return new KeyGroupedPartitioning(keys, fragmentShardingKeys.size());
    }
    return new UnknownPartitioning(numPartitions >= 0 ? numPartitions : 0);
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
    Predicate[] runtimePredicatesSnapshot = runtimePredicatesSnapshot();
    result =
        result.$plus(
            scala.Tuple2.apply("runtimePredicates", Arrays.toString(runtimePredicatesSnapshot)));
    result =
        result.$plus(
            scala.Tuple2.apply(
                "originalFragmentCount", Integer.toString(precomputedSplits.size())));
    result =
        result.$plus(
            scala.Tuple2.apply(
                "survivingFragmentCount",
                Integer.toString(
                    survivingFragmentCount >= 0
                        ? survivingFragmentCount
                        : precomputedSplits.size())));
    synchronized (runtimeFilterLock) {
      result =
          result.$plus(
              scala.Tuple2.apply(
                  "rejectedRuntimeFilterCount", Integer.toString(rejectedRuntimeFilterCount)));
    }
    if (readOptions.getFullTextQuery() != null) {
      result =
          result.$plus(
              scala.Tuple2.apply(
                  "fullTextQuery",
                  FullTextQueryUtils.fullTextQueryToString(readOptions.getFullTextQuery())));
    }
    return result;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    runtimeFilterLock = new Object();
  }

  @Override
  public Statistics estimateStatistics() {
    return statistics;
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return LanceCustomMetrics.allMetrics();
  }

  /**
   * Required for Spark's ReusedExchange: {@code BatchScanExec.equals()} compares {@code batch}
   * objects, which delegate to this method since LanceScan implements Batch.
   *
   * <p>Excludes {@code scanId} (per-instance tracing UUID, not scan identity).
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceScan that = (LanceScan) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(readOptions, that.readOptions)
        && Objects.equals(whereConditions, that.whereConditions)
        && Objects.equals(limit, that.limit)
        && Objects.equals(offset, that.offset)
        && Objects.equals(topNSortOrders.toString(), that.topNSortOrders.toString())
        && aggregationEquals(pushedAggregation, that.pushedAggregation)
        && equivalentPredicates(pushedPredicates, that.pushedPredicates);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            schema, readOptions, whereConditions, limit, offset, topNSortOrders.toString());
    result = 31 * result + Arrays.hashCode(sortedByHash(pushedPredicates));
    result = 31 * result + aggregationHashCode(pushedAggregation);
    return result;
  }

  /**
   * Compares two Optional&lt;Aggregation&gt; by value. {@code Aggregation}'s auto-generated {@code
   * equals()} uses reference identity for its array components, so we sort by hashCode and compare
   * element-wise — following {@code AggregatePushDownUtils.equivalentAggregations()}.
   */
  private static boolean aggregationEquals(Optional<Aggregation> a, Optional<Aggregation> b) {
    if (a.isPresent() != b.isPresent()) {
      return false;
    }
    if (!a.isPresent()) {
      return true;
    }
    Aggregation agg1 = a.get();
    Aggregation agg2 = b.get();
    return Arrays.equals(
            sortedByHash(agg1.aggregateExpressions()), sortedByHash(agg2.aggregateExpressions()))
        && Arrays.equals(
            sortedByHash(agg1.groupByExpressions()), sortedByHash(agg2.groupByExpressions()));
  }

  private static int aggregationHashCode(Optional<Aggregation> agg) {
    if (!agg.isPresent()) {
      return 0;
    }
    return Objects.hash(
        Arrays.hashCode(sortedByHash(agg.get().aggregateExpressions())),
        Arrays.hashCode(sortedByHash(agg.get().groupByExpressions())));
  }

  /**
   * Returns whether two predicate arrays are equivalent regardless of order. Follows Spark's {@code
   * FileScan.equivalentFilters()}: sort by hashCode, then compare element-wise.
   */
  private static boolean equivalentPredicates(Predicate[] a, Predicate[] b) {
    return Arrays.equals(sortedByHash(a), sortedByHash(b));
  }

  private static <T> T[] sortedByHash(T[] arr) {
    T[] copy = Arrays.copyOf(arr, arr.length);
    Arrays.sort(copy, (x, y) -> Integer.compare(x.hashCode(), y.hashCode()));
    return copy;
  }

  private static class LanceReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      LanceRuntime.enableOpenTelemetry();
      Preconditions.checkArgument(
          partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");
      return LanceRowPartitionReader.create((LanceInputPartition) partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      LanceRuntime.enableOpenTelemetry();
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
