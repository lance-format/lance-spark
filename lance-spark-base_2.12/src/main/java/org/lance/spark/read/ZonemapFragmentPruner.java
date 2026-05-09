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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Analyzes pushed Spark filters against zonemap index statistics to determine which fragments can
 * be pruned.
 *
 * <p>This is analogous to partition pruning in traditional data sources: if all zones within a
 * fragment provably cannot match a filter predicate, that fragment is eliminated from the scan —
 * avoiding fragment opens, scan setup, and task scheduling.
 *
 * <p>Zonemap pruning is inexact (conservative): it may include fragments that ultimately contain no
 * matching rows, but it will never exclude fragments that do contain matching rows.
 *
 * <p>Multiple filters are treated as conjuncts (implicit AND); their fragment sets are intersected.
 * For each column that has both a pushed filter and zonemap stats, we evaluate which fragments
 * could possibly match. Multiple columns produce independent fragment sets that are intersected.
 */
public final class ZonemapFragmentPruner {

  private static final Logger LOG = LoggerFactory.getLogger(ZonemapFragmentPruner.class);

  private ZonemapFragmentPruner() {}

  /**
   * Prune fragments using zonemap statistics.
   *
   * @param pushedFilters the filters pushed down by Spark
   * @param zonemapStatsByColumn map from column name to its zonemap zone stats
   * @return present with the set of fragment IDs that might match; empty if no pruning can be
   *     derived
   */
  public static Optional<Set<Integer>> pruneFragments(
      Filter[] pushedFilters, Map<String, List<ZoneStats>> zonemapStatsByColumn) {

    if (pushedFilters == null
        || pushedFilters.length == 0
        || zonemapStatsByColumn == null
        || zonemapStatsByColumn.isEmpty()) {
      return Optional.empty();
    }

    // Multiple top-level filters are implicitly ANDed by Spark.
    // We intersect the fragment sets from each filter that provides one.
    Set<Integer> result = null;
    for (Filter filter : pushedFilters) {
      Optional<Set<Integer>> fragmentIds = analyzeFilter(filter, zonemapStatsByColumn);
      if (fragmentIds.isPresent()) {
        if (result == null) {
          result = new HashSet<>(fragmentIds.get());
        } else {
          result.retainAll(fragmentIds.get());
        }
      }
    }

    if (result == null) {
      return Optional.empty();
    }

    return Optional.of(Collections.unmodifiableSet(result));
  }

  /**
   * Recursively analyzes a single filter to extract fragment IDs from zonemap constraints.
   *
   * <p>CONTRACT: when present, the returned Set is always a fresh mutable {@link HashSet} that is
   * not aliased by any other reference. Callers may freely mutate it.
   */
  private static Optional<Set<Integer>> analyzeFilter(
      Filter filter, Map<String, List<ZoneStats>> statsByColumn) {

    if (filter instanceof EqualTo) {
      return analyzeComparison(
          ((EqualTo) filter).attribute(),
          ((EqualTo) filter).value(),
          statsByColumn,
          ComparisonType.EQUALS);
    } else if (filter instanceof LessThan) {
      return analyzeComparison(
          ((LessThan) filter).attribute(),
          ((LessThan) filter).value(),
          statsByColumn,
          ComparisonType.LESS_THAN);
    } else if (filter instanceof LessThanOrEqual) {
      return analyzeComparison(
          ((LessThanOrEqual) filter).attribute(),
          ((LessThanOrEqual) filter).value(),
          statsByColumn,
          ComparisonType.LESS_THAN_OR_EQUAL);
    } else if (filter instanceof GreaterThan) {
      return analyzeComparison(
          ((GreaterThan) filter).attribute(),
          ((GreaterThan) filter).value(),
          statsByColumn,
          ComparisonType.GREATER_THAN);
    } else if (filter instanceof GreaterThanOrEqual) {
      return analyzeComparison(
          ((GreaterThanOrEqual) filter).attribute(),
          ((GreaterThanOrEqual) filter).value(),
          statsByColumn,
          ComparisonType.GREATER_THAN_OR_EQUAL);
    } else if (filter instanceof In) {
      return analyzeIn(((In) filter).attribute(), ((In) filter).values(), statsByColumn);
    } else if (filter instanceof IsNull) {
      return analyzeIsNull(((IsNull) filter).attribute(), statsByColumn);
    } else if (filter instanceof IsNotNull) {
      return analyzeIsNotNull(((IsNotNull) filter).attribute(), statsByColumn);
    } else if (filter instanceof And) {
      return analyzeAnd((And) filter, statsByColumn);
    } else if (filter instanceof Or) {
      return analyzeOr((Or) filter, statsByColumn);
    } else if (filter instanceof Not) {
      // Cannot safely prune for NOT filters — any fragment might match the negation.
      return Optional.empty();
    }

    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  private static Optional<Set<Integer>> analyzeComparison(
      String column,
      Object value,
      Map<String, List<ZoneStats>> statsByColumn,
      ComparisonType type) {

    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null || value == null) {
      return Optional.empty();
    }

    Comparable<Object> target;
    try {
      target = (Comparable<Object>) value;
    } catch (ClassCastException e) {
      LOG.warn("Cannot cast filter value {} to Comparable for zonemap pruning", value);
      return Optional.empty();
    }

    Set<Integer> matchingFragments = new HashSet<>();
    for (ZoneStats zone : stats) {
      if (zoneMatchesComparison(zone, target, type)) {
        matchingFragments.add(zone.getFragmentId());
      }
    }

    return Optional.of(matchingFragments);
  }

  @SuppressWarnings("unchecked")
  private static boolean zoneMatchesComparison(
      ZoneStats zone, Comparable<Object> target, ComparisonType type) {

    Comparable<Object> min = (Comparable<Object>) zone.getMin();
    Comparable<Object> max = (Comparable<Object>) zone.getMax();

    // If min or max is null, the zone contains only nulls for the indexed range;
    // non-null comparisons cannot match.
    if (min == null || max == null) {
      return false;
    }

    try {
      switch (type) {
        case EQUALS:
          // target ∈ [min, max]
          return target.compareTo(min) >= 0 && target.compareTo(max) <= 0;
        case LESS_THAN:
          // ∃ row < target  ⟺  zone.min < target
          return min.compareTo(target) < 0;
        case LESS_THAN_OR_EQUAL:
          return min.compareTo(target) <= 0;
        case GREATER_THAN:
          return max.compareTo(target) > 0;
        case GREATER_THAN_OR_EQUAL:
          return max.compareTo(target) >= 0;
        default:
          return true; // conservative
      }
    } catch (ClassCastException e) {
      // Type mismatch between filter value and zone stats — be conservative
      LOG.warn("Type mismatch in zonemap comparison, skipping pruning for zone", e);
      return true;
    }
  }

  @SuppressWarnings("unchecked")
  private static Optional<Set<Integer>> analyzeIn(
      String column, Object[] values, Map<String, List<ZoneStats>> statsByColumn) {

    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null) {
      return Optional.empty();
    }

    Set<Integer> matchingFragments = new HashSet<>();
    for (ZoneStats zone : stats) {
      for (Object value : values) {
        if (value == null) {
          if (zone.getNullCount() > 0) {
            matchingFragments.add(zone.getFragmentId());
            break;
          }
        } else {
          try {
            Comparable<Object> target = (Comparable<Object>) value;
            if (zoneMatchesComparison(zone, target, ComparisonType.EQUALS)) {
              matchingFragments.add(zone.getFragmentId());
              break;
            }
          } catch (ClassCastException e) {
            // Non-comparable value, conservatively include
            matchingFragments.add(zone.getFragmentId());
            break;
          }
        }
      }
    }

    return Optional.of(matchingFragments);
  }

  private static Optional<Set<Integer>> analyzeIsNull(
      String column, Map<String, List<ZoneStats>> statsByColumn) {

    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null) {
      return Optional.empty();
    }

    Set<Integer> matchingFragments = new HashSet<>();
    for (ZoneStats zone : stats) {
      if (zone.getNullCount() > 0) {
        matchingFragments.add(zone.getFragmentId());
      }
    }

    return Optional.of(matchingFragments);
  }

  private static Optional<Set<Integer>> analyzeIsNotNull(
      String column, Map<String, List<ZoneStats>> statsByColumn) {

    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null) {
      return Optional.empty();
    }

    Set<Integer> matchingFragments = new HashSet<>();
    for (ZoneStats zone : stats) {
      // Zone has non-null rows if zoneLength exceeds nullCount.
      // zoneLength is the row offset span (may include gaps from deletions),
      // so this is conservative: we include a zone even if only the offset range
      // implies there might be non-null values.
      if (zone.getNullCount() < zone.getZoneLength()) {
        matchingFragments.add(zone.getFragmentId());
      }
    }

    return Optional.of(matchingFragments);
  }

  private static Optional<Set<Integer>> analyzeAnd(
      And filter, Map<String, List<ZoneStats>> statsByColumn) {
    Optional<Set<Integer>> left = analyzeFilter(filter.left(), statsByColumn);
    Optional<Set<Integer>> right = analyzeFilter(filter.right(), statsByColumn);

    if (left.isPresent() && right.isPresent()) {
      // Intersect both sides
      Set<Integer> intersection = new HashSet<>(left.get());
      intersection.retainAll(right.get());
      return Optional.of(intersection);
    }
    // Only one side constrains — return that side
    if (left.isPresent()) return left;
    if (right.isPresent()) return right;
    return Optional.empty();
  }

  private static Optional<Set<Integer>> analyzeOr(
      Or filter, Map<String, List<ZoneStats>> statsByColumn) {
    Optional<Set<Integer>> left = analyzeFilter(filter.left(), statsByColumn);
    Optional<Set<Integer>> right = analyzeFilter(filter.right(), statsByColumn);

    // For OR, both sides must constrain to allow pruning.
    // If either side is unconstrained, any fragment could match.
    if (left.isPresent() && right.isPresent()) {
      Set<Integer> union = new HashSet<>(left.get());
      union.addAll(right.get());
      return Optional.of(union);
    }
    return Optional.empty();
  }

  private enum ComparisonType {
    EQUALS,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL
  }

  /**
   * Result of partition detection: the ordered list of partition column names, a parallel list of
   * Spark {@link DataType}s (used to encode each fragment's tuple into an {@link InternalRow}), and
   * a map from fragment ID to the partition tuple (one value per declared column, in declaration
   * order).
   *
   * <p>Width invariants (enforced by the constructor): {@code columnTypes.size()} equals {@code
   * columnNames.size()}; every tuple has length {@code columnNames.size()}; column names are
   * distinct and non-empty.
   */
  public static final class PartitionInfo implements Serializable {
    // Bumped from the single-column shape (1L on upstream main): adds columnTypes and uses
    // tuple storage (Map<Integer, Comparable<?>[]>) for multi-column type-aware encoding.
    private static final long serialVersionUID = 2L;

    private final List<String> columnNames;
    private final List<DataType> columnTypes;
    private final Map<Integer, Comparable<?>[]> fragmentPartitionKeys;
    private final boolean softCapped;

    public PartitionInfo(
        List<String> columnNames,
        List<DataType> columnTypes,
        Map<Integer, Comparable<?>[]> fragmentPartitionKeys) {
      this(columnNames, columnTypes, fragmentPartitionKeys, false);
    }

    public PartitionInfo(
        List<String> columnNames,
        List<DataType> columnTypes,
        Map<Integer, Comparable<?>[]> fragmentPartitionKeys,
        boolean softCapped) {
      if (columnNames == null || columnNames.isEmpty()) {
        throw new IllegalArgumentException("columnNames must be non-empty");
      }
      if (columnTypes == null || columnTypes.size() != columnNames.size()) {
        throw new IllegalArgumentException("columnTypes must have the same size as columnNames");
      }
      if (new HashSet<>(columnNames).size() != columnNames.size()) {
        throw new IllegalArgumentException("columnNames must be distinct: " + columnNames);
      }
      int width = columnNames.size();
      Map<Integer, Comparable<?>[]> copy = new HashMap<>();
      for (Map.Entry<Integer, Comparable<?>[]> e : fragmentPartitionKeys.entrySet()) {
        Comparable<?>[] tuple = e.getValue();
        if (tuple == null || tuple.length != width) {
          throw new IllegalArgumentException(
              "tuple for fragment " + e.getKey() + " must have length " + width);
        }
        copy.put(e.getKey(), tuple.clone());
      }
      this.columnNames = Collections.unmodifiableList(new java.util.ArrayList<>(columnNames));
      this.columnTypes = Collections.unmodifiableList(new java.util.ArrayList<>(columnTypes));
      this.fragmentPartitionKeys = Collections.unmodifiableMap(copy);
      this.softCapped = softCapped;
    }

    /**
     * Factory for the single-column case. Wraps each scalar partition value into a length-1 tuple
     * and delegates to the list-form constructor.
     */
    public static PartitionInfo forSingleColumn(
        String columnName, DataType columnType, Map<Integer, Comparable<?>> valueByFragment) {
      Map<Integer, Comparable<?>[]> tupleMap = new HashMap<>();
      for (Map.Entry<Integer, Comparable<?>> e : valueByFragment.entrySet()) {
        tupleMap.put(e.getKey(), new Comparable<?>[] {e.getValue()});
      }
      return new PartitionInfo(
          Collections.singletonList(columnName), Collections.singletonList(columnType), tupleMap);
    }

    public List<String> getColumnNames() {
      return columnNames;
    }

    public List<DataType> getColumnTypes() {
      return columnTypes;
    }

    /**
     * Returns the fragment-id → tuple map as an unmodifiable snapshot; each tuple array is
     * defensively cloned on every call so mutating the returned arrays cannot corrupt internal
     * state. Prefer {@link #partitionKeyForFragment(int)} for hot paths — this getter exists for
     * inspection, equality checks, and serialization round-trip tests.
     */
    public Map<Integer, Comparable<?>[]> getFragmentPartitionKeys() {
      Map<Integer, Comparable<?>[]> snapshot = new HashMap<>(fragmentPartitionKeys.size());
      for (Map.Entry<Integer, Comparable<?>[]> e : fragmentPartitionKeys.entrySet()) {
        snapshot.put(e.getKey(), e.getValue().clone());
      }
      return Collections.unmodifiableMap(snapshot);
    }

    public int size() {
      return fragmentPartitionKeys.size();
    }

    public boolean isSoftCapped() {
      return softCapped;
    }

    /**
     * Returns a new PartitionInfo restricted to the given fragment-id set. Preserves column order
     * and tuple shape. The {@code softCapped} flag is NOT carried over because the cap decision is
     * a function of size; if the restricted size still exceeds the cap, the caller must re-apply it
     * via {@link #withSoftCapped()}. Used after filter pushdown narrows the surviving fragment set.
     */
    public PartitionInfo restrictTo(Set<Integer> survivingFragmentIds) {
      Map<Integer, Comparable<?>[]> restricted = new HashMap<>();
      for (Map.Entry<Integer, Comparable<?>[]> e : fragmentPartitionKeys.entrySet()) {
        if (survivingFragmentIds.contains(e.getKey())) {
          restricted.put(e.getKey(), e.getValue());
        }
      }
      return new PartitionInfo(columnNames, columnTypes, restricted, false);
    }

    /** Marks this info as soft-capped, returning a new instance (immutability preserved). */
    public PartitionInfo withSoftCapped() {
      return new PartitionInfo(columnNames, columnTypes, fragmentPartitionKeys, true);
    }

    /**
     * Returns a partition key {@link InternalRow} for the given fragment ID. The row contains one
     * or more columns (in declaration order), each converted to a Spark-compatible type.
     */
    public InternalRow partitionKeyForFragment(int fragmentId) {
      Comparable<?>[] tuple = fragmentPartitionKeys.get(fragmentId);
      int width = columnNames.size();
      Object[] out = new Object[width];
      if (tuple == null) {
        return new GenericInternalRow(out);
      }
      for (int i = 0; i < width; i++) {
        out[i] = toSparkValue(tuple[i], columnTypes.get(i));
      }
      return new GenericInternalRow(out);
    }

    /**
     * Converts a ZoneStats value to the exact Java class Spark's {@link InternalRow} expects for
     * the target slot. ZoneStats returns {@code Long} for every integral Arrow width (int8/16/32
     * included) and for Date (epoch-days) / Timestamp (epoch-micros); those need explicit narrowing
     * to match {@code getByte}/{@code getShort}/{@code getInt} accessors. Boolean is already typed;
     * Strings are wrapped in {@link UTF8String}.
     */
    private static Object toSparkValue(Comparable<?> value, DataType type) {
      if (value == null) {
        return null;
      }
      if (DataTypes.BooleanType.equals(type)) {
        return value;
      }
      if (DataTypes.ByteType.equals(type)) {
        return ((Number) value).byteValue();
      }
      if (DataTypes.ShortType.equals(type)) {
        return ((Number) value).shortValue();
      }
      if (DataTypes.IntegerType.equals(type) || DataTypes.DateType.equals(type)) {
        return ((Number) value).intValue();
      }
      if (DataTypes.LongType.equals(type) || DataTypes.TimestampType.equals(type)) {
        return ((Number) value).longValue();
      }
      if (DataTypes.StringType.equals(type)) {
        return UTF8String.fromString((String) value);
      }
      throw new IllegalArgumentException("Unsupported partition column type: " + type);
    }
  }

  /**
   * Checks whether zonemap zones are partitionable — i.e., every fragment has exactly one distinct
   * value (all zones have {@code min == max} with the same value per fragment).
   *
   * @param zones zonemap zones for a single column
   * @return map from fragment ID to partition value, or empty if zones are not partitionable
   */
  static Optional<Map<Integer, Comparable<?>>> computeFragmentPartitionValues(
      List<ZoneStats> zones) {

    if (zones == null || zones.isEmpty()) {
      return Optional.empty();
    }

    Map<Integer, Comparable<?>> result = new HashMap<>();

    for (ZoneStats zone : zones) {
      Comparable<?> min = zone.getMin();
      Comparable<?> max = zone.getMax();

      if (min == null || max == null) {
        return Optional.empty();
      }

      if (!min.equals(max)) {
        return Optional.empty();
      }

      int fragId = zone.getFragmentId();
      Comparable<?> existing = result.get(fragId);
      if (existing != null && !existing.equals(min)) {
        return Optional.empty();
      }

      result.put(fragId, min);
    }

    return Optional.of(result);
  }
}
