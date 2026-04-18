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
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  public static final class Assignment implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int fragmentId;
    private final Comparable<?> value;

    Assignment(int fragmentId, Comparable<?> value) {
      this.fragmentId = fragmentId;
      this.value = value;
    }

    public int getFragmentId() {
      return fragmentId;
    }

    public Comparable<?> getValue() {
      return value;
    }
  }

  public static final class PartitionInfo implements Serializable {
    private static final long serialVersionUID = 2L;
    private final String columnName;
    private final List<Assignment> assignments;
    private final int distinctPartitionCount;

    public PartitionInfo(String columnName, List<Assignment> assignments) {
      this.columnName = Objects.requireNonNull(columnName, "columnName");
      this.assignments = Collections.unmodifiableList(new ArrayList<>(assignments));
      Set<Object> distinct = new HashSet<>();
      for (Assignment a : this.assignments) {
        distinct.add(a.getValue());
      }
      this.distinctPartitionCount = distinct.size();
    }

    public String getColumnName() {
      return columnName;
    }

    public List<Assignment> getAssignments() {
      return assignments;
    }

    public int getDistinctPartitionCount() {
      return distinctPartitionCount;
    }

    /** SQL predicate for a partition value, using the same compiler as filter pushdown. */
    public String compilePredicate(Comparable<?> value) {
      Objects.requireNonNull(value, "partition value must not be null");
      org.apache.spark.sql.sources.Filter filter =
          new org.apache.spark.sql.sources.EqualTo(columnName, value);
      org.lance.spark.utils.Optional<String> compiled =
          FilterPushDown.compileFiltersToSqlWhereClause(
              new org.apache.spark.sql.sources.Filter[] {filter});
      if (!compiled.isPresent()) {
        throw new IllegalStateException(
            "EqualTo(" + columnName + ", " + value + ") compiled to empty SQL");
      }
      return compiled.get();
    }

    public InternalRow partitionKeyFor(Comparable<?> value) {
      return new GenericInternalRow(new Object[] {toSparkValue(value)});
    }

    private static Object toSparkValue(Comparable<?> value) {
      if (value == null) {
        return null;
      }
      if (value instanceof String) {
        return UTF8String.fromString((String) value);
      }
      return value;
    }
  }

  static final int LANCE_SPJ_MAX_ASSIGNMENTS = 10_000;

  public static Optional<PartitionInfo> computeZonePartitions(
      String columnName, List<ZoneStats> zones) {
    if (zones == null || zones.isEmpty()) {
      return Optional.empty();
    }
    Set<Map.Entry<Integer, Comparable<?>>> seen = new HashSet<>();
    List<Assignment> assignments = new ArrayList<>();
    for (ZoneStats zone : zones) {
      Comparable<?> min = zone.getMin();
      Comparable<?> max = zone.getMax();
      if (min == null || max == null || !min.equals(max)) {
        LOG.info(
            "SPJ disabled for '{}': min!=max (frag={}, min={}, max={})",
            columnName,
            zone.getFragmentId(),
            min,
            max);
        return Optional.empty();
      }
      if (zone.getNullCount() > 0) {
        LOG.info(
            "SPJ disabled for '{}': nulls in zone (frag={}, nullCount={})",
            columnName,
            zone.getFragmentId(),
            zone.getNullCount());
        return Optional.empty();
      }
      if (!isPartitionValueTypeSupported(min)) {
        LOG.info(
            "SPJ disabled for '{}': type {} not in allowlist",
            columnName,
            min.getClass().getName());
        return Optional.empty();
      }
      Map.Entry<Integer, Comparable<?>> key =
          new AbstractMap.SimpleImmutableEntry<>(zone.getFragmentId(), min);
      if (seen.add(key)) {
        assignments.add(new Assignment(zone.getFragmentId(), min));
        if (assignments.size() > LANCE_SPJ_MAX_ASSIGNMENTS) {
          LOG.warn(
              "SPJ disabled for '{}': exceeded max assignments {}",
              columnName,
              LANCE_SPJ_MAX_ASSIGNMENTS);
          return Optional.empty();
        }
      }
    }
    return Optional.of(new PartitionInfo(columnName, assignments));
  }

  static boolean isPartitionValueTypeSupported(Object value) {
    return value instanceof String
        || value instanceof Long
        || value instanceof Integer
        || value instanceof Boolean;
  }
}
