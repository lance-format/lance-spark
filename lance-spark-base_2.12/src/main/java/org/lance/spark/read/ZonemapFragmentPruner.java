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
import org.lance.ipc.FragmentSlice;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Analyzes pushed Spark predicates against zonemap index statistics to determine which physical
 * fragment ranges can be scanned.
 *
 * <p>This is analogous to partition pruning in traditional data sources: if all zones within a
 * fragment provably cannot match a predicate, that fragment is eliminated from the scan — avoiding
 * fragment opens, scan setup, and task scheduling.
 *
 * <p>Zonemap pruning is inexact (conservative): it may include fragments that ultimately contain no
 * matching rows, but it will never exclude fragments that do contain matching rows.
 *
 * <p>Multiple predicates are treated as conjuncts (implicit AND); their physical ranges are
 * intersected. The original filter remains on the native scan, so zonemap results are always a
 * conservative prefilter.
 */
public final class ZonemapFragmentPruner {

  private static final Logger LOG = LoggerFactory.getLogger(ZonemapFragmentPruner.class);

  private ZonemapFragmentPruner() {}

  /**
   * Prune fragments, assuming the stats describe every fragment.
   *
   * @deprecated asserts full coverage, which cannot be checked here. Use {@link
   *     #pruneFragments(Predicate[], Map, Map)} and pass per-column coverage.
   */
  @Deprecated
  public static Optional<Set<Integer>> pruneFragments(
      Predicate[] pushedPredicates, Map<String, List<ZoneStats>> zonemapStatsByColumn) {
    return pruneFragments(pushedPredicates, zonemapStatsByColumn, Collections.emptyMap());
  }

  /**
   * Prune fragments using zonemap statistics, retaining fragments the stats do not describe.
   *
   * @param uncoveredFragmentsByColumn per column, the fragments its zones do not describe. An
   *     absent or empty entry means fully described.
   * @return fragment IDs that might match; empty if no pruning can be derived
   */
  public static Optional<Set<Integer>> pruneFragments(
      Predicate[] pushedPredicates,
      Map<String, List<ZoneStats>> zonemapStatsByColumn,
      Map<String, Set<Integer>> uncoveredFragmentsByColumn) {
    Set<Integer> allFragmentIds = new HashSet<>();
    if (zonemapStatsByColumn != null) {
      for (List<ZoneStats> zones : zonemapStatsByColumn.values()) {
        for (ZoneStats zone : zones) {
          allFragmentIds.add(zone.getFragmentId());
        }
      }
    }
    if (uncoveredFragmentsByColumn != null) {
      for (Set<Integer> uncovered : uncoveredFragmentsByColumn.values()) {
        allFragmentIds.addAll(uncovered);
      }
    }
    return planFragmentSlices(
            pushedPredicates, zonemapStatsByColumn, uncoveredFragmentsByColumn, allFragmentIds)
        .map(ZonemapScanPlan::getSurvivingFragmentIds);
  }

  /**
   * Plans physical fragment slices for pushed predicates.
   *
   * <p>The result distinguishes a full-fragment scan from a set of physical row ranges. A fragment
   * absent from both result sets is proven not to match and can be omitted. Unsupported predicates
   * are handled conservatively: they contribute no pruning under AND and force a full scan under
   * OR.
   */
  static Optional<ZonemapScanPlan> planFragmentSlices(
      Predicate[] pushedPredicates,
      Map<String, List<ZoneStats>> zonemapStatsByColumn,
      Map<String, Set<Integer>> uncoveredFragmentsByColumn,
      Set<Integer> allFragmentIds) {
    return planFragmentSlices(
        pushedPredicates,
        zonemapStatsByColumn,
        uncoveredFragmentsByColumn,
        allFragmentIds,
        Collections.emptyMap());
  }

  static Optional<ZonemapScanPlan> planFragmentSlices(
      Predicate[] pushedPredicates,
      Map<String, List<ZoneStats>> zonemapStatsByColumn,
      Map<String, Set<Integer>> uncoveredFragmentsByColumn,
      Set<Integer> allFragmentIds,
      Map<Integer, Long> physicalRowCounts) {
    if (pushedPredicates == null
        || pushedPredicates.length == 0
        || zonemapStatsByColumn == null
        || zonemapStatsByColumn.isEmpty()
        || allFragmentIds == null) {
      return Optional.empty();
    }

    Map<Integer, RangeSet> result = null;
    for (Predicate predicate : pushedPredicates) {
      Optional<Map<Integer, RangeSet>> predicateRanges =
          analyzePredicateRanges(
              predicate,
              zonemapStatsByColumn,
              uncoveredFragmentsByColumn,
              allFragmentIds,
              physicalRowCounts);
      if (!predicateRanges.isPresent()) {
        continue;
      }
      result =
          result == null
              ? predicateRanges.get()
              : combineRanges(result, predicateRanges.get(), allFragmentIds, true);
    }

    if (result == null) {
      return Optional.empty();
    }

    Set<Integer> fullScanFragments = new HashSet<>();
    Map<Integer, List<FragmentSlice>> slicesByFragment = new HashMap<>();
    for (int fragmentId : allFragmentIds) {
      RangeSet ranges = result.getOrDefault(fragmentId, RangeSet.full());
      Long physicalRowCount = physicalRowCounts == null ? null : physicalRowCounts.get(fragmentId);
      if (ranges.isFull() || ranges.coversWholeFragment(physicalRowCount)) {
        fullScanFragments.add(fragmentId);
      } else if (!ranges.isEmpty()) {
        List<FragmentSlice> slices = new ArrayList<>(ranges.ranges.size());
        for (RowRange range : ranges.ranges) {
          slices.add(new FragmentSlice(fragmentId, range.start, range.end - range.start));
        }
        slicesByFragment.put(fragmentId, slices);
      }
    }
    return Optional.of(new ZonemapScanPlan(fullScanFragments, slicesByFragment));
  }

  private static Optional<Map<Integer, RangeSet>> analyzePredicateRanges(
      Predicate predicate,
      Map<String, List<ZoneStats>> statsByColumn,
      Map<String, Set<Integer>> uncoveredByColumn,
      Set<Integer> allFragmentIds,
      Map<Integer, Long> physicalRowCounts) {
    if (predicate instanceof And) {
      Optional<Map<Integer, RangeSet>> left =
          analyzePredicateRanges(
              ((And) predicate).left(),
              statsByColumn,
              uncoveredByColumn,
              allFragmentIds,
              physicalRowCounts);
      Optional<Map<Integer, RangeSet>> right =
          analyzePredicateRanges(
              ((And) predicate).right(),
              statsByColumn,
              uncoveredByColumn,
              allFragmentIds,
              physicalRowCounts);
      if (left.isPresent() && right.isPresent()) {
        return Optional.of(combineRanges(left.get(), right.get(), allFragmentIds, true));
      }
      return left.isPresent() ? left : right;
    }
    if (predicate instanceof Or) {
      Optional<Map<Integer, RangeSet>> left =
          analyzePredicateRanges(
              ((Or) predicate).left(),
              statsByColumn,
              uncoveredByColumn,
              allFragmentIds,
              physicalRowCounts);
      Optional<Map<Integer, RangeSet>> right =
          analyzePredicateRanges(
              ((Or) predicate).right(),
              statsByColumn,
              uncoveredByColumn,
              allFragmentIds,
              physicalRowCounts);
      if (!left.isPresent() || !right.isPresent()) {
        return Optional.empty();
      }
      return Optional.of(combineRanges(left.get(), right.get(), allFragmentIds, false));
    }
    if (predicate instanceof Not) {
      return Optional.empty();
    }

    Expression[] children = predicate.children();
    switch (predicate.name()) {
      case "=":
        return comparisonRanges(
            children,
            statsByColumn,
            uncoveredByColumn,
            allFragmentIds,
            physicalRowCounts,
            ComparisonType.EQUALS);
      case "<":
        return comparisonRanges(
            children,
            statsByColumn,
            uncoveredByColumn,
            allFragmentIds,
            physicalRowCounts,
            ComparisonType.LESS_THAN);
      case "<=":
        return comparisonRanges(
            children,
            statsByColumn,
            uncoveredByColumn,
            allFragmentIds,
            physicalRowCounts,
            ComparisonType.LESS_THAN_OR_EQUAL);
      case ">":
        return comparisonRanges(
            children,
            statsByColumn,
            uncoveredByColumn,
            allFragmentIds,
            physicalRowCounts,
            ComparisonType.GREATER_THAN);
      case ">=":
        return comparisonRanges(
            children,
            statsByColumn,
            uncoveredByColumn,
            allFragmentIds,
            physicalRowCounts,
            ComparisonType.GREATER_THAN_OR_EQUAL);
      case "IN":
        return inRanges(
            children, statsByColumn, uncoveredByColumn, allFragmentIds, physicalRowCounts);
      case "IS_NULL":
        return nullRanges(
            children, statsByColumn, uncoveredByColumn, allFragmentIds, physicalRowCounts, true);
      case "IS_NOT_NULL":
        return nullRanges(
            children, statsByColumn, uncoveredByColumn, allFragmentIds, physicalRowCounts, false);
      default:
        return Optional.empty();
    }
  }

  @SuppressWarnings("unchecked")
  private static Optional<Map<Integer, RangeSet>> comparisonRanges(
      Expression[] children,
      Map<String, List<ZoneStats>> statsByColumn,
      Map<String, Set<Integer>> uncoveredByColumn,
      Set<Integer> allFragmentIds,
      Map<Integer, Long> physicalRowCounts,
      ComparisonType type) {
    if (children.length != 2
        || !(children[0] instanceof NamedReference)
        || !(children[1] instanceof Literal)) {
      return Optional.empty();
    }
    Object value = normalizeLiteral(((Literal<?>) children[1]).value());
    if (value == null) {
      return Optional.empty();
    }
    Comparable<Object> target;
    try {
      target = (Comparable<Object>) value;
    } catch (ClassCastException e) {
      return Optional.empty();
    }
    return matchingRanges(
        columnName((NamedReference) children[0]),
        statsByColumn,
        uncoveredByColumn,
        allFragmentIds,
        physicalRowCounts,
        zone -> zoneMatchesComparison(zone, target, type));
  }

  private static Optional<Map<Integer, RangeSet>> inRanges(
      Expression[] children,
      Map<String, List<ZoneStats>> statsByColumn,
      Map<String, Set<Integer>> uncoveredByColumn,
      Set<Integer> allFragmentIds,
      Map<Integer, Long> physicalRowCounts) {
    if (children.length < 1 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    List<Object> values = new ArrayList<>(children.length - 1);
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        return Optional.empty();
      }
      values.add(normalizeLiteral(((Literal<?>) children[i]).value()));
    }
    return matchingRanges(
        columnName((NamedReference) children[0]),
        statsByColumn,
        uncoveredByColumn,
        allFragmentIds,
        physicalRowCounts,
        zone -> {
          for (Object value : values) {
            if (value == null) {
              if (zone.getNullCount() > 0) {
                return true;
              }
            } else {
              @SuppressWarnings("unchecked")
              Comparable<Object> target = (Comparable<Object>) value;
              if (zoneMatchesComparison(zone, target, ComparisonType.EQUALS)) {
                return true;
              }
            }
          }
          return false;
        });
  }

  private static Optional<Map<Integer, RangeSet>> nullRanges(
      Expression[] children,
      Map<String, List<ZoneStats>> statsByColumn,
      Map<String, Set<Integer>> uncoveredByColumn,
      Set<Integer> allFragmentIds,
      Map<Integer, Long> physicalRowCounts,
      boolean isNull) {
    if (children.length != 1 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    return matchingRanges(
        columnName((NamedReference) children[0]),
        statsByColumn,
        uncoveredByColumn,
        allFragmentIds,
        physicalRowCounts,
        zone -> isNull ? zone.getNullCount() > 0 : zone.getNullCount() < zone.getZoneLength());
  }

  private static Optional<Map<Integer, RangeSet>> matchingRanges(
      String column,
      Map<String, List<ZoneStats>> statsByColumn,
      Map<String, Set<Integer>> uncoveredByColumn,
      Set<Integer> allFragmentIds,
      Map<Integer, Long> physicalRowCounts,
      ZoneMatcher matcher) {
    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null) {
      return Optional.empty();
    }

    Map<Integer, List<ZoneStats>> statsByFragment = new HashMap<>();
    for (ZoneStats zone : stats) {
      statsByFragment.computeIfAbsent(zone.getFragmentId(), ignored -> new ArrayList<>()).add(zone);
    }
    Set<Integer> uncovered =
        uncoveredByColumn == null
            ? Collections.emptySet()
            : uncoveredByColumn.getOrDefault(column, Collections.emptySet());

    Map<Integer, RangeSet> result = new HashMap<>();
    for (int fragmentId : allFragmentIds) {
      List<ZoneStats> fragmentStats = statsByFragment.get(fragmentId);
      if (uncovered.contains(fragmentId) || fragmentStats == null || fragmentStats.isEmpty()) {
        result.put(fragmentId, RangeSet.full());
        continue;
      }

      List<RowRange> matching = new ArrayList<>();
      List<RowRange> coverage = new ArrayList<>(fragmentStats.size());
      boolean invalidRange = false;
      Long physicalRowCount = physicalRowCounts == null ? null : physicalRowCounts.get(fragmentId);
      for (ZoneStats zone : fragmentStats) {
        long start = zone.getZoneStart();
        long length = zone.getZoneLength();
        long nullCount = zone.getNullCount();
        if (start < 0
            || length < 0
            || start > Long.MAX_VALUE - length
            || nullCount < 0
            || nullCount > length
            || (physicalRowCount != null
                && (physicalRowCount < 0 || start + length > physicalRowCount))) {
          LOG.warn(
              "Invalid zonemap physical range for column '{}': fragment={}, start={}, length={}; "
                  + "falling back to a full fragment scan",
              column,
              fragmentId,
              start,
              length);
          invalidRange = true;
          break;
        }
        RowRange range = new RowRange(start, start + length);
        coverage.add(range);
        if (length > 0) {
          try {
            if (matcher.matches(zone)) {
              matching.add(range);
            }
          } catch (RuntimeException e) {
            LOG.warn(
                "Incompatible zonemap metadata for column '{}', fragment={}; "
                    + "falling back to a full fragment scan: {}",
                column,
                fragmentId,
                e.toString());
            invalidRange = true;
            break;
          }
        }
      }
      if (!invalidRange
          && physicalRowCount != null
          && !coversPhysicalFragment(coverage, physicalRowCount)) {
        LOG.warn(
            "Incomplete zonemap coverage for column '{}', fragment={}: physical_row_count={}; "
                + "falling back to a full fragment scan",
            column,
            fragmentId,
            physicalRowCount);
        invalidRange = true;
      }
      result.put(fragmentId, invalidRange ? RangeSet.full() : RangeSet.of(matching));
    }
    return Optional.of(result);
  }

  private static boolean coversPhysicalFragment(List<RowRange> coverage, long physicalRowCount) {
    if (physicalRowCount == 0) {
      return coverage.isEmpty()
          || coverage.stream().allMatch(range -> range.start == 0 && range.end == 0);
    }
    List<RowRange> sorted = new ArrayList<>(coverage);
    sorted.sort(Comparator.comparingLong(range -> range.start));
    long coveredUntil = 0;
    for (RowRange range : sorted) {
      if (range.start > coveredUntil) {
        return false;
      }
      coveredUntil = Math.max(coveredUntil, range.end);
    }
    return coveredUntil == physicalRowCount;
  }

  private static Map<Integer, RangeSet> combineRanges(
      Map<Integer, RangeSet> left,
      Map<Integer, RangeSet> right,
      Set<Integer> allFragmentIds,
      boolean intersect) {
    Map<Integer, RangeSet> result = new HashMap<>();
    for (int fragmentId : allFragmentIds) {
      RangeSet leftRanges = left.getOrDefault(fragmentId, RangeSet.full());
      RangeSet rightRanges = right.getOrDefault(fragmentId, RangeSet.full());
      result.put(
          fragmentId,
          intersect ? leftRanges.intersect(rightRanges) : leftRanges.union(rightRanges));
    }
    return result;
  }

  @FunctionalInterface
  private interface ZoneMatcher {
    boolean matches(ZoneStats zone);
  }

  private static final class RowRange {
    private final long start;
    private final long end;

    private RowRange(long start, long end) {
      this.start = start;
      this.end = end;
    }
  }

  private static final class RangeSet {
    private final boolean full;
    private final List<RowRange> ranges;

    private RangeSet(boolean full, List<RowRange> ranges) {
      this.full = full;
      this.ranges = ranges;
    }

    private static RangeSet full() {
      return new RangeSet(true, Collections.emptyList());
    }

    private static RangeSet of(List<RowRange> ranges) {
      if (ranges.isEmpty()) {
        return new RangeSet(false, Collections.emptyList());
      }
      List<RowRange> sorted = new ArrayList<>(ranges);
      sorted.sort(Comparator.comparingLong(range -> range.start));
      List<RowRange> merged = new ArrayList<>();
      RowRange current = sorted.get(0);
      for (int i = 1; i < sorted.size(); i++) {
        RowRange next = sorted.get(i);
        if (next.start <= current.end) {
          current = new RowRange(current.start, Math.max(current.end, next.end));
        } else {
          merged.add(current);
          current = next;
        }
      }
      merged.add(current);
      return new RangeSet(false, Collections.unmodifiableList(merged));
    }

    private boolean isFull() {
      return full;
    }

    private boolean isEmpty() {
      return !full && ranges.isEmpty();
    }

    private boolean coversWholeFragment(Long physicalRowCount) {
      return physicalRowCount != null
          && physicalRowCount > 0
          && ranges.size() == 1
          && ranges.get(0).start == 0
          && ranges.get(0).end == physicalRowCount;
    }

    private RangeSet intersect(RangeSet other) {
      if (full) {
        return other;
      }
      if (other.full) {
        return this;
      }
      List<RowRange> intersection = new ArrayList<>();
      int leftIndex = 0;
      int rightIndex = 0;
      while (leftIndex < ranges.size() && rightIndex < other.ranges.size()) {
        RowRange left = ranges.get(leftIndex);
        RowRange right = other.ranges.get(rightIndex);
        long start = Math.max(left.start, right.start);
        long end = Math.min(left.end, right.end);
        if (start < end) {
          intersection.add(new RowRange(start, end));
        }
        if (left.end < right.end) {
          leftIndex++;
        } else {
          rightIndex++;
        }
      }
      return of(intersection);
    }

    private RangeSet union(RangeSet other) {
      if (full || other.full) {
        return full();
      }
      List<RowRange> union = new ArrayList<>(ranges.size() + other.ranges.size());
      union.addAll(ranges);
      union.addAll(other.ranges);
      return of(union);
    }
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

    switch (type) {
      case EQUALS:
        return target.compareTo(min) >= 0 && target.compareTo(max) <= 0;
      case LESS_THAN:
        return min.compareTo(target) < 0;
      case LESS_THAN_OR_EQUAL:
        return min.compareTo(target) <= 0;
      case GREATER_THAN:
        return max.compareTo(target) > 0;
      case GREATER_THAN_OR_EQUAL:
        return max.compareTo(target) >= 0;
      default:
        return true;
    }
  }

  private static String columnName(NamedReference ref) {
    String[] names = ref.fieldNames();
    return names.length == 1 ? names[0] : String.join(".", names);
  }

  /**
   * V2 {@link Literal} exposes values in Spark's internal representation, while Lance's JNI
   * materializes {@code ZoneStats.min/max} with every integer width boxed as {@code Long} and every
   * floating-point width as {@code Double}. Widen narrow Java boxed primitives (Byte / Short /
   * Integer / Float) to match — otherwise an Integer literal against a Long zone bound would throw
   * {@code ClassCastException} from {@code Comparable.compareTo}. Also normalizes {@code
   * UTF8String} → {@code String} for the same reason.
   */
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

  private enum ComparisonType {
    EQUALS,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL
  }
}
