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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
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
import java.util.stream.Collectors;

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
 * <p>Multiple predicates form an implicit AND. By default, each predicate selects whole fragments
 * and their fragment sets are intersected. When the scan enables slices, the intersection uses
 * physical row ranges within each fragment. The original filter still checks the selected rows.
 */
public final class ZonemapFragmentPruner {

  private static final Logger LOG = LoggerFactory.getLogger(ZonemapFragmentPruner.class);

  private final Map<String, List<ZoneStats>> statsByColumn;
  private final Map<String, Set<Integer>> uncoveredByColumn;
  private final Set<Integer> allFragmentIds;
  private final boolean useSlices;

  private ZonemapFragmentPruner(
      Map<String, List<ZoneStats>> statsByColumn,
      Map<String, Set<Integer>> uncoveredByColumn,
      Set<Integer> allFragmentIds,
      boolean useSlices) {
    this.statsByColumn = statsByColumn;
    this.uncoveredByColumn = uncoveredByColumn;
    this.allFragmentIds = allFragmentIds;
    this.useSlices = useSlices;
  }

  /**
   * A column may have several zonemap indexes, but {@code getZonemapStats} returns zones from only
   * one. Combining their index metadata could claim coverage for fragments absent from those zones.
   */
  static Map<String, Set<Integer>> uncoveredFragments(
      Map<String, List<ZoneStats>> stats, Set<Integer> liveFragmentIds) {
    Map<String, Set<Integer>> uncoveredByColumn = new HashMap<>();
    for (Map.Entry<String, List<ZoneStats>> entry : stats.entrySet()) {
      Set<Integer> described = new HashSet<>();
      for (ZoneStats zone : entry.getValue()) {
        described.add(zone.getFragmentId());
      }
      Set<Integer> uncovered = new HashSet<>(liveFragmentIds);
      uncovered.removeAll(described);
      uncoveredByColumn.put(entry.getKey(), uncovered);
      if (!uncovered.isEmpty()) {
        LOG.info(
            "Zonemap zones for '{}' describe {} of {} fragments;"
                + " retaining {} unindexed fragment(s) in the scan",
            entry.getKey(),
            liveFragmentIds.size() - uncovered.size(),
            liveFragmentIds.size(),
            uncovered.size());
      }
    }

    return uncoveredByColumn;
  }

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
    return pruneFragments(
        pushedPredicates, zonemapStatsByColumn, uncoveredFragmentsByColumn, allFragmentIds);
  }

  static Optional<Set<Integer>> pruneFragments(
      Predicate[] pushedPredicates,
      Map<String, List<ZoneStats>> zonemapStatsByColumn,
      Map<String, Set<Integer>> uncoveredFragmentsByColumn,
      Set<Integer> allFragmentIds) {
    return matchingFragmentRanges(
            pushedPredicates,
            zonemapStatsByColumn,
            uncoveredFragmentsByColumn,
            allFragmentIds,
            false)
        .map(
            ranges ->
                ranges.entrySet().stream()
                    .filter(entry -> !entry.getValue().isEmpty())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toUnmodifiableSet()));
  }

  /**
   * Selects whole fragments or physical row ranges for pushed predicates.
   *
   * <p>An empty Optional means the predicates yield no pruning decision. A present map covers every
   * live fragment. A full range keeps the fragment, an empty range rules it out, and bounded ranges
   * identify physical rows that may match. An unsupported branch under AND still allows pruning by
   * the other branch. Under OR either branch could supply matching rows.
   */
  static Optional<Map<Integer, RangeSet>> matchingFragmentRanges(
      Predicate[] pushedPredicates,
      Map<String, List<ZoneStats>> zonemapStatsByColumn,
      Map<String, Set<Integer>> uncoveredFragmentsByColumn,
      Set<Integer> allFragmentIds,
      boolean useSlices) {
    if (pushedPredicates == null
        || pushedPredicates.length == 0
        || zonemapStatsByColumn == null
        || zonemapStatsByColumn.isEmpty()
        || allFragmentIds == null) {
      return Optional.empty();
    }

    ZonemapFragmentPruner pruner =
        new ZonemapFragmentPruner(
            zonemapStatsByColumn, uncoveredFragmentsByColumn, allFragmentIds, useSlices);
    Map<Integer, RangeSet> result = null;
    for (Predicate predicate : pushedPredicates) {
      Optional<Map<Integer, RangeSet>> predicateRanges = pruner.analyzePredicateRanges(predicate);
      if (!predicateRanges.isPresent()) {
        continue;
      }
      result =
          result == null
              ? predicateRanges.get()
              : combineRanges(result, predicateRanges.get(), allFragmentIds, true);
    }

    return Optional.ofNullable(result);
  }

  /**
   * Rescales Lance zonemap bounds to Spark's units (microseconds for timestamps, days for dates).
   * Uses floor division to match the reader's rounding and avoid pruning matching fragments.
   *
   * <p>UInt64 bounds are skipped because signed long comparisons can break their ordering.
   *
   * @return rescaled zones, or null if the bounds cannot be safely converted or compared
   */
  public static List<ZoneStats> toSparkUnits(List<ZoneStats> zones, ArrowType type) {
    long multiplier = 1;
    long divisor = 1;
    if (type instanceof ArrowType.Int
        && !((ArrowType.Int) type).getIsSigned()
        && ((ArrowType.Int) type).getBitWidth() == 64) {
      return null;
    } else if (type instanceof ArrowType.Timestamp) {
      switch (((ArrowType.Timestamp) type).getUnit()) {
        case SECOND:
          multiplier = 1_000_000L;
          break;
        case MILLISECOND:
          multiplier = 1_000L;
          break;
        case NANOSECOND:
          divisor = 1_000L;
          break;
        default:
          return zones;
      }
    } else if (type instanceof ArrowType.Date
        && ((ArrowType.Date) type).getUnit() == DateUnit.MILLISECOND) {
      divisor = 86_400_000L;
    } else {
      return zones;
    }

    List<ZoneStats> rescaled = new ArrayList<>(zones.size());
    try {
      for (ZoneStats zone : zones) {
        rescaled.add(
            new ZoneStats(
                zone.getFragmentId(),
                zone.getZoneStart(),
                zone.getZoneLength(),
                rescale(zone.getMin(), multiplier, divisor),
                rescale(zone.getMax(), multiplier, divisor),
                zone.getNullCount()));
      }
    } catch (ArithmeticException | ClassCastException e) {
      return null;
    }
    return rescaled;
  }

  private static Long rescale(Comparable<?> bound, long multiplier, long divisor) {
    if (bound == null) {
      return null;
    }
    long value = (Long) bound;
    return divisor > 1 ? Math.floorDiv(value, divisor) : Math.multiplyExact(value, multiplier);
  }

  private Optional<Map<Integer, RangeSet>> analyzePredicateRanges(Predicate predicate) {
    if (predicate instanceof And) {
      Optional<Map<Integer, RangeSet>> left = analyzePredicateRanges(((And) predicate).left());
      Optional<Map<Integer, RangeSet>> right = analyzePredicateRanges(((And) predicate).right());
      if (left.isPresent() && right.isPresent()) {
        return Optional.of(combineRanges(left.get(), right.get(), allFragmentIds, true));
      }
      return left.isPresent() ? left : right;
    }
    if (predicate instanceof Or) {
      Optional<Map<Integer, RangeSet>> left = analyzePredicateRanges(((Or) predicate).left());
      Optional<Map<Integer, RangeSet>> right = analyzePredicateRanges(((Or) predicate).right());
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
        return comparisonRanges(children, ComparisonType.EQUALS);
      case "<":
        return comparisonRanges(children, ComparisonType.LESS_THAN);
      case "<=":
        return comparisonRanges(children, ComparisonType.LESS_THAN_OR_EQUAL);
      case ">":
        return comparisonRanges(children, ComparisonType.GREATER_THAN);
      case ">=":
        return comparisonRanges(children, ComparisonType.GREATER_THAN_OR_EQUAL);
      case "IN":
        return inRanges(children);
      case "IS_NULL":
        return nullRanges(children, true);
      case "IS_NOT_NULL":
        return nullRanges(children, false);
      default:
        return Optional.empty();
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<Map<Integer, RangeSet>> comparisonRanges(
      Expression[] children, ComparisonType type) {
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
        zone -> zoneMatchesComparison(zone, target, type));
  }

  private Optional<Map<Integer, RangeSet>> inRanges(Expression[] children) {
    if (children.length < 1 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    List<Object> values = new ArrayList<>(children.length - 1);
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        // Dropping an unknown IN value would exclude rows that match only that value.
        return Optional.empty();
      }
      values.add(normalizeLiteral(((Literal<?>) children[i]).value()));
    }
    // Sort once so each zone needs one binary search, even for large runtime IN filters.
    boolean hasNull = values.removeIf(value -> value == null);
    try {
      values.sort(ZonemapFragmentPruner::compare);
    } catch (ClassCastException e) {
      LOG.debug(
          "Cannot order IN values for zonemap pruning, skipping pruning on {}",
          columnName((NamedReference) children[0]),
          e);
      return Optional.empty();
    }
    return matchingRanges(
        columnName((NamedReference) children[0]),
        zone -> (hasNull && zone.getNullCount() > 0) || zoneContainsAny(zone, values));
  }

  /** Whether a non-null sorted value may lie within the zone's bounds. */
  private static boolean zoneContainsAny(ZoneStats zone, List<Object> sortedValues) {
    if (sortedValues.isEmpty()) {
      return false;
    }
    Object min = normalizeLiteral(zone.getMin());
    Object max = normalizeLiteral(zone.getMax());
    // Missing bounds may describe a type whose bounds Lance does not report (e.g. decimals).
    if (min == null || max == null) {
      return zone.getNullCount() < zone.getZoneLength();
    }
    // Find the first value not below min. As with comparisons, matchingRanges handles type
    // mismatches by falling back for the whole fragment.
    int low = 0;
    int high = sortedValues.size();
    while (low < high) {
      int mid = (low + high) >>> 1;
      if (compare(sortedValues.get(mid), min) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low < sortedValues.size() && compare(sortedValues.get(low), max) <= 0;
  }

  private Optional<Map<Integer, RangeSet>> nullRanges(Expression[] children, boolean isNull) {
    if (children.length != 1 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    // A zone's physical length can include gaps from deletions. For IS NOT NULL, a length
    // above the null count means the zone could contain a live non-null row.
    return matchingRanges(
        columnName((NamedReference) children[0]),
        zone -> isNull ? zone.getNullCount() > 0 : zone.getNullCount() < zone.getZoneLength());
  }

  private Optional<Map<Integer, RangeSet>> matchingRanges(String column, ZoneMatcher matcher) {
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
        // This column cannot rule out the fragment. Other conjuncts can still narrow it.
        result.put(fragmentId, RangeSet.full());
        continue;
      }

      result.put(fragmentId, matchingRanges(column, fragmentId, fragmentStats, matcher));
    }
    return Optional.of(result);
  }

  /** Invalid or incomparable stats leave the whole fragment eligible for this predicate. */
  private RangeSet matchingRanges(
      String column, int fragmentId, List<ZoneStats> zones, ZoneMatcher matcher) {
    List<RowRange> matching = new ArrayList<>();
    for (ZoneStats zone : zones) {
      long length = zone.getZoneLength();
      long start = zone.getZoneStart();
      if (length < 0
          || zone.getNullCount() < 0
          || zone.getNullCount() > length
          || (useSlices && (start < 0 || start > Long.MAX_VALUE - length))) {
        LOG.warn(
            "Invalid zonemap statistics for column '{}', fragment={}; scanning the full fragment",
            column,
            fragmentId);
        return RangeSet.full();
      }
      try {
        if (length > 0 && matcher.matches(zone)) {
          // A single matching zone keeps the fragment. AND can then intersect fragment IDs
          // without relying on physical offsets, which older writers could misreport after deletes.
          if (!useSlices) {
            return RangeSet.full();
          }
          matching.add(new RowRange(start, start + length));
        }
      } catch (RuntimeException e) {
        LOG.warn(
            "Incompatible zonemap metadata for '{}', fragment={}; scanning the full fragment: {}",
            column,
            fragmentId,
            e.toString());
        return RangeSet.full();
      }
    }
    return RangeSet.of(matching);
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

  /** Merges adjacent or overlapping {@code [start, end)} ranges so readers visit each row once. */
  static final class RangeSet {
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

    boolean isEmpty() {
      return !full && ranges.isEmpty();
    }

    List<FragmentSplit> fragmentSplits(int fragmentId) {
      if (full) {
        return Collections.singletonList(FragmentSplit.full(fragmentId));
      }
      return ranges.stream()
          .map(range -> FragmentSplit.slice(fragmentId, range.start, range.end - range.start))
          .collect(Collectors.toList());
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

  /**
   * Compares {@code a} with {@code b}, strings by code point, which is how Lance orders zone
   * bounds. Java's {@link String#compareTo} compares UTF-16 code units, which orders a character
   * above U+FFFF, stored as a surrogate pair, below U+E000 to U+FFFF.
   */
  @SuppressWarnings("unchecked")
  private static int compare(Object a, Object b) {
    if (a instanceof String && b instanceof String) {
      return compareCodePoints((String) a, (String) b);
    }
    return ((Comparable<Object>) a).compareTo(b);
  }

  static int compareCodePoints(String a, String b) {
    int i = 0;
    int j = 0;
    while (i < a.length() && j < b.length()) {
      int x = a.codePointAt(i);
      int y = b.codePointAt(j);
      if (x != y) {
        return Integer.compare(x, y);
      }
      i += Character.charCount(x);
      j += Character.charCount(y);
    }
    return Integer.compare(a.length() - i, b.length() - j);
  }

  @SuppressWarnings("unchecked")
  private static boolean zoneMatchesComparison(
      ZoneStats zone, Comparable<Object> target, ComparisonType type) {

    Comparable<Object> min = (Comparable<Object>) normalizeLiteral(zone.getMin());
    Comparable<Object> max = (Comparable<Object>) normalizeLiteral(zone.getMax());

    // Missing bounds mean either an all-null zone, which no comparison matches, or a type whose
    // bounds Lance does not report (e.g. decimals), which may match anything.
    if (min == null || max == null) {
      return zone.getNullCount() < zone.getZoneLength();
    }

    // Let matchingRanges fall back for the whole fragment when the types cannot be compared.
    switch (type) {
      case EQUALS:
        return compare(target, min) >= 0 && compare(target, max) <= 0;
      case LESS_THAN:
        return compare(min, target) < 0;
      case LESS_THAN_OR_EQUAL:
        return compare(min, target) <= 0;
      case GREATER_THAN:
        return compare(max, target) > 0;
      case GREATER_THAN_OR_EQUAL:
        return compare(max, target) >= 0;
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
   * UTF8String} → {@code String} for the same reason. Canonicalize signed floating zeros in both
   * literals and bounds: SQL considers them equal, while {@code Double.compareTo} distinguishes
   * them.
   */
  private static Object normalizeLiteral(Object value) {
    if (value instanceof UTF8String) {
      return value.toString();
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
      return ((Number) value).longValue();
    }
    if (value instanceof Float || value instanceof Double) {
      double number = ((Number) value).doubleValue();
      return number == 0.0D ? 0.0D : number;
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
