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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Analyzes pushed Spark predicates against zonemap index statistics to determine which fragments
 * can be pruned.
 *
 * <p>This is analogous to partition pruning in traditional data sources: if all zones within a
 * fragment provably cannot match a predicate, that fragment is eliminated from the scan — avoiding
 * fragment opens, scan setup, and task scheduling.
 *
 * <p>Zonemap pruning is inexact (conservative): it may include fragments that ultimately contain no
 * matching rows, but it will never exclude fragments that do contain matching rows.
 *
 * <p>Multiple predicates are treated as conjuncts (implicit AND); their fragment sets are
 * intersected. For each column that has both a pushed predicate and zonemap stats, we evaluate
 * which fragments could possibly match. Multiple columns produce independent fragment sets that are
 * intersected.
 */
public final class ZonemapFragmentPruner {

  private static final Logger LOG = LoggerFactory.getLogger(ZonemapFragmentPruner.class);

  private ZonemapFragmentPruner() {}

  /**
   * Prune fragments using zonemap statistics.
   *
   * @param pushedPredicates the V2 predicates pushed down by Spark
   * @param zonemapStatsByColumn map from column name to its zonemap zone stats
   * @return present with the set of fragment IDs that might match; empty if no pruning can be
   *     derived
   */
  public static Optional<Set<Integer>> pruneFragments(
      Predicate[] pushedPredicates, Map<String, List<ZoneStats>> zonemapStatsByColumn) {

    if (pushedPredicates == null
        || pushedPredicates.length == 0
        || zonemapStatsByColumn == null
        || zonemapStatsByColumn.isEmpty()) {
      return Optional.empty();
    }

    Set<Integer> result = null;
    for (Predicate predicate : pushedPredicates) {
      Optional<Set<Integer>> fragmentIds = analyzePredicate(predicate, zonemapStatsByColumn);
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
   * Recursively analyzes a single predicate to extract fragment IDs from zonemap constraints.
   *
   * <p>CONTRACT: when present, the returned Set is always a fresh mutable {@link HashSet} that is
   * not aliased by any other reference. Callers may freely mutate it.
   */
  private static Optional<Set<Integer>> analyzePredicate(
      Predicate predicate, Map<String, List<ZoneStats>> statsByColumn) {

    if (predicate instanceof And) {
      return analyzeAnd((And) predicate, statsByColumn);
    }
    if (predicate instanceof Or) {
      return analyzeOr((Or) predicate, statsByColumn);
    }
    if (predicate instanceof Not) {
      return Optional.empty();
    }

    Expression[] children = predicate.children();
    String name = predicate.name();
    switch (name) {
      case "=":
        return analyzeComparison(children, statsByColumn, ComparisonType.EQUALS);
      case "<":
        return analyzeComparison(children, statsByColumn, ComparisonType.LESS_THAN);
      case "<=":
        return analyzeComparison(children, statsByColumn, ComparisonType.LESS_THAN_OR_EQUAL);
      case ">":
        return analyzeComparison(children, statsByColumn, ComparisonType.GREATER_THAN);
      case ">=":
        return analyzeComparison(children, statsByColumn, ComparisonType.GREATER_THAN_OR_EQUAL);
      case "IN":
        return analyzeIn(children, statsByColumn);
      case "IS_NULL":
        return analyzeIsNull(children, statsByColumn);
      case "IS_NOT_NULL":
        return analyzeIsNotNull(children, statsByColumn);
      default:
        return Optional.empty();
    }
  }

  @SuppressWarnings("unchecked")
  private static Optional<Set<Integer>> analyzeComparison(
      Expression[] children, Map<String, List<ZoneStats>> statsByColumn, ComparisonType type) {

    if (children.length != 2
        || !(children[0] instanceof NamedReference)
        || !(children[1] instanceof Literal)) {
      return Optional.empty();
    }
    String column = columnName((NamedReference) children[0]);
    Object value = normalizeLiteral(((Literal<?>) children[1]).value());

    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null || value == null) {
      return Optional.empty();
    }

    Comparable<Object> target;
    try {
      target = (Comparable<Object>) value;
    } catch (ClassCastException e) {
      LOG.warn("Cannot cast predicate value {} to Comparable for zonemap pruning", value);
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
    } catch (ClassCastException e) {
      LOG.warn("Type mismatch in zonemap comparison, skipping pruning for zone", e);
      return true;
    }
  }

  private static Optional<Set<Integer>> analyzeIn(
      Expression[] children, Map<String, List<ZoneStats>> statsByColumn) {

    if (children.length < 1 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    String column = columnName((NamedReference) children[0]);
    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null) {
      return Optional.empty();
    }

    // Hoist literal extraction out of the per-zone loop: invariant across zones.
    // Bail (no pruning) on any non-Literal child rather than silently dropping it,
    // which would shrink the IN list and risk excluding fragments that actually match.
    List<Object> normalizedValues = new ArrayList<>(children.length - 1);
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        return Optional.empty();
      }
      normalizedValues.add(normalizeLiteral(((Literal<?>) children[i]).value()));
    }

    Set<Integer> matchingFragments = new HashSet<>();
    for (ZoneStats zone : stats) {
      for (Object value : normalizedValues) {
        if (value == null) {
          if (zone.getNullCount() > 0) {
            matchingFragments.add(zone.getFragmentId());
            break;
          }
        } else {
          try {
            @SuppressWarnings("unchecked")
            Comparable<Object> target = (Comparable<Object>) value;
            if (zoneMatchesComparison(zone, target, ComparisonType.EQUALS)) {
              matchingFragments.add(zone.getFragmentId());
              break;
            }
          } catch (ClassCastException e) {
            matchingFragments.add(zone.getFragmentId());
            break;
          }
        }
      }
    }

    return Optional.of(matchingFragments);
  }

  private static Optional<Set<Integer>> analyzeIsNull(
      Expression[] children, Map<String, List<ZoneStats>> statsByColumn) {

    if (children.length != 1 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    String column = columnName((NamedReference) children[0]);
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
      Expression[] children, Map<String, List<ZoneStats>> statsByColumn) {

    if (children.length != 1 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    String column = columnName((NamedReference) children[0]);
    List<ZoneStats> stats = statsByColumn.get(column);
    if (stats == null) {
      return Optional.empty();
    }

    Set<Integer> matchingFragments = new HashSet<>();
    for (ZoneStats zone : stats) {
      // Zone has non-null rows if zoneLength exceeds nullCount.
      // Conservative: zoneLength may include gaps from deletions.
      if (zone.getNullCount() < zone.getZoneLength()) {
        matchingFragments.add(zone.getFragmentId());
      }
    }

    return Optional.of(matchingFragments);
  }

  private static Optional<Set<Integer>> analyzeAnd(
      And predicate, Map<String, List<ZoneStats>> statsByColumn) {
    Optional<Set<Integer>> left = analyzePredicate(predicate.left(), statsByColumn);
    Optional<Set<Integer>> right = analyzePredicate(predicate.right(), statsByColumn);

    if (left.isPresent() && right.isPresent()) {
      Set<Integer> intersection = new HashSet<>(left.get());
      intersection.retainAll(right.get());
      return Optional.of(intersection);
    }
    if (left.isPresent()) return left;
    if (right.isPresent()) return right;
    return Optional.empty();
  }

  private static Optional<Set<Integer>> analyzeOr(
      Or predicate, Map<String, List<ZoneStats>> statsByColumn) {
    Optional<Set<Integer>> left = analyzePredicate(predicate.left(), statsByColumn);
    Optional<Set<Integer>> right = analyzePredicate(predicate.right(), statsByColumn);

    if (left.isPresent() && right.isPresent()) {
      Set<Integer> union = new HashSet<>(left.get());
      union.addAll(right.get());
      return Optional.of(union);
    }
    return Optional.empty();
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
