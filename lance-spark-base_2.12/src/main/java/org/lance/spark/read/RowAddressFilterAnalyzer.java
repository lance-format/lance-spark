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

import org.lance.spark.LanceConstant;
import org.lance.spark.join.FragmentAwareJoinUtils;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Analyzes pushed Spark predicates to extract target fragment IDs from {@code _rowaddr}
 * constraints.
 *
 * <p>Since {@code _rowaddr = (fragment_id << 32) | row_index}, we can extract the fragment ID from
 * any {@code _rowaddr} constant value and prune fragments that cannot match. This provides
 * fragment-level pruning analogous to partition pruning in traditional Spark data sources —
 * eliminating unnecessary fragment opens, scan setup, and task scheduling.
 *
 * <p>This also serves as a correctness fix for a lance-core bug where {@code Fragment.newScan()}
 * returns wrong results for {@code _rowaddr} filters on non-matching fragments (see
 * lance-format/lance#5351).
 */
public final class RowAddressFilterAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(RowAddressFilterAnalyzer.class);

  private static final Set<String> ROW_ADDR_ONLY = Collections.singleton(LanceConstant.ROW_ADDRESS);

  private static final Set<String> ROW_ADDR_AND_ROW_ID =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(LanceConstant.ROW_ADDRESS, LanceConstant.ROW_ID)));

  private RowAddressFilterAnalyzer() {}

  /**
   * Extracts the set of fragment IDs that could match the given pushed predicates based on {@code
   * _rowaddr} constraints. Multiple predicates are treated as conjuncts (implicit AND); their
   * fragment sets are intersected.
   *
   * @param predicates the pushed V2 predicates
   * @return present with the set of matching fragment IDs if pruning is possible; empty if no
   *     pruning can be applied
   */
  public static Optional<Set<Integer>> extractTargetFragmentIds(Predicate[] predicates) {
    return extractTargetFragmentIds(predicates, false);
  }

  /**
   * Same as {@link #extractTargetFragmentIds(Predicate[])}, optionally also treating {@code _rowid}
   * as a row address. Only pass {@code true} for datasets without stable row IDs, where Lance emits
   * {@code _rowid} equal to {@code _rowaddr}.
   */
  public static Optional<Set<Integer>> extractTargetFragmentIds(
      Predicate[] predicates, boolean rowIdIsRowAddress) {
    Set<String> columns = rowIdIsRowAddress ? ROW_ADDR_AND_ROW_ID : ROW_ADDR_ONLY;
    if (predicates == null || predicates.length == 0) {
      return Optional.empty();
    }

    Set<Integer> result = null;
    for (Predicate predicate : predicates) {
      Optional<Set<Integer>> fragmentIds = analyzePredicate(predicate, columns);
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
   * Recursively analyzes a single predicate to extract fragment IDs from {@code _rowaddr}
   * constraints.
   *
   * <p>CONTRACT: when present, the returned Set is always a fresh mutable {@link HashSet} that is
   * not aliased by any other reference. Callers may freely mutate it (e.g. via {@code retainAll} or
   * {@code addAll}) without affecting other results.
   */
  private static Optional<Set<Integer>> analyzePredicate(Predicate predicate, Set<String> columns) {
    if (predicate instanceof And) {
      return analyzeAnd((And) predicate, columns);
    }
    if (predicate instanceof Or) {
      return analyzeOr((Or) predicate, columns);
    }
    if (predicate instanceof Not) {
      // Cannot safely prune for NOT predicates — any fragment might match the negation.
      return Optional.empty();
    }
    switch (predicate.name()) {
      case "=":
        return analyzeEqualTo(predicate, columns);
      case "IN":
        return analyzeIn(predicate, columns);
      default:
        // Range predicates on _rowaddr could in principle compute the range of fragment IDs
        // covered (e.g. _rowaddr >= X AND _rowaddr < Y). Conservatively skipped for now.
        return Optional.empty();
    }
  }

  private static Optional<Set<Integer>> analyzeEqualTo(Predicate predicate, Set<String> columns) {
    Expression[] children = predicate.children();
    if (children.length != 2
        || !(children[0] instanceof NamedReference)
        || !(children[1] instanceof Literal)) {
      return Optional.empty();
    }
    if (!columns.contains(columnName((NamedReference) children[0]))) {
      return Optional.empty();
    }
    Optional<Long> rowAddr = toLong(((Literal<?>) children[1]).value());
    if (!rowAddr.isPresent()) {
      return Optional.empty();
    }
    int fragmentId = FragmentAwareJoinUtils.extractFragmentId(rowAddr.get());
    Set<Integer> result = new HashSet<>();
    result.add(fragmentId);
    return Optional.of(result);
  }

  private static Optional<Set<Integer>> analyzeIn(Predicate predicate, Set<String> columns) {
    Expression[] children = predicate.children();
    if (children.length == 0 || !(children[0] instanceof NamedReference)) {
      return Optional.empty();
    }
    if (!columns.contains(columnName((NamedReference) children[0]))) {
      return Optional.empty();
    }
    Set<Integer> fragmentIds = new HashSet<>();
    // Empty values list → Optional.of(emptySet) means "no fragment can match" (IN([]) is
    // unsatisfiable). Optional.empty() means "no pruning info available". Spark is unlikely
    // to push IN([]), but we handle it defensively.
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        return Optional.empty();
      }
      Optional<Long> rowAddr = toLong(((Literal<?>) children[i]).value());
      if (!rowAddr.isPresent()) {
        return Optional.empty();
      }
      fragmentIds.add(FragmentAwareJoinUtils.extractFragmentId(rowAddr.get()));
    }
    return Optional.of(fragmentIds);
  }

  private static Optional<Set<Integer>> analyzeAnd(And predicate, Set<String> columns) {
    Optional<Set<Integer>> left = analyzePredicate(predicate.left(), columns);
    Optional<Set<Integer>> right = analyzePredicate(predicate.right(), columns);

    if (left.isPresent() && right.isPresent()) {
      Set<Integer> intersection = new HashSet<>(left.get());
      intersection.retainAll(right.get());
      return Optional.of(intersection);
    }
    if (left.isPresent()) {
      return left;
    }
    if (right.isPresent()) {
      return right;
    }
    return Optional.empty();
  }

  private static Optional<Set<Integer>> analyzeOr(Or predicate, Set<String> columns) {
    Optional<Set<Integer>> left = analyzePredicate(predicate.left(), columns);
    Optional<Set<Integer>> right = analyzePredicate(predicate.right(), columns);

    // For OR, both sides must constrain _rowaddr to allow pruning — otherwise any fragment
    // could match via the unconstrained side.
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
   * Safely converts a predicate literal value to long. Only accepts integral types (Long, Integer,
   * Short, Byte) to avoid silent truncation of floating-point values. Returns empty for any other
   * type (Float, Double, BigDecimal, BigInteger, String, etc.), falling back to no-pruning rather
   * than crashing the query.
   */
  private static Optional<Long> toLong(Object value) {
    if (value instanceof Long) {
      return Optional.of((Long) value);
    } else if (value instanceof Integer) {
      return Optional.of(((Integer) value).longValue());
    } else if (value instanceof Short) {
      return Optional.of(((Short) value).longValue());
    } else if (value instanceof Byte) {
      return Optional.of(((Byte) value).longValue());
    }
    LOG.warn(
        "Unsupported _rowaddr value type for fragment pruning: {}",
        value != null ? value.getClass().getSimpleName() : "null");
    return Optional.empty();
  }
}
