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

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Analyzes pushed Spark filters to extract target fragment IDs from {@code _rowaddr} constraints.
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

  private static final String ROW_ADDR_COLUMN = LanceConstant.ROW_ADDRESS;

  private RowAddressFilterAnalyzer() {}

  /**
   * Extracts the set of fragment IDs that could match the given pushed filters based on {@code
   * _rowaddr} constraints. Multiple filters are treated as conjuncts (implicit AND); their fragment
   * sets are intersected.
   *
   * @param filters the pushed Spark filters
   * @return present with the set of matching fragment IDs if pruning is possible; empty if no
   *     pruning can be applied
   */
  public static Optional<Set<Integer>> extractTargetFragmentIds(Filter[] filters) {
    if (filters == null || filters.length == 0) {
      return Optional.empty();
    }

    // Multiple top-level filters are implicitly ANDed by Spark. We intersect the fragment sets
    // from each filter that provides one. If two top-level _rowaddr constraints target different
    // fragments (e.g. [EqualTo("_rowaddr", 0L), EqualTo("_rowaddr", 4294967296L)]), the
    // intersection will be empty, correctly producing zero results.
    Set<Integer> result = null;
    for (Filter filter : filters) {
      Optional<Set<Integer>> fragmentIds = analyzeFilter(filter);
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
   * Recursively analyzes a single filter to extract fragment IDs from {@code _rowaddr} constraints.
   *
   * <p>CONTRACT: when present, the returned Set is always a fresh mutable {@link HashSet} that is
   * not aliased by any other reference. Callers (including {@link #extractTargetFragmentIds} and
   * {@link #analyzeAnd}) may freely mutate it (e.g. via {@code retainAll} or {@code addAll})
   * without affecting other results.
   *
   * @param filter the filter to analyze
   * @return present with the set of matching fragment IDs; empty if no pruning can be derived
   */
  private static Optional<Set<Integer>> analyzeFilter(Filter filter) {
    if (filter instanceof EqualTo) {
      return analyzeEqualTo((EqualTo) filter);
    } else if (filter instanceof In) {
      return analyzeIn((In) filter);
    } else if (filter instanceof And) {
      return analyzeAnd((And) filter);
    } else if (filter instanceof Or) {
      return analyzeOr((Or) filter);
    } else if (filter instanceof Not) {
      // Cannot safely prune for NOT filters — any fragment might match the negation.
      return Optional.empty();
    } else {
      // TODO: Range filters (GreaterThan, LessThan, etc.) on _rowaddr could compute the
      //  range of fragment IDs covered and prune accordingly (e.g. _rowaddr >= X AND
      //  _rowaddr < Y). We conservatively skip pruning for these types.
      return Optional.empty();
    }
  }

  private static Optional<Set<Integer>> analyzeEqualTo(EqualTo filter) {
    // Case-sensitive comparison: Spark resolves column names against the schema before pushing
    // filters, so filter.attribute() matches the schema's original column name ("_rowaddr").
    // If a user writes uppercase (e.g. _ROWADDR), Spark normalizes it before pushdown.
    if (!ROW_ADDR_COLUMN.equals(filter.attribute())) {
      return Optional.empty();
    }
    Optional<Long> rowAddr = toLong(filter.value());
    if (!rowAddr.isPresent()) {
      return Optional.empty();
    }
    int fragmentId = FragmentAwareJoinUtils.extractFragmentId(rowAddr.get());
    Set<Integer> result = new HashSet<>();
    result.add(fragmentId);
    return Optional.of(result);
  }

  private static Optional<Set<Integer>> analyzeIn(In filter) {
    if (!ROW_ADDR_COLUMN.equals(filter.attribute())) {
      return Optional.empty();
    }
    Set<Integer> fragmentIds = new HashSet<>();
    // An empty values array yields Optional.of(emptySet), meaning zero fragments can match —
    // correct because IN([]) is unsatisfiable. This differs from Optional.empty() which means
    // "no pruning info available, pass all fragments through". Spark is unlikely to push
    // IN([]), but we handle it defensively.
    for (Object value : filter.values()) {
      Optional<Long> rowAddr = toLong(value);
      if (!rowAddr.isPresent()) {
        // Non-numeric value in _rowaddr IN list — unexpected for a bigint column, skip pruning
        return Optional.empty();
      }
      fragmentIds.add(FragmentAwareJoinUtils.extractFragmentId(rowAddr.get()));
    }
    return Optional.of(fragmentIds);
  }

  private static Optional<Set<Integer>> analyzeAnd(And filter) {
    Optional<Set<Integer>> left = analyzeFilter(filter.left());
    Optional<Set<Integer>> right = analyzeFilter(filter.right());

    if (left.isPresent() && right.isPresent()) {
      // Intersect both sides (new HashSet — safe from aliasing). If the result is empty
      // (e.g. _rowaddr = 0 AND _rowaddr = 1<<32), that's intentional — no fragment can satisfy
      // contradictory constraints, yielding an empty scan with zero rows.
      Set<Integer> intersection = new HashSet<>(left.get());
      intersection.retainAll(right.get());
      return Optional.of(intersection);
    }
    // Only one side constrains _rowaddr — return that side's set directly.
    // This is safe because the non-_rowaddr side may reduce rows further within those
    // fragments, but cannot expand to other fragments (AND can only narrow).
    // This includes cases like NOT(condition) AND _rowaddr = 0: the Not returns empty()
    // (no pruning info), so the _rowaddr side's constraint is used — which is correct
    // because in an AND, the Not side can only further restrict rows within the fragments
    // already selected by the _rowaddr constraint; it cannot cause matches in additional
    // fragments.
    // No copy needed: the CONTRACT on analyzeFilter guarantees a fresh mutable HashSet.
    if (left.isPresent()) {
      return left;
    }
    if (right.isPresent()) {
      return right;
    }
    return Optional.empty();
  }

  private static Optional<Set<Integer>> analyzeOr(Or filter) {
    Optional<Set<Integer>> left = analyzeFilter(filter.left());
    Optional<Set<Integer>> right = analyzeFilter(filter.right());

    // For OR, both sides must constrain _rowaddr to allow pruning.
    // If either side is unconstrained, any fragment could match.
    if (left.isPresent() && right.isPresent()) {
      Set<Integer> union = new HashSet<>(left.get());
      union.addAll(right.get());
      return Optional.of(union);
    }
    return Optional.empty();
  }

  /**
   * Safely converts a filter value to long. Only accepts integral types (Long, Integer, Short,
   * Byte) to avoid silent truncation of floating-point values. Returns empty for any other type
   * (Float, Double, BigDecimal, BigInteger, String, etc.), falling back to no-pruning rather than
   * crashing the query. BigInteger values that fit in a long are also rejected for simplicity —
   * Spark does not push BigInteger filter values for bigint columns.
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
    // All other types (Float, Double, BigDecimal, BigInteger, String, etc.) → no pruning.
    // We prefer false negatives (no pruning) over incorrect pruning from truncation.
    LOG.warn(
        "Unsupported _rowaddr value type for fragment pruning: {}",
        value != null ? value.getClass().getSimpleName() : "null");
    return Optional.empty();
  }
}
