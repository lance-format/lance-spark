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

import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class RowAddressFilterAnalyzerTest {

  @Test
  public void testEqualToFragmentZero() {
    // _rowaddr = 0 → fragment 0, row index 0
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", 0L)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testRowIdPrunesOnlyWhenItIsARowAddress() {
    Predicate[] filters = new Predicate[] {TestPredicates.in("_rowid", 4294967296L)};
    assertFalse(RowAddressFilterAnalyzer.extractTargetFragmentIds(filters).isPresent());
    assertFalse(RowAddressFilterAnalyzer.extractTargetFragmentIds(filters, false).isPresent());
    assertEquals(
        Optional.of(Set.of(1)), RowAddressFilterAnalyzer.extractTargetFragmentIds(filters, true));
  }

  @Test
  public void testEqualToFragmentOne() {
    // _rowaddr = 1L << 32 = 4294967296L → fragment 1
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", 4294967296L)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testInMultipleFragments() {
    // _rowaddr IN (0L, 8589934592L) → fragments {0, 2} (8589934592L = 2L << 32)
    Predicate[] filters = new Predicate[] {TestPredicates.in("_rowaddr", 0L, 8589934592L)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testAndWithRowAddrAndOtherFilter() {
    // _rowaddr = 0 AND name = 'Alice' → fragment 0
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.and(TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("name", "Alice"))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testOrWithTwoRowAddrFilters() {
    // _rowaddr = 0 OR _rowaddr = 4294967296L → fragments {0, 1}
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.or(
              TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("_rowaddr", 4294967296L))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testOrWithOneNonRowAddrSide() {
    // _rowaddr = 0 OR name = 'Alice' → no pruning (either side could match any fragment)
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.or(TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("name", "Alice"))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNonRowAddrFilters() {
    // Filters on other columns → no pruning
    Predicate[] filters =
        new Predicate[] {TestPredicates.eq("name", "Alice"), TestPredicates.gt("age", 30)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testRangeFilterOnRowAddr() {
    // GreaterThan on _rowaddr → no pruning (conservative)
    Predicate[] filters = new Predicate[] {TestPredicates.gt("_rowaddr", 0L)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testEmptyFilters() {
    Predicate[] filters = new Predicate[] {};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNullFilters() {
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(null);
    assertFalse(result.isPresent());
  }

  @Test
  public void testMultipleTopLevelFiltersIntersect() {
    // _rowaddr = 0L AND _rowaddr IN (0L, 4294967296L) → fragment {0} (intersection)
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.eq("_rowaddr", 0L), TestPredicates.in("_rowaddr", 0L, 4294967296L)
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testTopLevelContradictoryFiltersYieldEmptySet() {
    // Two separate top-level EqualTo filters targeting different fragments → empty set.
    // This exercises the retainAll path in the extractTargetFragmentIds loop (not analyzeAnd).
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("_rowaddr", 4294967296L)
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testNotFilter() {
    // NOT(_rowaddr = 0) → no pruning (conservative)
    Predicate[] filters = new Predicate[] {TestPredicates.not(TestPredicates.eq("_rowaddr", 0L))};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testInSameFragment() {
    // Multiple _rowaddr values in the same fragment
    // 0L and 1L are both in fragment 0
    Predicate[] filters = new Predicate[] {TestPredicates.in("_rowaddr", 0L, 1L, 2L)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testIntegerValue() {
    // Integer value (not Long) should also work — use 0 for fragment 0
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", 0)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testIntegerValueNonZeroRowIndex() {
    // Integer.MAX_VALUE = 2^31 - 1 < 2^32, so an Integer _rowaddr always lands in fragment 0
    // (upper 32 bits are 0). This test verifies the int→long widening path in toLong().
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", 1024)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testContradictoryAndYieldsEmptySet() {
    // Contradictory constraints inside a single And filter → empty set
    // This exercises the retainAll path inside analyzeAnd (both sides present).
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.and(
              TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("_rowaddr", 4294967296L))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testInWithEmptyValuesArray() {
    // Defensive: Spark never pushes IN([]), but we handle it correctly — empty set, no matches
    Predicate[] filters = new Predicate[] {TestPredicates.in("_rowaddr")};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testNonNumericValueFallsBackToNoPruning() {
    // Non-numeric _rowaddr value → no pruning (graceful fallback)
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", "not-a-number")};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testInWithNonNumericValueFallsBackToNoPruning() {
    // IN list containing a non-numeric value → no pruning (graceful fallback)
    Predicate[] filters = new Predicate[] {TestPredicates.in("_rowaddr", 0L, "bad")};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testFloatingPointValueFallsBackToNoPruning() {
    // Double _rowaddr value → no pruning (reject to avoid silent truncation)
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", 3.7)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNotCompoundOrFallsBackToNoPruning() {
    // NOT(_rowaddr = 0 OR _rowaddr = 4294967296L) → no pruning (conservative)
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.not(
              TestPredicates.or(
                  TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("_rowaddr", 4294967296L)))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNotCompoundAndFallsBackToNoPruning() {
    // NOT(_rowaddr = 0 AND name = 'Alice') → no pruning (conservative)
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.not(
              TestPredicates.and(
                  TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("name", "Alice")))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testShortValue() {
    // Short max (32767) << 2^32, so fragment ID is always 0. Exercises toLong(Short) path.
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", (short) 0)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testByteValue() {
    // Byte max (127) << 2^32, so fragment ID is always 0. Exercises toLong(Byte) path.
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", (byte) 0)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testNestedAndInsideOr() {
    // Or(And(EqualTo("_rowaddr", 0L), EqualTo("name", "Alice")), EqualTo("_rowaddr", 1<<32))
    // → {0, 1}: And yields {0}, Or unions with {1}
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.or(
              TestPredicates.and(
                  TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("name", "Alice")),
              TestPredicates.eq("_rowaddr", 4294967296L))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testNestedAndInsideAnd() {
    // And(And(EqualTo("_rowaddr", 0L), EqualTo("name", "Alice")), EqualTo("age", 30))
    // → {0}: inner And yields {0}, outer And passes through
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.and(
              TestPredicates.and(
                  TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("name", "Alice")),
              TestPredicates.eq("age", 30))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testLargeFragmentId() {
    // fragmentId = Integer.MAX_VALUE → _rowaddr = (long) Integer.MAX_VALUE << 32
    long rowAddr = (long) Integer.MAX_VALUE << 32;
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", rowAddr)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(Integer.MAX_VALUE), result.get());
  }

  @Test
  public void testNestedOrInsideOrWithNonRowAddr() {
    // Or(Or(_rowaddr=0, _rowaddr=1<<32), non_rowaddr) → no pruning
    // The inner Or yields {0,1}, but the outer Or has an unconstrained side
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.or(
              TestPredicates.or(
                  TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("_rowaddr", 4294967296L)),
              TestPredicates.eq("name", "Alice"))
        };
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }

  @Test
  public void testTopLevelMixedRowAddrAndNonRowAddrFilters() {
    // Top-level: [EqualTo("_rowaddr", 0L), EqualTo("name", "Alice")]
    // The non-_rowaddr filter is ignored; only the _rowaddr filter's set is used.
    Predicate[] filters =
        new Predicate[] {TestPredicates.eq("_rowaddr", 0L), TestPredicates.eq("name", "Alice")};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testNullValueFallsBackToNoPruning() {
    // Spark shouldn't push null literal filters, but toLong(null) falls through all
    // instanceof checks and returns Optional.empty() — correct graceful fallback.
    Predicate[] filters = new Predicate[] {TestPredicates.eq("_rowaddr", null)};
    Optional<Set<Integer>> result = RowAddressFilterAnalyzer.extractTargetFragmentIds(filters);
    assertFalse(result.isPresent());
  }
}
