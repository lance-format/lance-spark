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
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ZonemapFragmentPrunerTest {

  /**
   * Helper to build a map of zonemap stats by column name.
   *
   * <p>Creates a simple three-fragment layout:
   *
   * <ul>
   *   <li>Fragment 0: values [0, 99]
   *   <li>Fragment 1: values [100, 199]
   *   <li>Fragment 2: values [200, 299]
   * </ul>
   */
  private static Map<String, List<ZoneStats>> threeFragmentStats(String column) {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        column,
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0),
            new ZoneStats(1, 0, 100, 100L, 199L, 0),
            new ZoneStats(2, 0, 100, 200L, 299L, 0)));
    return stats;
  }

  @Test
  public void testEqualToMatchesOneFragment() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.eq("x", 150L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testEqualToMatchesNoFragment() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.eq("x", 500L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testEqualToMatchesBoundary() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // Value at exact boundary between fragment 0 and 1
    Predicate[] filters = new Predicate[] {TestPredicates.eq("x", 99L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testEqualToMatchesExactMin() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.eq("x", 100L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testLessThanPrunesHighFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x < 50 → only fragment 0's min (0) < 50
    Predicate[] filters = new Predicate[] {TestPredicates.lt("x", 50L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testLessThanOrEqualIncludesBoundary() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x <= 100 → fragment 0 (min=0 <= 100) and fragment 1 (min=100 <= 100)
    Predicate[] filters = new Predicate[] {TestPredicates.lte("x", 100L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testGreaterThanPrunesLowFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x > 250 → only fragment 2's max (299) > 250
    Predicate[] filters = new Predicate[] {TestPredicates.gt("x", 250L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(2), result.get());
  }

  @Test
  public void testGreaterThanOrEqualIncludesBoundary() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x >= 199 → fragment 1 (max=199 >= 199) and fragment 2 (max=299 >= 199)
    Predicate[] filters = new Predicate[] {TestPredicates.gte("x", 199L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1, 2), result.get());
  }

  @Test
  public void testInWithMultipleValues() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x IN (50, 250) → fragment 0 and fragment 2
    Predicate[] filters = new Predicate[] {TestPredicates.in("x", 50L, 250L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testInWithNoMatchingValues() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x IN (500, 600) → no fragments match
    Predicate[] filters = new Predicate[] {TestPredicates.in("x", 500L, 600L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testInWithNonLiteralChildBailsOut() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x IN (50, <non-Literal expression>) — 50 alone matches only fragment 0,
    // but the non-Literal child could match anything, so the pruner must not
    // narrow to {0}. It must bail out (Optional.empty()) so the scan reads all
    // fragments. Silently dropping the non-Literal would be a correctness bug.
    Expression[] children =
        new Expression[] {
          FieldReference.apply("x"), TestPredicates.literalOf(50L), FieldReference.apply("y")
        };
    Predicate[] filters = new Predicate[] {new Predicate("IN", children)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertFalse(
        result.isPresent(),
        "IN with a non-Literal child must bail out instead of pruning on the remaining literals");
  }

  @Test
  public void testIsNullWithNoNulls() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // All zones have nullCount=0
    Predicate[] filters = new Predicate[] {TestPredicates.isNull("x")};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testIsNullWithSomeNulls() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0),
            new ZoneStats(1, 0, 100, 100L, 199L, 5), // has nulls
            new ZoneStats(2, 0, 100, 200L, 299L, 0)));

    Predicate[] filters = new Predicate[] {TestPredicates.isNull("x")};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testIsNotNull() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0), // no nulls
            new ZoneStats(1, 0, 100, null, null, 100), // all nulls
            new ZoneStats(2, 0, 100, 200L, 299L, 50))); // some nulls

    Predicate[] filters = new Predicate[] {TestPredicates.isNotNull("x")};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testAndIntersectsFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x >= 50 AND x <= 150 → intersection of {0,1,2} and {0,1}
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.and(TestPredicates.gte("x", 50L), TestPredicates.lte("x", 150L))
        };

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testOrUnionsFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x = 50 OR x = 250 → {0} ∪ {2}
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.or(TestPredicates.eq("x", 50L), TestPredicates.eq("x", 250L))
        };

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testOrWithUnconstainedSideReturnsEmpty() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x = 50 OR name = 'Alice' → no pruning (name has no zonemap)
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.or(TestPredicates.eq("x", 50L), TestPredicates.eq("name", "Alice"))
        };

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNotReturnsNoPruning() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.not(TestPredicates.eq("x", 50L))};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNonIndexedColumnReturnsNoPruning() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // Filter on column 'y' which has no zonemap stats
    Predicate[] filters = new Predicate[] {TestPredicates.eq("y", 50L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testEmptyFilters() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(new Predicate[] {}, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNullFilters() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(null, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testEmptyStats() {
    Predicate[] filters = new Predicate[] {TestPredicates.eq("x", 50L)};
    Optional<Set<Integer>> result =
        ZonemapFragmentPruner.pruneFragments(filters, Collections.emptyMap());
    assertFalse(result.isPresent());
  }

  @Test
  public void testMultipleTopLevelFiltersIntersect() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // Two separate top-level filters: x >= 50 and x < 150
    // First: {0,1,2}, Second: {0}
    Predicate[] filters =
        new Predicate[] {TestPredicates.gte("x", 50L), TestPredicates.lt("x", 150L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    // x >= 50 matches {0,1,2} (all have max >= 50)
    // x < 150 matches {0,1} (min < 150 for frags 0 and 1)
    // intersection = {0,1}
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testMultipleColumnsIntersect() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    // Column x: frag0=[0,99], frag1=[100,199], frag2=[200,299]
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0),
            new ZoneStats(1, 0, 100, 100L, 199L, 0),
            new ZoneStats(2, 0, 100, 200L, 299L, 0)));
    // Column y: frag0=[1000,1099], frag1=[1100,1199], frag2=[1200,1299]
    stats.put(
        "y",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 1000L, 1099L, 0),
            new ZoneStats(1, 0, 100, 1100L, 1199L, 0),
            new ZoneStats(2, 0, 100, 1200L, 1299L, 0)));

    // x = 50 (matches frag 0) AND y = 1200 (matches frag 2)
    // intersection = empty
    Predicate[] filters =
        new Predicate[] {TestPredicates.eq("x", 50L), TestPredicates.eq("y", 1200L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testMultipleZonesPerFragment() {
    // Fragment 0 has two zones: [0,49] and [50,99]
    // Fragment 1 has one zone: [100,199]
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 50, 0L, 49L, 0),
            new ZoneStats(0, 50, 50, 50L, 99L, 0),
            new ZoneStats(1, 0, 100, 100L, 199L, 0)));

    // x = 75 → matches second zone of fragment 0 → fragment 0 survives
    Predicate[] filters = new Predicate[] {TestPredicates.eq("x", 75L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testStringColumnComparison() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "name",
        Arrays.asList(
            new ZoneStats(0, 0, 100, "aaa", "dzz", 0),
            new ZoneStats(1, 0, 100, "eaa", "hzz", 0),
            new ZoneStats(2, 0, 100, "iaa", "zzz", 0)));

    // name = 'foo' → falls in [eaa, hzz] → fragment 1
    Predicate[] filters = new Predicate[] {TestPredicates.eq("name", "foo")};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testInWithNullValue() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0),
            new ZoneStats(1, 0, 100, 100L, 199L, 5))); // fragment 1 has nulls

    // x IN (null, 50) → fragment 0 (has 50) and fragment 1 (has nulls)
    Predicate[] filters = new Predicate[] {TestPredicates.in("x", null, 50L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testContradictoryAndYieldsEmptySet() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x > 300 AND x < 0 → both constrain; intersection is empty
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.and(TestPredicates.gt("x", 300L), TestPredicates.lt("x", 0L))
        };

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testNestedAndInsideOr() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // (x >= 50 AND x <= 99) OR x = 250
    // Left AND: {0,1,2} ∩ {0} = {0}
    // Right: {2}
    // OR: {0,2}
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.or(
              TestPredicates.and(TestPredicates.gte("x", 50L), TestPredicates.lte("x", 99L)),
              TestPredicates.eq("x", 250L))
        };

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testAllNullZoneSkippedForEqualTo() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0),
            new ZoneStats(1, 0, 100, null, null, 100))); // all nulls

    // x = 50 → fragment 0 matches, fragment 1 (all null) does not
    Predicate[] filters = new Predicate[] {TestPredicates.eq("x", 50L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testIntegerLiteralAgainstLongZoneStats() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("seq");
    Predicate[] filters = new Predicate[] {TestPredicates.eq("seq", 150)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testShortLiteralAgainstLongZoneStats() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.gt("x", (short) 150)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1, 2), result.get());
  }

  @Test
  public void testByteLiteralAgainstLongZoneStats() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.lte("x", (byte) 50)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testFloatLiteralAgainstDoubleZoneStats() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "f",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0.0d, 9.9d, 0),
            new ZoneStats(1, 0, 100, 10.0d, 19.9d, 0),
            new ZoneStats(2, 0, 100, 20.0d, 29.9d, 0)));

    Predicate[] filters = new Predicate[] {TestPredicates.eq("f", 15.0f)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testDateLiteralAgainstLongZoneStats() {
    // Spark DateType literal is Integer epoch days; Date32 zone bounds are Long
    // epoch days — same Integer→Long widening as INT32.
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "d",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 19000L, 19099L, 0),
            new ZoneStats(1, 0, 100, 19100L, 19199L, 0),
            new ZoneStats(2, 0, 100, 19200L, 19299L, 0)));

    Predicate[] filters =
        new Predicate[] {TestPredicates.eq("d", Date.valueOf("2022-04-27"))}; // day 19109

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testInListWithIntegerLiteralsAgainstLongZoneStats() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.in("x", 50, 250, 999)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testInListWithMixedWidthLiteralsAgainstLongZoneStats() {
    // Per-element widening inside analyzeIn's loop.
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Predicate[] filters = new Predicate[] {TestPredicates.in("x", 50, 250L, (short) 70, (byte) 5)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }
}
