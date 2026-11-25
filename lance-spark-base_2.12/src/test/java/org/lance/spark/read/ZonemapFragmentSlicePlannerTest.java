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
import org.lance.spark.read.ZonemapFragmentPruner.RangeSet;

import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.lance.spark.read.ZonemapFragmentPruner.matchingFragmentRanges;

public class ZonemapFragmentSlicePlannerTest {

  @Test
  public void testStringRangesUseUtf8Ordering() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    // These are valid UTF-8 bounds but reverse Java's UTF-16 order at the upper bound.
    stats.put(
        "s",
        Arrays.asList(
            new ZoneStats(0, 0, 4, "\uE000", "\uD800\uDC00", 0),
            new ZoneStats(0, 4, 4, "\uF000", "\uF000", 0)));
    Map<Integer, RangeSet> ranges =
        matchingFragmentRanges(
                new Predicate[] {TestPredicates.in("s", "\uF000", "a")},
                stats,
                Collections.emptyMap(),
                Set.of(0),
                true)
            .orElseThrow();
    assertSplits(ranges.get(0).fragmentSplits(0), FragmentSplit.slice(0, 0, 8));
  }

  @Test
  public void testPlansMultipleFragmentsAndSlices() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 0L, 9L, 0),
            new ZoneStats(0, 10, 10, 10L, 19L, 0),
            new ZoneStats(0, 20, 10, 100L, 109L, 0),
            new ZoneStats(0, 30, 10, 5L, 14L, 0),
            new ZoneStats(1, 0, 10, 50L, 59L, 0),
            new ZoneStats(1, 10, 10, 0L, 9L, 0),
            new ZoneStats(2, 0, 10, 100L, 109L, 0)));

    Map<Integer, RangeSet> ranges =
        matchingFragmentRanges(
                new Predicate[] {TestPredicates.lt("x", 20L)},
                stats,
                Map.of("x", Set.of(3)),
                Set.of(0, 1, 2, 3),
                true)
            .orElseThrow();

    // Adjacent matches merge, but the gap at [20, 30) remains.
    assertSplits(
        ranges.get(0).fragmentSplits(0),
        FragmentSplit.slice(0, 0, 20),
        FragmentSplit.slice(0, 30, 10));
    assertSplits(ranges.get(1).fragmentSplits(1), FragmentSplit.slice(1, 10, 10));
    // Fragment 2 is ruled out by its bounds. Fragment 3 has no index coverage.
    assertTrue(ranges.get(2).isEmpty());
    assertSplits(ranges.get(3).fragmentSplits(3), FragmentSplit.full(3));
  }

  @Test
  public void testAndIntersectsRangesAcrossColumns() {
    // x matches [0, 20) and y matches [10, 30), leaving [10, 20) in their intersection.
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 20, 0L, 19L, 0), new ZoneStats(0, 20, 20, 100L, 119L, 0)));
    stats.put(
        "y",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 200L, 209L, 0),
            new ZoneStats(0, 10, 20, 0L, 19L, 0),
            new ZoneStats(0, 30, 10, 100L, 109L, 0)));

    Map<Integer, RangeSet> ranges =
        matchingFragmentRanges(
                new Predicate[] {TestPredicates.lt("x", 50L), TestPredicates.lt("y", 50L)},
                stats,
                Collections.emptyMap(),
                Set.of(0),
                true)
            .orElseThrow();

    assertSplits(ranges.get(0).fragmentSplits(0), FragmentSplit.slice(0, 10, 10));

    // Each column can match this fragment, even though these two zones do not overlap.
    Predicate[] disjoint = {TestPredicates.lt("x", 50L), TestPredicates.eq("y", 105L)};
    assertEquals(
        Set.of(0),
        ZonemapFragmentPruner.pruneFragments(disjoint, stats, Collections.emptyMap())
            .orElseThrow());
    assertTrue(
        matchingFragmentRanges(disjoint, stats, Collections.emptyMap(), Set.of(0), true)
            .orElseThrow()
            .get(0)
            .isEmpty());
  }

  @Test
  public void testOrUnionsRanges() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 0L, 9L, 0),
            new ZoneStats(0, 10, 10, 50L, 59L, 0),
            new ZoneStats(0, 20, 10, 100L, 109L, 0)));

    Map<Integer, RangeSet> ranges =
        matchingFragmentRanges(
                new Predicate[] {
                  TestPredicates.or(TestPredicates.eq("x", 5L), TestPredicates.eq("x", 105L))
                },
                stats,
                Collections.emptyMap(),
                Set.of(0),
                true)
            .orElseThrow();

    assertSplits(
        ranges.get(0).fragmentSplits(0),
        FragmentSplit.slice(0, 0, 10),
        FragmentSplit.slice(0, 20, 10));
  }

  @Test
  public void testInvalidRangeFallsBackToFullScan() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, Long.MAX_VALUE - 5, 10, 0L, 9L, 0)));

    Map<Integer, RangeSet> ranges =
        matchingFragmentRanges(
                new Predicate[] {TestPredicates.eq("x", 5L)},
                stats,
                Collections.emptyMap(),
                Set.of(0),
                true)
            .orElseThrow();

    assertSplits(ranges.get(0).fragmentSplits(0), FragmentSplit.full(0));
  }

  @Test
  public void testTypeMismatchFallsBackToFullScan() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, 0, 10, 0L, 9L, 0)));

    Map<Integer, RangeSet> ranges =
        matchingFragmentRanges(
                new Predicate[] {TestPredicates.eq("x", "not-a-number")},
                stats,
                Collections.emptyMap(),
                Set.of(0),
                true)
            .orElseThrow();

    assertSplits(ranges.get(0).fragmentSplits(0), FragmentSplit.full(0));
  }

  @Test
  public void testLanceSplitSerializesFragmentSlices() throws Exception {
    List<LanceSplit> splits =
        Arrays.asList(
            new LanceSplit(Arrays.asList(FragmentSplit.full(7), FragmentSplit.full(8))),
            new LanceSplit(
                Arrays.asList(FragmentSplit.slice(7, 10, 5), FragmentSplit.slice(7, 30, 8))));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      for (LanceSplit split : splits) {
        output.writeObject(split);
      }
    }

    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      for (LanceSplit split : splits) {
        LanceSplit restored = (LanceSplit) input.readObject();
        assertEquals(split.getFragments(), restored.getFragments());
        assertSplits(
            restored.getFragmentSplits(), split.getFragmentSplits().toArray(new FragmentSplit[0]));
        assertEquals(split.hasFragmentSlices(), restored.hasFragmentSlices());
      }
    }
    assertEquals(Arrays.asList(7, 8), splits.get(0).getFragments());
    assertFalse(splits.get(0).hasFragmentSlices());
    assertEquals(Collections.singletonList(7), splits.get(1).getFragments());
    assertThrows(
        IllegalArgumentException.class,
        () -> new LanceSplit(Arrays.asList(FragmentSplit.full(7), FragmentSplit.slice(8, 10, 5))));
  }

  private static void assertSplits(List<FragmentSplit> actual, FragmentSplit... expected) {
    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i].getFragmentId(), actual.get(i).getFragmentId());
      assertEquals(expected[i].getSlice(), actual.get(i).getSlice());
    }
  }
}
