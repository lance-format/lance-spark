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
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ZonemapFragmentSlicePlannerTest {

  @Test
  public void testPlansAndMergesMatchingPhysicalZones() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 0L, 9L, 0),
            new ZoneStats(0, 10, 10, 10L, 19L, 0),
            new ZoneStats(0, 20, 10, 100L, 109L, 0),
            new ZoneStats(0, 40, 10, 15L, 18L, 0)));

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {TestPredicates.lt("x", 20L)},
            stats,
            Collections.emptyMap(),
            Set.of(0));

    assertEquals(
        Arrays.asList(new FragmentSlice(0, 0, 20), new FragmentSlice(0, 40, 10)),
        plan.getFragmentSlices(0));
    assertFalse(plan.scansFullFragment(0));
  }

  @Test
  public void testPlansMultipleFragmentsAndSlices() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 0L, 9L, 0),
            new ZoneStats(0, 10, 10, 100L, 109L, 0),
            new ZoneStats(0, 20, 10, 5L, 14L, 0),
            new ZoneStats(1, 0, 10, 50L, 59L, 0),
            new ZoneStats(1, 10, 10, 0L, 9L, 0),
            new ZoneStats(2, 0, 10, 100L, 109L, 0)));

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {TestPredicates.lt("x", 20L)},
            stats,
            Collections.emptyMap(),
            Set.of(0, 1, 2));

    assertEquals(Set.of(0, 1), plan.getSurvivingFragmentIds());
    assertEquals(
        Arrays.asList(new FragmentSlice(0, 0, 10), new FragmentSlice(0, 20, 10)),
        plan.getFragmentSlices(0));
    assertEquals(
        Collections.singletonList(new FragmentSlice(1, 10, 10)), plan.getFragmentSlices(1));
    assertTrue(plan.getFragmentSlices(2).isEmpty());
  }

  @Test
  public void testAndIntersectsRangesAcrossColumns() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 20, 0L, 19L, 0), new ZoneStats(0, 20, 20, 100L, 119L, 0)));
    stats.put(
        "y",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 100L, 109L, 0),
            new ZoneStats(0, 10, 20, 0L, 19L, 0),
            new ZoneStats(0, 30, 10, 100L, 109L, 0)));

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {TestPredicates.lt("x", 50L), TestPredicates.lt("y", 50L)},
            stats,
            Collections.emptyMap(),
            Set.of(0));

    assertEquals(
        Collections.singletonList(new FragmentSlice(0, 10, 10)), plan.getFragmentSlices(0));
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

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {
              TestPredicates.or(TestPredicates.eq("x", 5L), TestPredicates.eq("x", 105L))
            },
            stats,
            Collections.emptyMap(),
            Set.of(0));

    assertEquals(
        Arrays.asList(new FragmentSlice(0, 0, 10), new FragmentSlice(0, 20, 10)),
        plan.getFragmentSlices(0));
  }

  @Test
  public void testOrWithUnsupportedSideDoesNotPlanSlices() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, 0, 10, 0L, 9L, 0)));

    Optional<ZonemapScanPlan> plan =
        ZonemapFragmentPruner.planFragmentSlices(
            new Predicate[] {
              TestPredicates.or(TestPredicates.eq("x", 5L), TestPredicates.eq("y", 5L))
            },
            stats,
            Collections.emptyMap(),
            Set.of(0));

    assertFalse(plan.isPresent());
  }

  @Test
  public void testUncoveredFragmentFallsBackToFullScan() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, 0, 10, 0L, 9L, 0)));

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {TestPredicates.eq("x", 100L)},
            stats,
            Collections.singletonMap("x", Set.of(1)),
            Set.of(0, 1));

    assertEquals(Set.of(1), plan.getSurvivingFragmentIds());
    assertTrue(plan.scansFullFragment(1));
    assertTrue(plan.getFragmentSlices(0).isEmpty());
  }

  @Test
  public void testInvalidRangeFallsBackToFullScan() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, Long.MAX_VALUE - 5, 10, 0L, 9L, 0)));

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {TestPredicates.eq("x", 5L)}, stats, Collections.emptyMap(), Set.of(0));

    assertTrue(plan.scansFullFragment(0));
    assertEquals(Set.of(0), plan.getSurvivingFragmentIds());
  }

  @Test
  public void testRangeBeyondPhysicalFragmentFallsBackToFullScan() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, 0, 11, 0L, 9L, 0)));

    ZonemapScanPlan plan =
        ZonemapFragmentPruner.planFragmentSlices(
                new Predicate[] {TestPredicates.eq("x", 5L)},
                stats,
                Collections.emptyMap(),
                Set.of(0),
                Collections.singletonMap(0, 10L))
            .orElseThrow(AssertionError::new);

    assertTrue(plan.scansFullFragment(0));
  }

  @Test
  public void testTypeMismatchFallsBackToFullScan() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, 0, 10, 0L, 9L, 0)));

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {TestPredicates.eq("x", "not-a-number")},
            stats,
            Collections.emptyMap(),
            Set.of(0));

    assertTrue(plan.scansFullFragment(0));
  }

  @Test
  public void testNoMatchingZoneRemovesFragment() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put("x", Collections.singletonList(new ZoneStats(0, 0, 10, 0L, 9L, 0)));

    ZonemapScanPlan plan =
        plan(
            new Predicate[] {TestPredicates.eq("x", 50L)},
            stats,
            Collections.emptyMap(),
            Set.of(0));

    assertTrue(plan.getSurvivingFragmentIds().isEmpty());
  }

  @Test
  public void testLanceSplitSerializesFragmentSlices() throws Exception {
    LanceSplit split =
        new LanceSplit(
            Collections.singletonList(7),
            Arrays.asList(new FragmentSlice(7, 10, 5), new FragmentSlice(7, 30, 8)));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(split);
    }

    LanceSplit restored;
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (LanceSplit) input.readObject();
    }
    assertEquals(split.getFragments(), restored.getFragments());
    assertEquals(split.getFragmentSlices(), restored.getFragmentSlices());
  }

  private static ZonemapScanPlan plan(
      Predicate[] predicates,
      Map<String, List<ZoneStats>> stats,
      Map<String, Set<Integer>> uncovered,
      Set<Integer> allFragmentIds) {
    Optional<ZonemapScanPlan> result =
        ZonemapFragmentPruner.planFragmentSlices(predicates, stats, uncovered, allFragmentIds);
    assertTrue(result.isPresent());
    return result.get();
  }
}
