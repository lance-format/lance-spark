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
import org.junit.jupiter.api.Test;

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
    Filter[] filters = new Filter[] {new EqualTo("x", 150L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testEqualToMatchesNoFragment() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Filter[] filters = new Filter[] {new EqualTo("x", 500L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testEqualToMatchesBoundary() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // Value at exact boundary between fragment 0 and 1
    Filter[] filters = new Filter[] {new EqualTo("x", 99L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testEqualToMatchesExactMin() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Filter[] filters = new Filter[] {new EqualTo("x", 100L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1), result.get());
  }

  @Test
  public void testLessThanPrunesHighFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x < 50 → only fragment 0's min (0) < 50
    Filter[] filters = new Filter[] {new LessThan("x", 50L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  @Test
  public void testLessThanOrEqualIncludesBoundary() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x <= 100 → fragment 0 (min=0 <= 100) and fragment 1 (min=100 <= 100)
    Filter[] filters = new Filter[] {new LessThanOrEqual("x", 100L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testGreaterThanPrunesLowFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x > 250 → only fragment 2's max (299) > 250
    Filter[] filters = new Filter[] {new GreaterThan("x", 250L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(2), result.get());
  }

  @Test
  public void testGreaterThanOrEqualIncludesBoundary() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x >= 199 → fragment 1 (max=199 >= 199) and fragment 2 (max=299 >= 199)
    Filter[] filters = new Filter[] {new GreaterThanOrEqual("x", 199L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(1, 2), result.get());
  }

  @Test
  public void testInWithMultipleValues() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x IN (50, 250) → fragment 0 and fragment 2
    Filter[] filters = new Filter[] {new In("x", new Object[] {50L, 250L})};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testInWithNoMatchingValues() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x IN (500, 600) → no fragments match
    Filter[] filters = new Filter[] {new In("x", new Object[] {500L, 600L})};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertTrue(result.get().isEmpty());
  }

  @Test
  public void testIsNullWithNoNulls() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // All zones have nullCount=0
    Filter[] filters = new Filter[] {new IsNull("x")};

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

    Filter[] filters = new Filter[] {new IsNull("x")};

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

    Filter[] filters = new Filter[] {new IsNotNull("x")};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testAndIntersectsFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x >= 50 AND x <= 150 → intersection of {0,1,2} and {0,1}
    Filter[] filters =
        new Filter[] {new And(new GreaterThanOrEqual("x", 50L), new LessThanOrEqual("x", 150L))};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testOrUnionsFragments() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x = 50 OR x = 250 → {0} ∪ {2}
    Filter[] filters = new Filter[] {new Or(new EqualTo("x", 50L), new EqualTo("x", 250L))};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 2), result.get());
  }

  @Test
  public void testOrWithUnconstainedSideReturnsEmpty() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x = 50 OR name = 'Alice' → no pruning (name has no zonemap)
    Filter[] filters = new Filter[] {new Or(new EqualTo("x", 50L), new EqualTo("name", "Alice"))};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNotReturnsNoPruning() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Filter[] filters = new Filter[] {new Not(new EqualTo("x", 50L))};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testNonIndexedColumnReturnsNoPruning() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // Filter on column 'y' which has no zonemap stats
    Filter[] filters = new Filter[] {new EqualTo("y", 50L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertFalse(result.isPresent());
  }

  @Test
  public void testEmptyFilters() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(new Filter[] {}, stats);
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
    Filter[] filters = new Filter[] {new EqualTo("x", 50L)};
    Optional<Set<Integer>> result =
        ZonemapFragmentPruner.pruneFragments(filters, Collections.emptyMap());
    assertFalse(result.isPresent());
  }

  @Test
  public void testMultipleTopLevelFiltersIntersect() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // Two separate top-level filters: x >= 50 and x < 150
    // First: {0,1,2}, Second: {0}
    Filter[] filters = new Filter[] {new GreaterThanOrEqual("x", 50L), new LessThan("x", 150L)};

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
    Filter[] filters = new Filter[] {new EqualTo("x", 50L), new EqualTo("y", 1200L)};

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
    Filter[] filters = new Filter[] {new EqualTo("x", 75L)};

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
    Filter[] filters = new Filter[] {new EqualTo("name", "foo")};

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
    Filter[] filters = new Filter[] {new In("x", new Object[] {null, 50L})};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0, 1), result.get());
  }

  @Test
  public void testContradictoryAndYieldsEmptySet() {
    Map<String, List<ZoneStats>> stats = threeFragmentStats("x");
    // x > 300 AND x < 0 → both constrain; intersection is empty
    Filter[] filters = new Filter[] {new And(new GreaterThan("x", 300L), new LessThan("x", 0L))};

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
    Filter[] filters =
        new Filter[] {
          new Or(
              new And(new GreaterThanOrEqual("x", 50L), new LessThanOrEqual("x", 99L)),
              new EqualTo("x", 250L))
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
    Filter[] filters = new Filter[] {new EqualTo("x", 50L)};

    Optional<Set<Integer>> result = ZonemapFragmentPruner.pruneFragments(filters, stats);
    assertTrue(result.isPresent());
    assertEquals(Set.of(0), result.get());
  }

  // --- computeFragmentPartitionValues ---

  @Test
  public void testComputePartitionValuesNullInput() {
    assertEquals(Optional.empty(), ZonemapFragmentPruner.computeFragmentPartitionValues(null));
  }

  @Test
  public void testComputePartitionValuesEmptyInput() {
    assertEquals(
        Optional.empty(),
        ZonemapFragmentPruner.computeFragmentPartitionValues(Collections.emptyList()));
  }

  @Test
  public void testComputePartitionValuesSingleFragmentSingleZone() {
    List<ZoneStats> zones = Arrays.asList(new ZoneStats(0, 0, 100, "east", "east", 0));

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertTrue(result.isPresent());
    assertEquals(1, result.get().size());
    assertEquals("east", result.get().get(0));
  }

  @Test
  public void testComputePartitionValuesMultipleFragments() {
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 100, "east", "east", 0),
            new ZoneStats(1, 0, 100, "west", "west", 0),
            new ZoneStats(2, 0, 100, "north", "north", 0));

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertTrue(result.isPresent());
    assertEquals(3, result.get().size());
    assertEquals("east", result.get().get(0));
    assertEquals("west", result.get().get(1));
    assertEquals("north", result.get().get(2));
  }

  @Test
  public void testComputePartitionValuesMultipleZonesPerFragmentSameValue() {
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 50, "east", "east", 0),
            new ZoneStats(0, 50, 50, "east", "east", 0),
            new ZoneStats(1, 0, 100, "west", "west", 0));

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertTrue(result.isPresent());
    assertEquals(2, result.get().size());
    assertEquals("east", result.get().get(0));
    assertEquals("west", result.get().get(1));
  }

  @Test
  public void testComputePartitionValuesFailsWhenMinNotEqualsMax() {
    List<ZoneStats> zones = Arrays.asList(new ZoneStats(0, 0, 100, "east", "west", 0));
    assertFalse(ZonemapFragmentPruner.computeFragmentPartitionValues(zones).isPresent());
  }

  @Test
  public void testComputePartitionValuesFailsWhenNullMinMax() {
    List<ZoneStats> zones = Arrays.asList(new ZoneStats(0, 0, 100, null, null, 100));
    assertFalse(ZonemapFragmentPruner.computeFragmentPartitionValues(zones).isPresent());
  }

  @Test
  public void testComputePartitionValuesFailsWhenMultipleValuesInSameFragment() {
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 50, "east", "east", 0),
            new ZoneStats(0, 50, 50, "west", "west", 0));
    assertFalse(ZonemapFragmentPruner.computeFragmentPartitionValues(zones).isPresent());
  }

  @Test
  public void testComputePartitionValuesWithLongValues() {
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 100, 2023L, 2023L, 0), new ZoneStats(1, 0, 100, 2024L, 2024L, 0));

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertTrue(result.isPresent());
    assertEquals(2023L, result.get().get(0));
    assertEquals(2024L, result.get().get(1));
  }

  @Test
  public void testComputePartitionValuesSameValueAcrossAllFragments() {
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 100, "acme", "acme", 0),
            new ZoneStats(1, 0, 100, "acme", "acme", 0),
            new ZoneStats(2, 0, 100, "acme", "acme", 0));

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertTrue(result.isPresent());
    assertEquals("acme", result.get().get(0));
    assertEquals("acme", result.get().get(1));
    assertEquals("acme", result.get().get(2));
  }

  @Test
  public void testComputePartitionValuesNullMin() {
    List<ZoneStats> zones = Arrays.asList(new ZoneStats(0, 0, 100, null, "east", 50));
    assertFalse(ZonemapFragmentPruner.computeFragmentPartitionValues(zones).isPresent());
  }

  @Test
  public void testComputePartitionValuesNullMax() {
    List<ZoneStats> zones = Arrays.asList(new ZoneStats(0, 0, 100, "east", null, 50));
    assertFalse(ZonemapFragmentPruner.computeFragmentPartitionValues(zones).isPresent());
  }

  // --- PartitionInfo ---

  @Test
  public void testPartitionKeyForFragmentString() {
    Map<Integer, Comparable<?>> values = new HashMap<>();
    values.put(0, "east");
    values.put(1, "west");
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo("region", values);

    InternalRow row0 = info.partitionKeyForFragment(0);
    assertNotNull(row0);
    assertEquals(
        UTF8String.fromString("east"),
        row0.get(0, org.apache.spark.sql.types.DataTypes.StringType));

    InternalRow row1 = info.partitionKeyForFragment(1);
    assertEquals(
        UTF8String.fromString("west"),
        row1.get(0, org.apache.spark.sql.types.DataTypes.StringType));
  }

  @Test
  public void testPartitionKeyForFragmentLong() {
    Map<Integer, Comparable<?>> values = new HashMap<>();
    values.put(0, 2023L);
    values.put(1, 2024L);
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo("year", values);

    InternalRow row0 = info.partitionKeyForFragment(0);
    assertEquals(2023L, row0.getLong(0));

    InternalRow row1 = info.partitionKeyForFragment(1);
    assertEquals(2024L, row1.getLong(0));
  }

  @Test
  public void testPartitionKeyForMissingFragment() {
    Map<Integer, Comparable<?>> values = new HashMap<>();
    values.put(0, "east");
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo("region", values);

    InternalRow row = info.partitionKeyForFragment(99);
    assertNotNull(row);
    assertTrue(row.isNullAt(0));
  }

  @Test
  public void testPartitionInfoIsSerializable() throws Exception {
    Map<Integer, Comparable<?>> values = new HashMap<>();
    values.put(0, "east");
    values.put(1, "west");
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo("region", values);

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
    oos.writeObject(info);
    oos.close();

    java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray());
    java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
    ZonemapFragmentPruner.PartitionInfo deserialized =
        (ZonemapFragmentPruner.PartitionInfo) ois.readObject();

    assertEquals("region", deserialized.getColumnName());
    assertEquals("east", deserialized.getFragmentPartitionValues().get(0));
    assertEquals("west", deserialized.getFragmentPartitionValues().get(1));
  }

  @Test
  public void testPartitionInfoImmutableMap() {
    Map<Integer, Comparable<?>> values = new HashMap<>();
    values.put(0, "east");
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo("region", values);

    assertThrows(
        UnsupportedOperationException.class,
        () -> info.getFragmentPartitionValues().put(1, "west"));
  }
}
