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

import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link ColumnStatsAggregator}. */
class ColumnStatsAggregatorTest {

  private static ZoneStats zone(int fragId, Comparable<?> min, Comparable<?> max, long nulls) {
    return new ZoneStats(fragId, 0L, 100L, min, max, nulls);
  }

  @Test
  @DisplayName("empty input returns empty")
  void emptyInputReturnsEmpty() {
    assertFalse(ColumnStatsAggregator.aggregate(null).isPresent());
    assertFalse(ColumnStatsAggregator.aggregate(Collections.emptyList()).isPresent());
  }

  @Test
  @DisplayName("single-zone Long column reports min/max/nullCount")
  void singleZoneLongColumn() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(Arrays.asList(zone(0, 10L, 100L, 5L)));
    assertTrue(stats.isPresent());
    assertEquals(10L, stats.get().min().get());
    assertEquals(100L, stats.get().max().get());
    assertEquals(5L, stats.get().nullCount().getAsLong());
    assertFalse(stats.get().distinctCount().isPresent());
    assertFalse(stats.get().avgLen().isPresent());
    assertFalse(stats.get().histogram().isPresent());
  }

  @Test
  @DisplayName("multi-zone Long column reduces to global min/max and summed nullCount")
  void multiZoneLongColumn() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, 50L, 200L, 3L), zone(0, 10L, 150L, 0L), zone(1, 100L, 300L, 7L)));
    assertTrue(stats.isPresent());
    assertEquals(10L, stats.get().min().get());
    assertEquals(300L, stats.get().max().get());
    assertEquals(10L, stats.get().nullCount().getAsLong());
  }

  @Test
  @DisplayName("Integer column min/max")
  void integerColumn() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(Arrays.asList(zone(0, 1, 5, 0L), zone(1, -3, 8, 1L)));
    assertTrue(stats.isPresent());
    assertEquals(-3, stats.get().min().get());
    assertEquals(8, stats.get().max().get());
    assertEquals(1L, stats.get().nullCount().getAsLong());
  }

  @Test
  @DisplayName("Double column min/max")
  void doubleColumn() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, 1.5d, 2.5d, 0L), zone(1, -0.1d, 3.7d, 0L)));
    assertTrue(stats.isPresent());
    assertEquals(-0.1d, stats.get().min().get());
    assertEquals(3.7d, stats.get().max().get());
  }

  @Test
  @DisplayName("String column lex min/max")
  void stringColumn() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, "alpha", "kilo", 0L), zone(1, "beta", "tango", 2L)));
    assertTrue(stats.isPresent());
    assertEquals("alpha", stats.get().min().get());
    assertEquals("tango", stats.get().max().get());
    assertEquals(2L, stats.get().nullCount().getAsLong());
  }

  @Test
  @DisplayName("all-null zones report nullCount only, no min/max")
  void allNullZonesReportNullCountOnly() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, null, null, 50L), zone(1, null, null, 30L)));
    assertTrue(stats.isPresent());
    assertFalse(stats.get().min().isPresent());
    assertFalse(stats.get().max().isPresent());
    assertEquals(80L, stats.get().nullCount().getAsLong());
  }

  @Test
  @DisplayName("all-null zones with zero null count returns empty")
  void allNullZonesWithZeroNullCountReturnsEmpty() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, null, null, 0L), zone(1, null, null, 0L)));
    assertFalse(stats.isPresent());
  }

  @Test
  @DisplayName("mixed-null zones aggregate non-null values plus combined null count")
  void mixedNullZones() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, 10L, 20L, 4L), zone(0, null, null, 6L), zone(1, 5L, 15L, 1L)));
    assertTrue(stats.isPresent());
    assertEquals(5L, stats.get().min().get());
    assertEquals(20L, stats.get().max().get());
    assertEquals(11L, stats.get().nullCount().getAsLong());
  }

  @Test
  @DisplayName("type-inconsistent zones return empty rather than throw")
  void typeInconsistentZonesReturnEmpty() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(
                zone(0, 10L, 20L, 0L), zone(1, 5, 15, 0L))); // Integer where prior was Long
    assertFalse(stats.isPresent());
  }

  @Test
  @DisplayName("zone min and max differ in runtime class — skip column")
  void zoneInternalTypeMismatchReturnsEmpty() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(Arrays.asList(zone(0, 10L, "twenty", 0L)));
    assertFalse(stats.isPresent());
  }

  @Test
  @DisplayName("conservative NDV: every zone has min==max → distinct zone-min values count")
  void conservativeNdvEveryZoneSingleValue() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(
                zone(0, 1998L, 1998L, 0L),
                zone(1, 1999L, 1999L, 0L),
                zone(2, 2000L, 2000L, 0L),
                zone(3, 2001L, 2001L, 0L)));
    assertTrue(stats.isPresent());
    assertTrue(stats.get().distinctCount().isPresent());
    assertEquals(4L, stats.get().distinctCount().getAsLong());
  }

  @Test
  @DisplayName("conservative NDV: duplicate zone-min values dedupe")
  void conservativeNdvDuplicateZoneValuesDedupe() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(
                zone(0, 1998L, 1998L, 0L), zone(1, 1998L, 1998L, 0L), zone(2, 1999L, 1999L, 0L)));
    assertTrue(stats.isPresent());
    assertEquals(2L, stats.get().distinctCount().getAsLong());
  }

  @Test
  @DisplayName("conservative NDV: any zone with min!=max disables NDV reporting")
  void conservativeNdvDisabledWhenZoneSpansMultipleValues() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(
                zone(0, 1998L, 1998L, 0L),
                zone(1, 1999L, 2001L, 0L), // multi-value zone
                zone(2, 2002L, 2002L, 0L)));
    assertTrue(stats.isPresent());
    assertFalse(stats.get().distinctCount().isPresent());
  }

  @Test
  @DisplayName("conservative NDV: all-null zones are ignored")
  void conservativeNdvIgnoresAllNullZones() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, null, null, 50L), zone(1, 1L, 1L, 0L), zone(2, 2L, 2L, 0L)));
    assertTrue(stats.isPresent());
    assertEquals(2L, stats.get().distinctCount().getAsLong());
  }

  @Test
  @DisplayName("conservative NDV: zero distinct values (all-null) returns empty")
  void conservativeNdvAllNullReturnsEmpty() {
    Optional<ColumnStatistics> stats =
        ColumnStatsAggregator.aggregate(
            Arrays.asList(zone(0, null, null, 5L), zone(1, null, null, 7L)));
    assertTrue(stats.isPresent());
    assertFalse(stats.get().distinctCount().isPresent());
  }

  @Test
  @DisplayName("single column spans many fragments — pure reduction is order-independent")
  void manyFragmentsReductionIsCommutative() {
    List<ZoneStats> ascending =
        Arrays.asList(
            zone(0, 1L, 5L, 0L),
            zone(1, 6L, 10L, 0L),
            zone(2, 11L, 15L, 0L),
            zone(3, 16L, 20L, 0L));
    List<ZoneStats> descending =
        Arrays.asList(
            zone(3, 16L, 20L, 0L),
            zone(2, 11L, 15L, 0L),
            zone(1, 6L, 10L, 0L),
            zone(0, 1L, 5L, 0L));
    Optional<ColumnStatistics> a = ColumnStatsAggregator.aggregate(ascending);
    Optional<ColumnStatistics> b = ColumnStatsAggregator.aggregate(descending);
    assertTrue(a.isPresent() && b.isPresent());
    assertEquals(a.get().min().get(), b.get().min().get());
    assertEquals(a.get().max().get(), b.get().max().get());
    assertEquals(a.get().nullCount().getAsLong(), b.get().nullCount().getAsLong());
  }
}
