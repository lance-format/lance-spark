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
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class ZonemapPartitionDetectorTest {

  // --- computeFragmentPartitionValues() ---

  @Test
  public void testAreZonesPartitionableNullInput() {
    assertEquals(Optional.empty(), ZonemapFragmentPruner.computeFragmentPartitionValues(null));
  }

  @Test
  public void testAreZonesPartitionableEmptyInput() {
    assertEquals(
        Optional.empty(),
        ZonemapFragmentPruner.computeFragmentPartitionValues(Collections.emptyList()));
  }

  @Test
  public void testAreZonesPartitionableSingleFragmentSingleZone() {
    List<ZoneStats> zones = Arrays.asList(new ZoneStats(0, 0, 100, "east", "east", 0));

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertTrue(result.isPresent());
    assertEquals(1, result.get().size());
    assertEquals("east", result.get().get(0));
  }

  @Test
  public void testAreZonesPartitionableMultipleFragments() {
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
  public void testAreZonesPartitionableMultipleZonesPerFragmentSameValue() {
    // Fragment 0 has two zones, both with region="east"
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
  public void testAreZonesPartitionableFailsWhenMinNotEqualsMax() {
    List<ZoneStats> zones =
        Arrays.asList(new ZoneStats(0, 0, 100, "east", "west", 0)); // range, not partition

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertFalse(result.isPresent());
  }

  @Test
  public void testAreZonesPartitionableFailsWhenNullMinMax() {
    List<ZoneStats> zones =
        Arrays.asList(new ZoneStats(0, 0, 100, null, null, 100)); // all-null zone

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertFalse(result.isPresent());
  }

  @Test
  public void testAreZonesPartitionableFailsWhenMultipleValuesInSameFragment() {
    // Fragment 0, zone1: region="east", zone2: region="west"
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 50, "east", "east", 0),
            new ZoneStats(0, 50, 50, "west", "west", 0));

    Optional<Map<Integer, Comparable<?>>> result =
        ZonemapFragmentPruner.computeFragmentPartitionValues(zones);
    assertFalse(result.isPresent());
  }

  @Test
  public void testAreZonesPartitionableWithLongValues() {
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
  public void testAreZonesPartitionableSameValueAcrossAllFragments() {
    // All fragments have the same partition value — still valid
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
  public void testAreZonesPartitionableOneZoneNullMin() {
    List<ZoneStats> zones = Arrays.asList(new ZoneStats(0, 0, 100, null, "east", 50));
    assertFalse(ZonemapFragmentPruner.computeFragmentPartitionValues(zones).isPresent());
  }

  @Test
  public void testAreZonesPartitionableOneZoneNullMax() {
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

    // Fragment 99 doesn't exist — returns null value
    InternalRow row = info.partitionKeyForFragment(99);
    assertNotNull(row); // row exists but value is null
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
