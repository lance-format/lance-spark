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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ZonemapFragmentPruner.PartitionInfo} covering the multi-column refactor.
 */
public class PartitionInfoTest {

  private static Map<Integer, Comparable<?>[]> tuples(Object[]... entries) {
    Map<Integer, Comparable<?>[]> out = new HashMap<>();
    for (int i = 0; i < entries.length; i++) {
      Comparable<?>[] tuple = new Comparable<?>[entries[i].length];
      for (int j = 0; j < entries[i].length; j++) {
        tuple[j] = (Comparable<?>) entries[i][j];
      }
      out.put(i, tuple);
    }
    return out;
  }

  private static final List<DataType> STRING_LONG =
      Arrays.asList(DataTypes.StringType, DataTypes.LongType);

  private static final List<DataType> STRING_ONLY = Collections.singletonList(DataTypes.StringType);

  @Test
  public void rejectsEmptyColumnNames() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ZonemapFragmentPruner.PartitionInfo(
                Collections.emptyList(), Collections.emptyList(), new HashMap<>()));
  }

  @Test
  public void rejectsDuplicateColumnNames() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ZonemapFragmentPruner.PartitionInfo(
                Arrays.asList("a", "a"),
                Arrays.asList(DataTypes.StringType, DataTypes.StringType),
                tuples(new Object[] {"x", "y"})));
  }

  @Test
  public void rejectsTupleWidthMismatch() {
    Map<Integer, Comparable<?>[]> bad = new HashMap<>();
    bad.put(0, new Comparable<?>[] {"x"}); // expects width 2
    assertThrows(
        IllegalArgumentException.class,
        () -> new ZonemapFragmentPruner.PartitionInfo(Arrays.asList("a", "b"), STRING_LONG, bad));
  }

  @Test
  public void rejectsColumnTypesSizeMismatch() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ZonemapFragmentPruner.PartitionInfo(
                Arrays.asList("a", "b"),
                Collections.singletonList(DataTypes.StringType),
                tuples(new Object[] {"x", 1L})));
  }

  @Test
  public void constructorDefensivelyCopiesTuples() {
    Comparable<?>[] tuple = new Comparable<?>[] {"east", 2024L};
    Map<Integer, Comparable<?>[]> input = new HashMap<>();
    input.put(0, tuple);
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(
            Arrays.asList("region", "year"), STRING_LONG, input);
    tuple[0] = "west"; // mutate caller's array
    assertEquals("east", info.getFragmentPartitionKeys().get(0)[0]);
  }

  @Test
  public void getFragmentPartitionKeysIsUnmodifiable() {
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(
            Collections.singletonList("region"), STRING_ONLY, tuples(new Object[] {"east"}));
    assertThrows(
        UnsupportedOperationException.class,
        () -> info.getFragmentPartitionKeys().put(1, new Comparable<?>[] {"west"}));
  }

  @Test
  public void partitionKeyForFragmentMultiColumn() {
    Map<Integer, Comparable<?>[]> map = new HashMap<>();
    map.put(7, new Comparable<?>[] {"us", 2024L});
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(Arrays.asList("region", "year"), STRING_LONG, map);

    InternalRow row = info.partitionKeyForFragment(7);
    assertEquals(2, row.numFields());
    assertEquals(UTF8String.fromString("us"), row.get(0, DataTypes.StringType));
    assertEquals(2024L, row.getLong(1));
  }

  @Test
  public void partitionKeyForMissingFragmentReturnsNullRow() {
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(
            Arrays.asList("a", "b"), STRING_LONG, tuples(new Object[] {"x", 1L}));
    InternalRow row = info.partitionKeyForFragment(999);
    assertEquals(2, row.numFields());
    assertTrue(row.isNullAt(0));
    assertTrue(row.isNullAt(1));
  }

  @Test
  public void forSingleColumnMatchesListForm() {
    Map<Integer, Comparable<?>> scalarMap = new HashMap<>();
    scalarMap.put(0, "east");
    scalarMap.put(1, "west");
    ZonemapFragmentPruner.PartitionInfo factory =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "region", DataTypes.StringType, scalarMap);

    Map<Integer, Comparable<?>[]> listMap = new HashMap<>();
    listMap.put(0, new Comparable<?>[] {"east"});
    listMap.put(1, new Comparable<?>[] {"west"});
    ZonemapFragmentPruner.PartitionInfo direct =
        new ZonemapFragmentPruner.PartitionInfo(
            Collections.singletonList("region"), STRING_ONLY, listMap);

    assertEquals(direct.getColumnNames(), factory.getColumnNames());
    assertEquals(direct.size(), factory.size());
    // partitionKeyForFragment output must match for every fragment id.
    for (int fragId : new int[] {0, 1}) {
      InternalRow a = direct.partitionKeyForFragment(fragId);
      InternalRow b = factory.partitionKeyForFragment(fragId);
      assertEquals(a.numFields(), b.numFields());
      assertEquals(a.get(0, DataTypes.StringType), b.get(0, DataTypes.StringType));
    }
  }

  @Test
  public void restrictToSubsetsFragments() {
    Map<Integer, Comparable<?>[]> m = new HashMap<>();
    m.put(0, new Comparable<?>[] {"us", 2024L});
    m.put(1, new Comparable<?>[] {"us", 2025L});
    m.put(2, new Comparable<?>[] {"eu", 2024L});
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(Arrays.asList("region", "year"), STRING_LONG, m);
    ZonemapFragmentPruner.PartitionInfo narrowed =
        info.restrictTo(new HashSet<>(Arrays.asList(0, 2)));
    assertNotSame(info, narrowed);
    assertEquals(2, narrowed.size());
    assertEquals(3, info.size()); // original unchanged
    assertTrue(narrowed.getFragmentPartitionKeys().containsKey(0));
    assertTrue(narrowed.getFragmentPartitionKeys().containsKey(2));
    assertFalse(narrowed.getFragmentPartitionKeys().containsKey(1));
    // Column types survive restriction.
    assertEquals(STRING_LONG, narrowed.getColumnTypes());
  }

  @Test
  public void withSoftCappedCarriesFlagAndTypes() {
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(
            Collections.singletonList("a"), STRING_ONLY, tuples(new Object[] {"x"}));
    assertFalse(info.isSoftCapped());
    ZonemapFragmentPruner.PartitionInfo capped = info.withSoftCapped();
    assertTrue(capped.isSoftCapped());
    // Original untouched.
    assertFalse(info.isSoftCapped());
    // Data and types preserved.
    assertEquals(info.getColumnNames(), capped.getColumnNames());
    assertEquals(info.getColumnTypes(), capped.getColumnTypes());
    assertEquals(info.size(), capped.size());
  }

  @Test
  public void javaSerializationRoundTrip() throws Exception {
    Map<Integer, Comparable<?>[]> m = new HashMap<>();
    m.put(0, new Comparable<?>[] {"us", 2024L});
    m.put(1, new Comparable<?>[] {"eu", 2025L});
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(
            Arrays.asList("region", "year"), STRING_LONG, m, true);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(info);
    }
    ZonemapFragmentPruner.PartitionInfo restored;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      restored = (ZonemapFragmentPruner.PartitionInfo) ois.readObject();
    }

    assertEquals(Arrays.asList("region", "year"), restored.getColumnNames());
    assertEquals(STRING_LONG, restored.getColumnTypes());
    assertEquals(2, restored.size());
    assertTrue(restored.isSoftCapped());
    assertArrayEquals(new Object[] {"us", 2024L}, restored.getFragmentPartitionKeys().get(0));
  }

  @Test
  public void columnNamesAreImmutableView() {
    List<String> names = new java.util.ArrayList<>(Arrays.asList("a", "b"));
    ZonemapFragmentPruner.PartitionInfo info =
        new ZonemapFragmentPruner.PartitionInfo(names, STRING_LONG, tuples(new Object[] {"x", 1L}));
    names.add("c"); // mutate caller's list after construction
    assertEquals(Arrays.asList("a", "b"), info.getColumnNames());
    assertThrows(UnsupportedOperationException.class, () -> info.getColumnNames().add("c"));
  }

  // --- Type-aware narrowing (ZoneStats returns Long for every integral Arrow width) ---

  @Test
  public void byteColumnNarrowsLongToByte() {
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "b", DataTypes.ByteType, Collections.singletonMap(0, 5L));
    InternalRow row = info.partitionKeyForFragment(0);
    assertEquals((byte) 5, row.getByte(0));
  }

  @Test
  public void shortColumnNarrowsLongToShort() {
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "s", DataTypes.ShortType, Collections.singletonMap(0, 1234L));
    assertEquals((short) 1234, info.partitionKeyForFragment(0).getShort(0));
  }

  @Test
  public void intColumnNarrowsLongToInt() {
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "i", DataTypes.IntegerType, Collections.singletonMap(0, 100_000L));
    assertEquals(100_000, info.partitionKeyForFragment(0).getInt(0));
  }

  @Test
  public void dateColumnEncodesAsEpochDaysInt() {
    // ZoneStats returns epoch-days as Long (e.g. 19737 == 2024-01-15); Spark's InternalRow
    // for DateType holds an int. Narrow without loss of information.
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "d", DataTypes.DateType, Collections.singletonMap(0, 19737L));
    assertEquals(19737, info.partitionKeyForFragment(0).getInt(0));
  }

  @Test
  public void timestampColumnEncodesAsEpochMicrosLong() {
    // ZoneStats returns epoch-micros as Long; Spark's InternalRow for TimestampType holds long.
    long micros = 1_705_276_800_000_000L;
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "t", DataTypes.TimestampType, Collections.singletonMap(0, micros));
    assertEquals(micros, info.partitionKeyForFragment(0).getLong(0));
  }

  @Test
  public void booleanColumnPassesThrough() {
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "b", DataTypes.BooleanType, Collections.singletonMap(0, Boolean.TRUE));
    assertTrue(info.partitionKeyForFragment(0).getBoolean(0));
  }

  @Test
  public void stringColumnWrapsAsUtf8String() {
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "r", DataTypes.StringType, Collections.singletonMap(0, "east"));
    assertEquals(
        UTF8String.fromString("east"),
        info.partitionKeyForFragment(0).get(0, DataTypes.StringType));
  }

  @Test
  public void unsupportedPartitionTypeThrowsAtEncodeTime() {
    // If detection is bypassed and a non-whitelisted type reaches toSparkValue, we must fail loud
    // rather than hand Spark a slot that silently contains the wrong Java class.
    ZonemapFragmentPruner.PartitionInfo info =
        ZonemapFragmentPruner.PartitionInfo.forSingleColumn(
            "d", DataTypes.DoubleType, Collections.singletonMap(0, 1.5));
    assertThrows(IllegalArgumentException.class, () -> info.partitionKeyForFragment(0));
  }
}
