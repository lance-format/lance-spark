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
package org.lance.spark.join;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for FragmentAwareJoinUtils. */
public class FragmentAwareJoinUtilsTest {

  @Test
  public void testExtractFragmentId() {
    // Fragment ID 0, Row Index 0
    long rowAddr1 = 0L;
    assertEquals(0, FragmentAwareJoinUtils.extractFragmentId(rowAddr1));
    assertEquals(0, FragmentAwareJoinUtils.extractRowIndex(rowAddr1));

    // Fragment ID 1, Row Index 100
    long rowAddr2 = (1L << 32) | 100L;
    assertEquals(1, FragmentAwareJoinUtils.extractFragmentId(rowAddr2));
    assertEquals(100, FragmentAwareJoinUtils.extractRowIndex(rowAddr2));

    // Fragment ID 5, Row Index 999
    long rowAddr3 = (5L << 32) | 999L;
    assertEquals(5, FragmentAwareJoinUtils.extractFragmentId(rowAddr3));
    assertEquals(999, FragmentAwareJoinUtils.extractRowIndex(rowAddr3));

    // Large fragment ID and row index
    long rowAddr4 = (1000L << 32) | 0xFFFFFFFFL;
    assertEquals(1000, FragmentAwareJoinUtils.extractFragmentId(rowAddr4));
    assertEquals(0xFFFFFFFF, FragmentAwareJoinUtils.extractRowIndex(rowAddr4));
  }

  @Test
  public void testIsRowAddressColumn() {
    assertTrue(FragmentAwareJoinUtils.isRowAddressColumn("_rowaddr"));
    assertTrue(FragmentAwareJoinUtils.isRowAddressColumn("_ROWADDR"));
    assertTrue(FragmentAwareJoinUtils.isRowAddressColumn("_RowAddr"));
    assertFalse(FragmentAwareJoinUtils.isRowAddressColumn("_rowid"));
    assertFalse(FragmentAwareJoinUtils.isRowAddressColumn("rowaddr"));
    assertFalse(FragmentAwareJoinUtils.isRowAddressColumn("col1"));
  }

  @Test
  public void testIsRowIdColumn() {
    assertTrue(FragmentAwareJoinUtils.isRowIdColumn("_rowid"));
    assertTrue(FragmentAwareJoinUtils.isRowIdColumn("_ROWID"));
    assertTrue(FragmentAwareJoinUtils.isRowIdColumn("_RowId"));
    assertFalse(FragmentAwareJoinUtils.isRowIdColumn("_rowaddr"));
    assertFalse(FragmentAwareJoinUtils.isRowIdColumn("rowid"));
    assertFalse(FragmentAwareJoinUtils.isRowIdColumn("col1"));
  }

  @Test
  public void testIsRowAddressOrIdColumn() {
    assertTrue(FragmentAwareJoinUtils.isRowAddressOrIdColumn("_rowaddr"));
    assertTrue(FragmentAwareJoinUtils.isRowAddressOrIdColumn("_rowid"));
    assertTrue(FragmentAwareJoinUtils.isRowAddressOrIdColumn("_ROWADDR"));
    assertTrue(FragmentAwareJoinUtils.isRowAddressOrIdColumn("_ROWID"));
    assertFalse(FragmentAwareJoinUtils.isRowAddressOrIdColumn("rowaddr"));
    assertFalse(FragmentAwareJoinUtils.isRowAddressOrIdColumn("col1"));
  }

  @Test
  public void testLongRange() {
    FragmentAwareJoinUtils.LongRange range = new FragmentAwareJoinUtils.LongRange(100, 200);

    assertEquals(100, range.getStart());
    assertEquals(200, range.getEnd());

    assertTrue(range.contains(100));
    assertTrue(range.contains(150));
    assertTrue(range.contains(200));

    assertFalse(range.contains(99));
    assertFalse(range.contains(201));
    assertFalse(range.contains(0));
    assertFalse(range.contains(1000));

    assertEquals("[100, 200]", range.toString());
  }
}
