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

import java.io.Serializable;

/**
 * Describes a row range within a single Lance fragment. Used by {@link LanceInputPartition} to
 * define precise partition boundaries that can split a fragment across partitions or combine
 * multiple fragment ranges into a single partition.
 *
 * <p>Use {@link #allRows(int)} for a range covering the entire fragment.
 */
public class FragmentRowRange implements Serializable {
  private static final long serialVersionUID = 7482937492837492837L;

  /** Sentinel value indicating all rows should be read (no limit). */
  public static final long ALL_ROWS = -1;

  private final int fragmentId;
  private final long offset;
  private final long numRows;

  /**
   * @param fragmentId the Lance fragment ID
   * @param offset row offset within the fragment (0-based)
   * @param numRows number of rows to read, or {@link #ALL_ROWS} for all remaining rows
   */
  public FragmentRowRange(int fragmentId, long offset, long numRows) {
    this.fragmentId = fragmentId;
    this.offset = offset;
    this.numRows = numRows;
  }

  /** Creates a range covering all rows in the given fragment. */
  public static FragmentRowRange allRows(int fragmentId) {
    return new FragmentRowRange(fragmentId, 0, ALL_ROWS);
  }

  public int getFragmentId() {
    return fragmentId;
  }

  public long getOffset() {
    return offset;
  }

  public long getNumRows() {
    return numRows;
  }

  /** Returns true if this range covers the entire fragment (no offset or row limit). */
  public boolean isFullFragment() {
    return offset == 0 && numRows == ALL_ROWS;
  }

  @Override
  public String toString() {
    if (isFullFragment()) {
      return "FragmentRowRange{fragment=" + fragmentId + ", all}";
    }
    return "FragmentRowRange{fragment="
        + fragmentId
        + ", offset="
        + offset
        + ", numRows="
        + numRows
        + "}";
  }
}
