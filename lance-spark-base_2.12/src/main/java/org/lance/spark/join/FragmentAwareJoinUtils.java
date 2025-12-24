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

import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.LanceDatasetAdapter;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.ShiftRight;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for fragment-aware join optimization.
 *
 * <p>Lance stores data in fragments, and row addresses encode the fragment ID in the upper 32 bits:
 * {@code row_address = (fragment_id << 32) | row_index}
 *
 * <p>This class provides utilities to extract fragment IDs from row addresses and build mappings
 * for efficient fragment-based joins.
 */
public class FragmentAwareJoinUtils implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Extract fragment ID from a row address. Fragment ID is stored in the upper 32 bits of the row
   * address.
   *
   * @param rowAddress the row address value
   * @return the fragment ID
   */
  public static int extractFragmentId(long rowAddress) {
    return (int) (rowAddress >>> 32);
  }

  /**
   * Extract row index from a row address. Row index is stored in the lower 32 bits of the row
   * address.
   *
   * @param rowAddress the row address value
   * @return the row index within the fragment
   */
  public static int extractRowIndex(long rowAddress) {
    return (int) (rowAddress & 0xFFFFFFFFL);
  }

  /**
   * Create a Spark SQL expression to extract fragment ID from a row address column.
   *
   * <p>This generates an expression equivalent to: {@code rowaddr >>> 32}
   *
   * @param rowAddrExpr the expression representing the row address column
   * @return an expression that extracts the fragment ID
   */
  public static Expression createFragmentIdExtractor(Expression rowAddrExpr) {
    // rowaddr >>> 32 (logical right shift)
    return new ShiftRight(rowAddrExpr, Literal.create(32, DataTypes.IntegerType));
  }

  /**
   * Build a mapping of fragment IDs to their sizes for a Lance dataset.
   *
   * <p>This can be used to determine the distribution of data across fragments and optimize
   * partition planning.
   *
   * @param options the Lance read options
   * @return a map from fragment ID to the number of rows in that fragment
   */
  public static Map<Integer, Long> buildFragmentSizeMap(LanceSparkReadOptions options) {
    Map<Integer, Long> fragmentSizes = new HashMap<>();
    List<Integer> fragmentIds = LanceDatasetAdapter.getFragmentIds(options);

    // In a full implementation, we would query the fragment metadata
    // For now, we return an empty map as a placeholder
    for (Integer fragId : fragmentIds) {
      // TODO: Get actual fragment size from Lance metadata
      fragmentSizes.put(fragId, 0L);
    }

    return fragmentSizes;
  }

  /**
   * Represents a range of stable row IDs.
   *
   * <p>Used for mapping stable row IDs to fragment IDs when joining on _rowid instead of _rowaddr.
   */
  public static class LongRange implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long start;
    private final long end;

    public LongRange(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    public boolean contains(long value) {
      return value >= start && value <= end;
    }

    @Override
    public String toString() {
      return String.format("[%d, %d]", start, end);
    }
  }

  /**
   * Build a mapping from stable row ID ranges to fragment IDs.
   *
   * <p>This is used for fragment-aware joins when the join condition uses stable _rowid instead of
   * physical _rowaddr.
   *
   * <p>Note: This requires scanning the Lance manifest to build the row ID distribution. For large
   * tables, this mapping should be cached/broadcast.
   *
   * @param options the Lance read options
   * @return a map from row ID ranges to fragment IDs
   */
  public static Map<LongRange, Integer> buildRowIdFragmentMap(LanceSparkReadOptions options) {
    Map<LongRange, Integer> rowIdMap = new HashMap<>();
    List<Integer> fragmentIds = LanceDatasetAdapter.getFragmentIds(options);

    // TODO: Query Lance manifest to get row ID ranges for each fragment
    // For now, return an empty map as a placeholder
    // Full implementation would need to:
    // 1. Get fragment metadata from Lance
    // 2. Build ranges based on stable row ID distribution
    // 3. Handle tombstones/deletions

    return rowIdMap;
  }

  /**
   * Check if a column name represents a row address or row ID metadata column.
   *
   * @param columnName the column name to check
   * @return true if the column is a row address or row ID column
   */
  public static boolean isRowAddressOrIdColumn(String columnName) {
    return "_rowaddr".equalsIgnoreCase(columnName) || "_rowid".equalsIgnoreCase(columnName);
  }

  /**
   * Check if a column name represents a row address metadata column.
   *
   * @param columnName the column name to check
   * @return true if the column is a row address column
   */
  public static boolean isRowAddressColumn(String columnName) {
    return "_rowaddr".equalsIgnoreCase(columnName);
  }

  /**
   * Check if a column name represents a stable row ID metadata column.
   *
   * @param columnName the column name to check
   * @return true if the column is a stable row ID column
   */
  public static boolean isRowIdColumn(String columnName) {
    return "_rowid".equalsIgnoreCase(columnName);
  }
}
