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

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.ShiftRight;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

/**
 * Utilities for fragment-aware join optimization.
 *
 * <p>Lance stores data in fragments, and row addresses encode the fragment ID in the upper 32 bits:
 * {@code row_address = (fragment_id << 32) | row_index}
 *
 * <p>This class provides utilities to extract fragment IDs from row addresses for fragment-based
 * joins.
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
   * Check if a column name represents a row address or row ID metadata column.
   *
   * @param columnName the column name to check
   * @return true if the column is a row address or row ID column
   */
  public static boolean isRowAddressOrIdColumn(String columnName) {
    return isRowAddressColumn(columnName) || isRowIdColumn(columnName);
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
