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
package org.lance.spark.vectorized;

import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.spark.sql.vectorized.ColumnarArray;

/**
 * Accessor for LargeListVector (64-bit offset lists) that wraps element vectors in
 * LanceArrowColumnVector. This ensures that elements are properly handled by Lance-specific
 * accessors.
 */
public class LanceLargeArrayAccessor {

  private final LargeListVector accessor;
  private final LanceArrowColumnVector arrayData;

  public LanceLargeArrayAccessor(LargeListVector vector) {
    this.accessor = vector;
    this.arrayData = new LanceArrowColumnVector(vector.getDataVector());
  }

  public boolean isNullAt(int rowId) {
    return this.accessor.isNull(rowId);
  }

  public int getNullCount() {
    return this.accessor.getNullCount();
  }

  public ColumnarArray getArray(int rowId) {
    long start = accessor.getElementStartIndex(rowId);
    long end = accessor.getElementEndIndex(rowId);
    return new ColumnarArray(
        arrayData, toSparkArrayInt(start, "offset"), toSparkArrayInt(end - start, "length"));
  }

  static int toSparkArrayInt(long value, String description) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
          String.format(
              "LargeList %s %,d cannot be represented by Spark's 32-bit array indexing",
              description, value));
    }
    return (int) value;
  }

  public void close() {
    this.accessor.close();
  }
}
