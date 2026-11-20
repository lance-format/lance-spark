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

import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnarArray;

/**
 * Accessor for FixedSizeListVector that provides array access for Spark. This wraps a
 * FixedSizeListVector and exposes it as an array column.
 */
public class FixedSizeListAccessor {
  private final FixedSizeListVector vector;
  private final LanceArrowColumnVector childAccessor;
  private final int listSize;

  public FixedSizeListAccessor(FixedSizeListVector vector) {
    this(vector, new LanceArrowColumnVector(vector.getDataVector()));
  }

  /** Binds the elements through {@code elementType}, so pruned structs in them read by name. */
  public FixedSizeListAccessor(FixedSizeListVector vector, DataType elementType) {
    this(vector, new LanceArrowColumnVector(vector.getDataVector(), elementType));
  }

  private FixedSizeListAccessor(FixedSizeListVector vector, LanceArrowColumnVector childAccessor) {
    this.vector = vector;
    this.listSize = vector.getListSize();
    this.childAccessor = childAccessor;
  }

  public boolean isNullAt(int rowId) {
    return vector.isNull(rowId);
  }

  public int getNullCount() {
    return vector.getNullCount();
  }

  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }

    int start = rowId * listSize;
    return new ColumnarArray(childAccessor, start, listSize);
  }

  public void close() {
    if (childAccessor != null) {
      childAccessor.close();
    }
    vector.close();
  }
}
