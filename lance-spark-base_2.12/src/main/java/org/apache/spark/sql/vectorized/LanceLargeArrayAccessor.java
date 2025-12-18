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
package org.apache.spark.sql.vectorized;

import org.apache.arrow.vector.complex.LargeListVector;

/**
 * Accessor for LargeListVector (64-bit offset lists) that wraps element vectors in
 * LanceArrowColumnVector. This ensures that elements are properly handled by Lance-specific
 * accessors.
 */
public class LanceLargeArrayAccessor extends ArrowColumnVector.ArrowVectorAccessor {

  private final LargeListVector accessor;
  private final LanceArrowColumnVector arrayData;

  public LanceLargeArrayAccessor(LargeListVector vector) {
    super(vector);
    this.accessor = vector;
    this.arrayData = new LanceArrowColumnVector(vector.getDataVector());
  }

  @Override
  final ColumnarArray getArray(int rowId) {
    long start = accessor.getElementStartIndex(rowId);
    long end = accessor.getElementEndIndex(rowId);
    return new ColumnarArray(arrayData, (int) start, (int) (end - start));
  }
}
