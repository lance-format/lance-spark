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

import org.apache.arrow.vector.complex.StructVector;

/**
 * Accessor for Arrow StructVector that wraps child vectors in LanceArrowColumnVector. This ensures
 * that nested fields within structs (including arrays) are properly handled by Lance-specific
 * accessors.
 */
public class LanceStructAccessor extends ArrowColumnVector.ArrowVectorAccessor {

  private final StructVector accessor;
  private final LanceArrowColumnVector[] childColumns;

  public LanceStructAccessor(StructVector vector) {
    super(vector);
    this.accessor = vector;

    // Create LanceArrowColumnVector wrappers for all child vectors
    int numChildren = vector.size();
    this.childColumns = new LanceArrowColumnVector[numChildren];
    for (int i = 0; i < numChildren; i++) {
      childColumns[i] = new LanceArrowColumnVector(vector.getChildByOrdinal(i));
    }
  }

  /**
   * Returns the child column vector at the given ordinal.
   *
   * @param ordinal the index of the child column
   * @return the child column wrapped in LanceArrowColumnVector
   */
  public ColumnVector getChild(int ordinal) {
    return childColumns[ordinal];
  }
}
