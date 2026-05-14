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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * Accessor for Arrow StructVector that wraps child vectors in LanceArrowColumnVector. This ensures
 * that nested fields within structs (including arrays) are properly handled by Lance-specific
 * accessors.
 */
public class LanceStructAccessor {

  private final StructVector accessor;
  private final LanceArrowColumnVector[] childColumns;

  public LanceStructAccessor(StructVector vector) {
    this.accessor = vector;

    // Create LanceArrowColumnVector wrappers for all child vectors
    int numChildren = vector.size();
    this.childColumns = new LanceArrowColumnVector[numChildren];
    for (int i = 0; i < numChildren; i++) {
      childColumns[i] = new LanceArrowColumnVector(vector.getChildByOrdinal(i));
    }
  }

  /**
   * Schema-aware constructor that maps Arrow children by name to {@code sparkStructType}'s field
   * order. Use this when the caller's Spark schema may be a pruned or reordered subset of the Arrow
   * vector's on-disk children — Spark's generated projection accesses children by the pruned-schema
   * ordinal, so binding by physical Arrow ordinal causes a type mismatch (see GitHub issue #499).
   */
  public LanceStructAccessor(StructVector vector, StructType sparkStructType) {
    this.accessor = vector;

    StructField[] sparkFields = sparkStructType.fields();
    this.childColumns = new LanceArrowColumnVector[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      StructField sparkField = sparkFields[i];
      FieldVector arrowChild = vector.getChild(sparkField.name(), FieldVector.class);
      if (arrowChild == null) {
        throw new IllegalArgumentException(
            "Arrow struct vector "
                + vector.getField().getName()
                + " is missing required field: "
                + sparkField.name());
      }
      childColumns[i] = new LanceArrowColumnVector(arrowChild, sparkField.dataType());
    }
  }

  public boolean isNullAt(int rowId) {
    return this.accessor.isNull(rowId);
  }

  public int getNullCount() {
    return this.accessor.getNullCount();
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

  public void close() {
    this.accessor.close();
  }
}
