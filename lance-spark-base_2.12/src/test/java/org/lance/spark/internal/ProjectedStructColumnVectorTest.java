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
package org.lance.spark.internal;

import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ProjectedStructColumnVectorTest {
  @Test
  public void shouldNotInferParentNullnessFromProjectedChildren() {
    StructType structType =
        new StructType()
            .add("token_total", DataTypes.LongType, true)
            .add("token_prompt", DataTypes.LongType, true);
    ConstantColumnVector tokenTotal = new ConstantColumnVector(2, DataTypes.LongType);
    tokenTotal.setNull();
    ConstantColumnVector tokenPrompt = new ConstantColumnVector(2, DataTypes.LongType);
    tokenPrompt.setNull();

    try (ProjectedStructColumnVector vector =
        new ProjectedStructColumnVector(structType, new ColumnVector[] {tokenTotal, tokenPrompt})) {
      assertFalse(vector.isNullAt(0));
      assertFalse(vector.hasNull());
      assertSame(tokenTotal, vector.getChild(0));
      assertSame(tokenPrompt, vector.getChild(1));
    }
  }

  @Test
  public void shouldCloseChildrenOnlyOnce() {
    StructType structType = new StructType().add("token_total", DataTypes.LongType, true);
    TrackingColumnVector child = new TrackingColumnVector(DataTypes.LongType);

    ProjectedStructColumnVector vector =
        new ProjectedStructColumnVector(structType, new ColumnVector[] {child});
    vector.close();
    vector.close();

    assertSame(child, vector.getChild(0));
    org.junit.jupiter.api.Assertions.assertEquals(1, child.closeCalls);
  }

  private static final class TrackingColumnVector extends ColumnVector {
    private int closeCalls;

    private TrackingColumnVector(org.apache.spark.sql.types.DataType dataType) {
      super(dataType);
    }

    @Override
    public void close() {
      closeCalls++;
    }

    @Override
    public boolean hasNull() {
      throw unsupported();
    }

    @Override
    public int numNulls() {
      throw unsupported();
    }

    @Override
    public boolean isNullAt(int rowId) {
      throw unsupported();
    }

    @Override
    public boolean getBoolean(int rowId) {
      throw unsupported();
    }

    @Override
    public byte getByte(int rowId) {
      throw unsupported();
    }

    @Override
    public short getShort(int rowId) {
      throw unsupported();
    }

    @Override
    public int getInt(int rowId) {
      throw unsupported();
    }

    @Override
    public long getLong(int rowId) {
      throw unsupported();
    }

    @Override
    public float getFloat(int rowId) {
      throw unsupported();
    }

    @Override
    public double getDouble(int rowId) {
      throw unsupported();
    }

    @Override
    public org.apache.spark.sql.types.Decimal getDecimal(int rowId, int precision, int scale) {
      throw unsupported();
    }

    @Override
    public org.apache.spark.unsafe.types.UTF8String getUTF8String(int rowId) {
      throw unsupported();
    }

    @Override
    public byte[] getBinary(int rowId) {
      throw unsupported();
    }

    @Override
    public org.apache.spark.sql.vectorized.ColumnarArray getArray(int rowId) {
      throw unsupported();
    }

    @Override
    public org.apache.spark.sql.vectorized.ColumnarMap getMap(int rowId) {
      throw unsupported();
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw unsupported();
    }

    private UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException("TrackingColumnVector is only used for close()");
    }
  }
}
