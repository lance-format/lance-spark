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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

class ProjectedStructColumnVector extends ColumnVector {
  private final ColumnVector[] childColumns;
  private boolean closed;

  ProjectedStructColumnVector(StructType dataType, ColumnVector[] childColumns) {
    super(dataType);
    this.childColumns = childColumns;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (ColumnVector childColumn : childColumns) {
      if (childColumn != null) {
        childColumn.close();
      }
    }
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    // This vector is only used to provide struct child access for nested projection pushdown.
    // Projection planning only reconstructs non-nullable parent structs. Nullable structs
    // are kept as exact top-level projections so Spark still sees the real parent validity
    // bitmap from Arrow.
    return false;
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
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw unsupported();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw unsupported();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw unsupported();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw unsupported();
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw unsupported();
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return childColumns[ordinal];
  }

  private UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException(
        "ProjectedStructColumnVector only supports nested child access");
  }
}
