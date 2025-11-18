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

import com.lancedb.lance.spark.utils.BlobUtils;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;

public class LanceArrowColumnVector extends ColumnVector {
  private UInt8Accessor uInt8Accessor;
  private FixedSizeListAccessor fixedSizeListAccessor;
  private BlobStructAccessor blobStructAccessor;
  private LanceArrayAccessor arrayAccessor;
  private ArrowColumnVector arrowColumnVector;

  public LanceArrowColumnVector(ValueVector vector) {
    super(LanceArrowUtils.fromArrowField(vector.getField()));

    if (vector instanceof UInt8Vector) {
      uInt8Accessor = new UInt8Accessor((UInt8Vector) vector);
    } else if (vector instanceof FixedSizeListVector) {
      // Handle FixedSizeListVector with custom accessor
      fixedSizeListAccessor = new FixedSizeListAccessor((FixedSizeListVector) vector);
    } else if (vector instanceof StructVector && BlobUtils.isBlobArrowField(vector.getField())) {
      // Handle blob structs with special accessor to avoid unsigned Int64 issues
      // Creating BlobStructAccessor for blob field
      blobStructAccessor = new BlobStructAccessor((StructVector) vector);
    } else if (vector instanceof ListVector) {
      arrayAccessor = new LanceArrayAccessor((ListVector) vector);
    } else {
      arrowColumnVector = new ArrowColumnVector(vector);
    }
  }

  @Override
  public void close() {
    if (uInt8Accessor != null) {
      uInt8Accessor.close();
    }
    if (fixedSizeListAccessor != null) {
      fixedSizeListAccessor.close();
    }
    if (blobStructAccessor != null) {
      blobStructAccessor.close();
    }
    if (arrayAccessor != null) {
      arrayAccessor.close();
    }
    if (arrowColumnVector != null) {
      arrowColumnVector.close();
    }
  }

  @Override
  public boolean hasNull() {
    if (uInt8Accessor != null) {
      return uInt8Accessor.getNullCount() > 0;
    }
    if (fixedSizeListAccessor != null) {
      return fixedSizeListAccessor.getNullCount() > 0;
    }
    if (blobStructAccessor != null) {
      return blobStructAccessor.getNullCount() > 0;
    }
    if (arrayAccessor != null) {
      return arrayAccessor.getNullCount() > 0;
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.hasNull();
    }
    return false;
  }

  @Override
  public int numNulls() {
    if (uInt8Accessor != null) {
      return uInt8Accessor.getNullCount();
    }
    if (fixedSizeListAccessor != null) {
      return fixedSizeListAccessor.getNullCount();
    }
    if (blobStructAccessor != null) {
      return blobStructAccessor.getNullCount();
    }
    if (arrayAccessor != null) {
      return arrayAccessor.getNullCount();
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.numNulls();
    }
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (uInt8Accessor != null) {
      return uInt8Accessor.isNullAt(rowId);
    }
    if (fixedSizeListAccessor != null) {
      return fixedSizeListAccessor.isNullAt(rowId);
    }
    if (blobStructAccessor != null) {
      return blobStructAccessor.isNullAt(rowId);
    }
    if (arrayAccessor != null) {
      return arrayAccessor.isNullAt(rowId);
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.isNullAt(rowId);
    }
    return false;
  }

  @Override
  public boolean getBoolean(int rowId) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getBoolean(rowId);
    }
    return false;
  }

  @Override
  public byte getByte(int rowId) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getByte(rowId);
    }
    return 0;
  }

  @Override
  public short getShort(int rowId) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getShort(rowId);
    }
    return 0;
  }

  @Override
  public int getInt(int rowId) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getInt(rowId);
    }
    return 0;
  }

  @Override
  public long getLong(int rowId) {
    if (uInt8Accessor != null) {
      return uInt8Accessor.getLong(rowId);
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.getLong(rowId);
    }
    return 0L;
  }

  @Override
  public float getFloat(int rowId) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getFloat(rowId);
    }
    return 0;
  }

  @Override
  public double getDouble(int rowId) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getDouble(rowId);
    }
    return 0;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (fixedSizeListAccessor != null) {
      return fixedSizeListAccessor.getArray(rowId);
    }
    if (arrayAccessor != null) {
      return arrayAccessor.getArray(rowId);
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.getArray(rowId);
    }
    return null;
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getMap(ordinal);
    }
    return null;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getDecimal(rowId, precision, scale);
    }
    return null;
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getUTF8String(rowId);
    }
    return null;
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (blobStructAccessor != null) {
      // For blob columns, we return empty byte array as we don't materialize the data
      // The actual blob data can be fetched using the position and size from the struct
      // Return empty array as we don't materialize blob data
      return new byte[0];
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.getBinary(rowId);
    }
    return new byte[0];
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (arrowColumnVector != null) {
      return arrowColumnVector.getChild(ordinal);
    }
    // For blob structs, create child vectors on demand
    // This is a simplified implementation - in production would cache these
    return null;
  }

  /**
   * Returns the blob struct accessor if this column is a blob column.
   *
   * @return BlobStructAccessor or null if not a blob column
   */
  public BlobStructAccessor getBlobStructAccessor() {
    return blobStructAccessor;
  }
}
