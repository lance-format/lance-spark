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

import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;

public class LanceArrowColumnVector extends ColumnVector {
  private UInt1Accessor uInt1Accessor;
  private UInt2Accessor uInt2Accessor;
  private UInt4Accessor uInt4Accessor;
  private UInt8Accessor uInt8Accessor;
  private FixedSizeListAccessor fixedSizeListAccessor;
  private BlobStructAccessor blobStructAccessor;
  private LanceArrayAccessor arrayAccessor;
  private ArrowColumnVector arrowColumnVector;

  public LanceArrowColumnVector(ValueVector vector) {
    super(LanceArrowUtils.fromArrowField(vector.getField()));

    if (vector instanceof UInt1Vector) {
      uInt1Accessor = new UInt1Accessor((UInt1Vector) vector);
    } else if (vector instanceof UInt2Vector) {
      uInt2Accessor = new UInt2Accessor((UInt2Vector) vector);
    } else if (vector instanceof UInt4Vector) {
      uInt4Accessor = new UInt4Accessor((UInt4Vector) vector);
    } else if (vector instanceof UInt8Vector) {
      uInt8Accessor = new UInt8Accessor((UInt8Vector) vector);
    } else if (vector instanceof FixedSizeListVector) {
      fixedSizeListAccessor = new FixedSizeListAccessor((FixedSizeListVector) vector);
    } else if (vector instanceof StructVector && BlobUtils.isBlobArrowField(vector.getField())) {
      blobStructAccessor = new BlobStructAccessor((StructVector) vector);
    } else if (vector instanceof ListVector) {
      arrayAccessor = new LanceArrayAccessor((ListVector) vector);
    } else {
      arrowColumnVector = new ArrowColumnVector(vector);
    }
  }

  @Override
  public void close() {
    if (uInt1Accessor != null) {
      uInt1Accessor.close();
    }
    if (uInt2Accessor != null) {
      uInt2Accessor.close();
    }
    if (uInt4Accessor != null) {
      uInt4Accessor.close();
    }
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
    if (uInt1Accessor != null) {
      return uInt1Accessor.getNullCount() > 0;
    }
    if (uInt2Accessor != null) {
      return uInt2Accessor.getNullCount() > 0;
    }
    if (uInt4Accessor != null) {
      return uInt4Accessor.getNullCount() > 0;
    }
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
    if (uInt1Accessor != null) {
      return uInt1Accessor.getNullCount();
    }
    if (uInt2Accessor != null) {
      return uInt2Accessor.getNullCount();
    }
    if (uInt4Accessor != null) {
      return uInt4Accessor.getNullCount();
    }
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
    if (uInt1Accessor != null) {
      return uInt1Accessor.isNullAt(rowId);
    }
    if (uInt2Accessor != null) {
      return uInt2Accessor.isNullAt(rowId);
    }
    if (uInt4Accessor != null) {
      return uInt4Accessor.isNullAt(rowId);
    }
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
    if (uInt1Accessor != null) {
      return uInt1Accessor.getShort(rowId);
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.getShort(rowId);
    }
    return 0;
  }

  @Override
  public int getInt(int rowId) {
    if (uInt2Accessor != null) {
      return uInt2Accessor.getInt(rowId);
    }
    if (arrowColumnVector != null) {
      return arrowColumnVector.getInt(rowId);
    }
    return 0;
  }

  @Override
  public long getLong(int rowId) {
    if (uInt4Accessor != null) {
      return uInt4Accessor.getLong(rowId);
    }
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
