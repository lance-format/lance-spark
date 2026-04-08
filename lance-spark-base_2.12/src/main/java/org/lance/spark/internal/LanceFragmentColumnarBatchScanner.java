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

import org.lance.spark.LanceConstant;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.vectorized.BlobStructAccessor;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LanceFragmentColumnarBatchScanner implements AutoCloseable {
  private final LanceFragmentScanner fragmentScanner;
  private final ArrowReader arrowReader;
  private ColumnarBatch currentColumnarBatch;

  public LanceFragmentColumnarBatchScanner(
      LanceFragmentScanner fragmentScanner, ArrowReader arrowReader) {
    this.fragmentScanner = fragmentScanner;
    this.arrowReader = arrowReader;
  }

  public static LanceFragmentColumnarBatchScanner create(
      int fragmentId, LanceInputPartition inputPartition) {
    LanceFragmentScanner fragmentScanner = LanceFragmentScanner.create(fragmentId, inputPartition);
    return new LanceFragmentColumnarBatchScanner(fragmentScanner, fragmentScanner.getArrowReader());
  }

  public boolean loadNextBatch() throws IOException {
    if (arrowReader.loadNextBatch()) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      int rowCount = root.getRowCount();
      LanceInputPartition inputPartition = fragmentScanner.getInputPartition();
      StructType schema = inputPartition.getSchema();

      // Build name-based lookup for Arrow vectors
      Map<String, ColumnVector> vectorsByName = new HashMap<>();
      for (FieldVector fv : root.getFieldVectors()) {
        vectorsByName.put(fv.getField().getName(), new LanceArrowColumnVector(fv));
      }

      // Build blob virtual columns keyed by name
      Map<String, FieldVector> arrowFieldsByName = new HashMap<>();
      for (FieldVector fv : root.getFieldVectors()) {
        arrowFieldsByName.put(fv.getField().getName(), fv);
      }
      for (StructField field : schema.fields()) {
        String name = field.name();
        if (name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)) {
          String baseName =
              name.substring(0, name.length() - LanceConstant.BLOB_POSITION_SUFFIX.length());
          FieldVector blobVector = arrowFieldsByName.get(baseName);
          if (blobVector instanceof StructVector) {
            vectorsByName.put(name, new BlobPositionColumnVector((StructVector) blobVector));
          }
        } else if (name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX)) {
          String baseName =
              name.substring(0, name.length() - LanceConstant.BLOB_SIZE_SUFFIX.length());
          FieldVector blobVector = arrowFieldsByName.get(baseName);
          if (blobVector instanceof StructVector) {
            vectorsByName.put(name, new BlobSizeColumnVector((StructVector) blobVector));
          }
        }
      }

      // Add _fragid constant column if needed
      if (fragmentScanner.withFragemtId()) {
        ConstantColumnVector fragmentVector =
            new ConstantColumnVector(rowCount, DataTypes.IntegerType);
        fragmentVector.setInt(fragmentScanner.fragmentId());
        vectorsByName.put(LanceConstant.FRAGMENT_ID, fragmentVector);
      }

      // Assemble columns in exact schema order
      ColumnVector[] columns = new ColumnVector[schema.fields().length];
      for (int i = 0; i < schema.fields().length; i++) {
        columns[i] = vectorsByName.get(schema.fields()[i].name());
      }

      currentColumnarBatch = new ColumnarBatch(columns, rowCount);
      return true;
    }
    return false;
  }

  /**
   * @return the current batch, the caller responsible for closing the batch
   */
  public ColumnarBatch getCurrentBatch() {
    return currentColumnarBatch;
  }

  @Override
  public void close() throws IOException {
    try {
      if (currentColumnarBatch != null) {
        currentColumnarBatch.close();
      }
    } finally {
      try {
        arrowReader.close();
      } finally {
        fragmentScanner.close();
      }
    }
  }

  // Virtual column vector for blob position
  private static class BlobPositionColumnVector extends ColumnVector {
    private final BlobStructAccessor accessor;

    BlobPositionColumnVector(StructVector blobStruct) {
      super(DataTypes.LongType);
      this.accessor = new BlobStructAccessor(blobStruct);
    }

    @Override
    public void close() {
      try {
        accessor.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    @Override
    public boolean hasNull() {
      return accessor.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
      return accessor.getNullCount();
    }

    @Override
    public boolean isNullAt(int rowId) {
      return accessor.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException("Blob position is not boolean");
    }

    @Override
    public byte getByte(int rowId) {
      throw new UnsupportedOperationException("Blob position is not byte");
    }

    @Override
    public short getShort(int rowId) {
      throw new UnsupportedOperationException("Blob position is not short");
    }

    @Override
    public int getInt(int rowId) {
      return (int) getLong(rowId);
    }

    @Override
    public long getLong(int rowId) {
      Long position = accessor.getPosition(rowId);
      return position != null ? position : 0L;
    }

    @Override
    public float getFloat(int rowId) {
      throw new UnsupportedOperationException("Blob position is not float");
    }

    @Override
    public double getDouble(int rowId) {
      throw new UnsupportedOperationException("Blob position is not double");
    }

    @Override
    public org.apache.spark.sql.types.Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException("Blob position is not decimal");
    }

    @Override
    public org.apache.spark.unsafe.types.UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException("Blob position is not string");
    }

    @Override
    public byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException("Blob position is not binary");
    }

    @Override
    public ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException("Blob position is not array");
    }

    @Override
    public ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException("Blob position is not map");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw new UnsupportedOperationException("Blob position column does not have children");
    }
  }

  // Virtual column vector for blob size
  private static class BlobSizeColumnVector extends ColumnVector {
    private final BlobStructAccessor accessor;

    BlobSizeColumnVector(StructVector blobStruct) {
      super(DataTypes.LongType);
      this.accessor = new BlobStructAccessor(blobStruct);
    }

    @Override
    public void close() {
      try {
        accessor.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    @Override
    public boolean hasNull() {
      return accessor.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
      return accessor.getNullCount();
    }

    @Override
    public boolean isNullAt(int rowId) {
      return accessor.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException("Blob size is not boolean");
    }

    @Override
    public byte getByte(int rowId) {
      throw new UnsupportedOperationException("Blob size is not byte");
    }

    @Override
    public short getShort(int rowId) {
      throw new UnsupportedOperationException("Blob size is not short");
    }

    @Override
    public int getInt(int rowId) {
      return (int) getLong(rowId);
    }

    @Override
    public long getLong(int rowId) {
      Long size = accessor.getSize(rowId);
      return size != null ? size : 0L;
    }

    @Override
    public float getFloat(int rowId) {
      throw new UnsupportedOperationException("Blob size is not float");
    }

    @Override
    public double getDouble(int rowId) {
      throw new UnsupportedOperationException("Blob size is not double");
    }

    @Override
    public org.apache.spark.sql.types.Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException("Blob size is not decimal");
    }

    @Override
    public org.apache.spark.unsafe.types.UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException("Blob size is not string");
    }

    @Override
    public byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException("Blob size is not binary");
    }

    @Override
    public ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException("Blob size is not array");
    }

    @Override
    public ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException("Blob size is not map");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw new UnsupportedOperationException("Blob size column does not have children");
    }
  }
}
