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
package org.lance.spark.read;

import org.lance.Dataset;
import org.lance.ipc.FilteredRead;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.internal.LanceDatasetCache;
import org.lance.spark.internal.LanceFragmentColumnarBatchScanner;
import org.lance.spark.vectorized.BlobStructAccessor;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LanceColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceInputPartition inputPartition;
  private int fragmentIndex;
  private LanceFragmentColumnarBatchScanner fragmentReader;
  private ColumnarBatch currentBatch;

  // Distributed mode fields
  private ArrowReader distributedReader;
  private boolean distributedReaderInitialized;
  private boolean distributedModeFailed;

  public LanceColumnarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.fragmentIndex = 0;
    this.distributedReaderInitialized = false;
    this.distributedModeFailed = false;
  }

  @Override
  public boolean next() throws IOException {
    if (inputPartition.isDistributedMode() && !distributedModeFailed) {
      return nextDistributed();
    }
    return nextLegacy();
  }

  /**
   * Distributed read path: executes a pre-planned task via {@link
   * FilteredRead#executeFilteredRead}. Falls back to legacy path if execution fails (e.g. v1 format
   * datasets).
   */
  private boolean nextDistributed() throws IOException {
    if (!distributedReaderInitialized) {
      initDistributedReader();
      distributedReaderInitialized = true;
    }
    try {
      if (distributedReader.loadNextBatch()) {
        VectorSchemaRoot root = distributedReader.getVectorSchemaRoot();
        currentBatch = buildColumnarBatch(root);
        if (currentBatch == null) {
          // Schema mismatch: Arrow output columns don't match readSchema — fall back to legacy
          distributedModeFailed = true;
          distributedReader.close();
          distributedReader = null;
          return nextLegacy();
        }
        return true;
      }
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("FilteredRead on v1")) {
        distributedModeFailed = true;
        if (distributedReader != null) {
          distributedReader.close();
          distributedReader = null;
        }
        return nextLegacy();
      }
      throw e;
    }
    return false;
  }

  private void initDistributedReader() {
    byte[] task = inputPartition.getFilteredReadTask();
    LanceDatasetCache.CachedDataset cached =
        LanceDatasetCache.getDataset(
            inputPartition.getReadOptions(),
            inputPartition.getInitialStorageOptions(),
            inputPartition.getNamespaceImpl(),
            inputPartition.getNamespaceProperties());
    Dataset dataset = cached.getDataset();
    distributedReader = FilteredRead.executeFilteredRead(dataset, task, LanceRuntime.allocator());
  }

  /**
   * Builds a {@link ColumnarBatch} from a {@link VectorSchemaRoot}, assembling columns in schema
   * order using name-based lookup to avoid positional mismatch between Arrow output and Spark's
   * readSchema.
   */
  private ColumnarBatch buildColumnarBatch(VectorSchemaRoot root) {
    int rowCount = root.getRowCount();
    StructType schema = inputPartition.getSchema();

    // Build name-based lookup for Arrow vectors
    Map<String, ColumnVector> vectorsByName = new HashMap<>();
    for (FieldVector fv : root.getFieldVectors()) {
      vectorsByName.put(fv.getField().getName(), new LanceArrowColumnVector(fv));
    }

    // Build blob virtual columns and _fragid constant keyed by name
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
    if (schema.getFieldIndex(LanceConstant.FRAGMENT_ID).nonEmpty()) {
      int fragmentId = inputPartition.getLanceSplit().getFragments().get(0);
      ConstantColumnVector fragmentVector =
          new ConstantColumnVector(rowCount, DataTypes.IntegerType);
      fragmentVector.setInt(fragmentId);
      vectorsByName.put(LanceConstant.FRAGMENT_ID, fragmentVector);
    }

    // Assemble columns in exact schema order
    ColumnVector[] columns = new ColumnVector[schema.fields().length];
    for (int i = 0; i < schema.fields().length; i++) {
      columns[i] = vectorsByName.get(schema.fields()[i].name());
      if (columns[i] == null) {
        // Schema column not found in Arrow output — caller should fall back to legacy path
        return null;
      }
    }

    return new ColumnarBatch(columns, rowCount);
  }

  /**
   * Legacy read path: iterates over fragments and reads batches using {@link
   * LanceFragmentColumnarBatchScanner}.
   */
  private boolean nextLegacy() throws IOException {
    if (loadNextBatchFromCurrentReader()) {
      return true;
    }
    while (fragmentIndex < inputPartition.getLanceSplit().getFragments().size()) {
      if (fragmentReader != null) {
        fragmentReader.close();
      }
      fragmentReader =
          LanceFragmentColumnarBatchScanner.create(
              inputPartition.getLanceSplit().getFragments().get(fragmentIndex), inputPartition);
      fragmentIndex++;
      if (loadNextBatchFromCurrentReader()) {
        return true;
      }
    }
    return false;
  }

  private boolean loadNextBatchFromCurrentReader() throws IOException {
    if (fragmentReader != null && fragmentReader.loadNextBatch()) {
      currentBatch = fragmentReader.getCurrentBatch();
      return true;
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    if (fragmentReader != null) {
      try {
        fragmentReader.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    if (distributedReader != null) {
      try {
        distributedReader.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Virtual column vector for blob position. Mirrors the implementation in {@link
   * LanceFragmentColumnarBatchScanner}.
   */
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
    public org.apache.spark.sql.vectorized.ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException("Blob position is not array");
    }

    @Override
    public org.apache.spark.sql.vectorized.ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException("Blob position is not map");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw new UnsupportedOperationException("Blob position column does not have children");
    }
  }

  /**
   * Virtual column vector for blob size. Mirrors the implementation in {@link
   * LanceFragmentColumnarBatchScanner}.
   */
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
    public org.apache.spark.sql.vectorized.ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException("Blob size is not array");
    }

    @Override
    public org.apache.spark.sql.vectorized.ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException("Blob size is not map");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw new UnsupportedOperationException("Blob size column does not have children");
    }
  }
}
