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

import org.lance.spark.utils.BlobReference;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

public class BlobStructAccessor implements AutoCloseable {
  private final StructVector structVector;
  private final UInt8Vector positionVector;
  private final UInt8Vector sizeVector;

  // Blob reference context — set by the scanner to enable serializing blob references
  private String datasetUri;
  private String columnName;
  private long[] rowAddresses;

  // Constant serialized prefix (magic + version + datasetUri + columnName) for this batch.
  // Precomputed once in setBlobReferenceContext so the per-row hot path only appends rowAddress.
  private byte[] referencePrefix;

  public BlobStructAccessor(StructVector structVector) {
    this.structVector = structVector;
    // Blob structs have two fields: position and size (both unsigned Int64)
    this.positionVector = (UInt8Vector) structVector.getChild("position");
    this.sizeVector = (UInt8Vector) structVector.getChild("size");
  }

  /**
   * Sets the context needed to produce blob references. When set, {@link #getBlobReference(int)}
   * will return a serialized {@link BlobReference} that the write side can use to fetch the actual
   * blob bytes from the source dataset.
   *
   * @param datasetUri the URI of the source dataset
   * @param columnName the blob column name
   * @param rowAddresses row addresses for each row in this batch
   */
  public void setBlobReferenceContext(String datasetUri, String columnName, long[] rowAddresses) {
    this.datasetUri = datasetUri;
    this.columnName = columnName;
    this.rowAddresses = rowAddresses;
    // datasetUri and columnName are constant for the batch — encode the reference prefix once
    // here rather than re-encoding both strings per row in getBlobReference().
    this.referencePrefix = BlobReference.serializePrefix(datasetUri, columnName);
  }

  /** Returns true if blob reference context has been set. */
  public boolean hasBlobReferenceContext() {
    return datasetUri != null && columnName != null && rowAddresses != null;
  }

  public int getNullCount() {
    return structVector.getNullCount();
  }

  public boolean isNullAt(int rowId) {
    return structVector.isNull(rowId);
  }

  /**
   * Returns a serialized blob reference for the given row. Returns null if the row is null or if
   * the blob reference context is not set. Returns empty byte array if the blob has zero size (null
   * blob value encoded as position=0, size=0).
   */
  public byte[] getBlobReference(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    if (!hasBlobReferenceContext()) {
      return new byte[0];
    }
    // Hot path (once per scanned blob row): read size with the primitive accessor instead of
    // boxing through getObjectNoOverflow(), which allocates a BigInteger per row. The unsigned
    // overflow handling is irrelevant here — a real blob never approaches 2^63 bytes, and only 0L
    // compares equal to zero for the null/empty check below.
    long blobSize = sizeVector.isNull(rowId) ? 0L : sizeVector.get(rowId);
    if (blobSize == 0L) {
      // Zero-size blob — either truly empty or null encoded as (0,0)
      return new byte[0];
    }
    // Carry the blob size so the write side can budget resolved bytes against maxBatchBytes before
    // the reference is materialized into actual blob bytes.
    return BlobReference.appendRowAddressAndSize(referencePrefix, rowAddresses[rowId], blobSize);
  }

  public InternalRow getStruct(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }

    // Create a struct with position and size as Long values
    Object[] values = new Object[2];
    values[0] = positionVector.isNull(rowId) ? null : positionVector.getObjectNoOverflow(rowId);
    values[1] = sizeVector.isNull(rowId) ? null : sizeVector.getObjectNoOverflow(rowId);

    return new GenericInternalRow(values);
  }

  public Long getPosition(int rowId) {
    if (isNullAt(rowId) || positionVector.isNull(rowId)) {
      return null;
    }
    return ((java.math.BigInteger) positionVector.getObjectNoOverflow(rowId)).longValue();
  }

  public Long getSize(int rowId) {
    if (isNullAt(rowId) || sizeVector.isNull(rowId)) {
      return null;
    }
    return ((java.math.BigInteger) sizeVector.getObjectNoOverflow(rowId)).longValue();
  }

  @Override
  public void close() {
    // Vector cleanup is handled by parent
  }
}
