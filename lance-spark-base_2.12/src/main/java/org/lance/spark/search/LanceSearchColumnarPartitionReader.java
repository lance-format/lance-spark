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
package org.lance.spark.search;

import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceRuntime;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LanceSearchColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceSearchInputPartition inputPartition;
  private ArrowFileReader arrowReader;
  private ColumnarBatch currentBatch;
  private boolean finished;

  public LanceSearchColumnarPartitionReader(LanceSearchInputPartition inputPartition) {
    this.inputPartition = inputPartition;
  }

  @Override
  public boolean next() throws IOException {
    if (finished) {
      return false;
    }
    if (arrowReader == null) {
      openArrowReader();
    }
    if (arrowReader.loadNextBatch()) {
      currentBatch = toColumnarBatch(arrowReader.getVectorSchemaRoot(), inputPartition.getSchema());
      return true;
    }
    finished = true;
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    try {
      if (currentBatch != null) {
        currentBatch.close();
      }
    } finally {
      if (arrowReader != null) {
        arrowReader.close();
      }
    }
  }

  private void openArrowReader() throws IOException {
    LanceSearchQuery query = inputPartition.getQuery();
    LanceNamespace namespace =
        LanceRuntime.getOrCreateNamespace(query.getNamespaceImpl(), query.getNamespaceProperties());
    if (namespace == null) {
      throw new IOException("Lance namespace is required for search");
    }
    try {
      byte[] bytes = namespace.queryTable(query.toQueryTableRequest());
      arrowReader =
          new ArrowFileReader(
              new ByteArrayReadableSeekableByteChannel(bytes), LanceRuntime.allocator());
    } finally {
      if (namespace instanceof Closeable) {
        ((Closeable) namespace).close();
      }
    }
  }

  private ColumnarBatch toColumnarBatch(VectorSchemaRoot root, StructType schema) {
    Map<String, FieldVector> actualFields = new HashMap<>();
    for (FieldVector vector : root.getFieldVectors()) {
      actualFields.put(vector.getField().getName(), vector);
    }

    StructField[] fields = schema.fields();
    ColumnVector[] vectors = new ColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String fieldName = fields[i].name();
      FieldVector vector = actualFields.get(fieldName);
      if (vector == null) {
        throw new IllegalStateException(
            "Lance search did not return expected field '" + fieldName + "'");
      }
      vectors[i] = new LanceArrowColumnVector(vector, false);
    }
    return new ColumnarBatch(vectors, root.getRowCount());
  }
}
