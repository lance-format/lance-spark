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
package org.lance.spark.write;

import org.lance.memwal.ShardingEvaluator;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceSchema;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Buffers Spark rows into Arrow batches and evaluates MemWAL sharding keys natively. */
final class ShardingBatchKeyEvaluator implements AutoCloseable {
  private final StructType sparkSchema;
  private final Schema arrowSchema;
  private final ShardingBinding binding;
  private final int batchSize;
  private final long maxBatchBytes;
  private final BufferAllocator allocator;
  private final List<InternalRow> pendingRows = new ArrayList<>();

  private VectorSchemaRoot root;
  private org.lance.spark.arrow.LanceArrowWriter arrowWriter;

  ShardingBatchKeyEvaluator(
      StructType sparkSchema, LanceSparkWriteOptions writeOptions, ShardingBinding binding) {
    this.sparkSchema = sparkSchema;
    this.arrowSchema =
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, writeOptions.isUseLargeVarTypes());
    this.binding = binding;
    this.batchSize = writeOptions.getBatchSize();
    this.maxBatchBytes = writeOptions.getMaxBatchBytes();
    this.allocator =
        LanceRuntime.allocator().newChildAllocator("sharding-key-evaluator", 0, Long.MAX_VALUE);
    allocateBatch();
  }

  void write(InternalRow row, RowConsumer consumer) throws IOException {
    InternalRow copied = row.copy();
    pendingRows.add(copied);
    arrowWriter.write(copied);
    if (pendingRows.size() >= batchSize
        || (allocator.getAllocatedMemory() >= maxBatchBytes && !pendingRows.isEmpty())) {
      flush(consumer);
    }
  }

  void flush(RowConsumer consumer) throws IOException {
    if (pendingRows.isEmpty()) {
      return;
    }
    arrowWriter.finish();
    root.setRowCount(pendingRows.size());
    try (ArrowReader reader = binding.evaluate(allocator, root)) {
      if (!reader.loadNextBatch()) {
        throw new IOException("Lance sharding evaluator returned no result batch");
      }
      VectorSchemaRoot result = reader.getVectorSchemaRoot();
      if (result.getRowCount() != pendingRows.size()) {
        throw new IOException(
            "Lance sharding evaluator returned "
                + result.getRowCount()
                + " rows for "
                + pendingRows.size()
                + " input rows");
      }
      for (int i = 0; i < pendingRows.size(); i++) {
        consumer.accept(pendingRows.get(i), keyAt(result, i));
      }
      if (reader.loadNextBatch()) {
        throw new IOException("Lance sharding evaluator returned multiple result batches");
      }
    } finally {
      pendingRows.clear();
      resetBatch();
    }
  }

  private Object keyAt(VectorSchemaRoot result, int rowId) {
    List<FieldVector> vectors = result.getFieldVectors();
    if (vectors.size() == 1) {
      return valueAt(vectors.get(0), rowId);
    }
    Object[] keys = new Object[vectors.size()];
    for (int i = 0; i < vectors.size(); i++) {
      keys[i] = valueAt(vectors.get(i), rowId);
    }
    return Arrays.asList(keys);
  }

  private static Object valueAt(FieldVector vector, int rowId) {
    if (vector.isNull(rowId)) {
      return null;
    }
    Object value = vector.getObject(rowId);
    if (value instanceof Text) {
      return value.toString();
    }
    if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      return ByteBuffer.wrap(Arrays.copyOf(bytes, bytes.length)).asReadOnlyBuffer();
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buffer = ((ByteBuffer) value).duplicate();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }
    return value;
  }

  private void resetBatch() {
    closeBatch();
    allocateBatch();
  }

  private void allocateBatch() {
    root = VectorSchemaRoot.create(arrowSchema, allocator);
    root.allocateNew();
    arrowWriter = org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(root, sparkSchema);
  }

  private void closeBatch() {
    if (root != null) {
      root.close();
      root = null;
      arrowWriter = null;
    }
  }

  @Override
  public void close() {
    closeBatch();
    allocator.close();
  }

  interface RowConsumer {
    void accept(InternalRow row, Object key) throws IOException;
  }

  static final class ShardingBinding {
    final ShardingSpec spec;
    final LanceSchema lanceSchema;

    ShardingBinding(ShardingSpec spec, LanceSchema lanceSchema) {
      this.spec = spec;
      this.lanceSchema = lanceSchema;
    }

    ArrowReader evaluate(BufferAllocator allocator, VectorSchemaRoot root) {
      if (lanceSchema == null) {
        return ShardingEvaluator.evaluate(allocator, root, spec);
      }
      return ShardingEvaluator.evaluate(allocator, root, spec, lanceSchema);
    }
  }
}
