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

import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Pool-based buffer for Arrow batches that pre-allocates a fixed set of VectorSchemaRoots and
 * cycles through them. No per-batch allocation after initialization — vectors are reused via {@code
 * reset()}, which preserves underlying buffer capacity.
 *
 * <p>This replaces both the semaphore-based and queue-based approaches:
 *
 * <ul>
 *   <li>Unlike the semaphore approach, there is no lock contention on every row write and no
 *       per-batch vector reallocation.
 *   <li>Unlike the queue approach, there is no per-batch child allocator overhead.
 * </ul>
 *
 * <p>Batches are flushed when either the row count reaches {@code batchSize} or the estimated
 * memory for the current batch exceeds {@code maxBatchBytes}, whichever comes first. Byte tracking
 * uses row-level measurement: fixed-width vector costs are precomputed from the schema, and
 * variable-width vector growth is tracked via {@code getDataBuffer().readableBytes()} deltas.
 *
 * <p>Architecture:
 *
 * <pre>
 * Producer (Spark thread):              Consumer (Fragment creation thread):
 * - Grabs free root from pool           - Takes filled root from readyQueue
 * - Fills rows, resets on reuse         - Reads batches via ArrowReader interface
 * - When full, puts in readyQueue       - Returns root to freePool when done
 * - Only blocks if pool is exhausted    - Processes in parallel with producer
 * </pre>
 */
public class PooledArrowBatchWriteBuffer extends ArrowBatchWriteBuffer {
  private static final int DEFAULT_POOL_SIZE = 4;

  private final Schema schema;
  private final StructType sparkSchema;
  private final int batchSize;
  private final long maxBatchBytes;
  private final int poolSize;

  /** Free roots ready for the producer to fill. */
  private final BlockingQueue<VectorSchemaRoot> freePool;

  /** Filled roots ready for the consumer to read. */
  private final BlockingQueue<VectorSchemaRoot> readyQueue;

  // -- Producer state (only touched by producer thread) --
  private VectorSchemaRoot producerBatch;
  private org.lance.spark.arrow.LanceArrowWriter producerArrowWriter;
  private int producerRowCount = 0;
  private volatile boolean producerFinished = false;

  // -- Consumer state (only touched by consumer thread) --
  private VectorSchemaRoot consumerBatch;
  private boolean consumerFinished = false;

  // -- Byte tracking --
  /** Precomputed fixed-width bytes per row (sum of type widths + validity bytes). */
  private final long fixedBytesPerRow;

  /** Indices of variable-width vectors for per-row byte tracking. */
  private final int[] variableWidthIndices;

  /** Accumulated variable-width bytes for current batch. */
  private long currentVarBytes = 0;

  public PooledArrowBatchWriteBuffer(
      BufferAllocator allocator, Schema schema, StructType sparkSchema, int batchSize) {
    this(
        allocator,
        schema,
        sparkSchema,
        batchSize,
        DEFAULT_POOL_SIZE,
        LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  public PooledArrowBatchWriteBuffer(
      BufferAllocator allocator,
      Schema schema,
      StructType sparkSchema,
      int batchSize,
      int poolSize,
      long maxBatchBytes) {
    super(allocator);
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(batchSize > 0, "Batch size must be positive");
    Preconditions.checkArgument(poolSize > 0, "Pool size must be positive");
    Preconditions.checkArgument(maxBatchBytes > 0, "maxBatchBytes must be positive");

    this.schema = schema;
    this.sparkSchema = sparkSchema;
    this.batchSize = batchSize;
    this.maxBatchBytes = maxBatchBytes;
    this.poolSize = poolSize;
    this.freePool = new ArrayBlockingQueue<>(poolSize);
    this.readyQueue = new ArrayBlockingQueue<>(poolSize);

    // Precompute byte tracking metadata from schema
    VectorSchemaRoot probe = VectorSchemaRoot.create(schema, allocator);
    long fixedBytes = 0;
    List<Integer> varIndices = new ArrayList<>();
    for (int i = 0; i < probe.getFieldVectors().size(); i++) {
      FieldVector vec = probe.getFieldVectors().get(i);
      if (vec instanceof BaseVariableWidthVector || vec instanceof BaseLargeVariableWidthVector) {
        varIndices.add(i);
      } else if (vec instanceof BaseFixedWidthVector) {
        // Fixed-width: type width + 1 bit validity (amortized to 1 byte per 8 rows,
        // but we approximate as 1 byte per row for simplicity)
        int typeWidth = ((BaseFixedWidthVector) vec).getTypeWidth();
        fixedBytes += typeWidth + 1; // data + validity byte
      }
    }
    probe.close();
    this.fixedBytesPerRow = fixedBytes;
    this.variableWidthIndices = varIndices.stream().mapToInt(Integer::intValue).toArray();

    // Pre-allocate all roots
    for (int i = 0; i < poolSize; i++) {
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
      root.allocateNew();
      freePool.add(root);
    }

    // Grab the first root for the producer
    producerBatch = freePool.poll();
    producerArrowWriter =
        org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(producerBatch, sparkSchema);
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public PooledArrowBatchWriteBuffer(StructType sparkSchema, int batchSize, int poolSize) {
    this(sparkSchema, batchSize, poolSize, false, LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  /** Constructor with large var types support, using LanceRuntime allocator. */
  public PooledArrowBatchWriteBuffer(
      StructType sparkSchema, int batchSize, int poolSize, boolean useLargeVarTypes) {
    this(
        sparkSchema,
        batchSize,
        poolSize,
        useLargeVarTypes,
        LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  /** Constructor with all tuning parameters, using LanceRuntime allocator. */
  public PooledArrowBatchWriteBuffer(
      StructType sparkSchema,
      int batchSize,
      int poolSize,
      boolean useLargeVarTypes,
      long maxBatchBytes) {
    this(
        LanceRuntime.allocator(),
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, useLargeVarTypes),
        sparkSchema,
        batchSize,
        poolSize,
        maxBatchBytes);
  }

  /** Returns whether the current batch should be flushed based on byte size. */
  private boolean isBatchFullByBytes() {
    if (maxBatchBytes == Long.MAX_VALUE) {
      return false;
    }
    long estimatedBytes = fixedBytesPerRow * producerRowCount + currentVarBytes;
    return estimatedBytes >= maxBatchBytes;
  }

  @Override
  public void write(InternalRow row) {
    Preconditions.checkNotNull(row);
    Preconditions.checkState(!producerFinished, "Cannot write after setFinished() is called");

    checkForError();

    producerArrowWriter.write(row);
    producerRowCount++;

    // Track variable-width byte growth. Use getBufferSizeFor(rowCount) because setSafe()
    // updates the offset buffer but never advances the data buffer's writerIndex
    // (that only happens at setValueCount), so readableBytes() would stay at 0.
    if (variableWidthIndices.length > 0) {
      long varBytes = 0;
      for (int idx : variableWidthIndices) {
        FieldVector vec = producerBatch.getVector(idx);
        if (vec instanceof BaseVariableWidthVector) {
          varBytes += ((BaseVariableWidthVector) vec).getBufferSizeFor(producerRowCount);
        } else if (vec instanceof BaseLargeVariableWidthVector) {
          varBytes += ((BaseLargeVariableWidthVector) vec).getBufferSizeFor(producerRowCount);
        }
      }
      currentVarBytes = varBytes;
    }

    if (producerRowCount >= batchSize || (producerRowCount > 0 && isBatchFullByBytes())) {
      flushAndAcquireNext();
    }
  }

  private void flushAndAcquireNext() {
    producerArrowWriter.finish();
    producerBatch.setRowCount(producerRowCount);

    try {
      while (!readyQueue.offer(producerBatch, 100, TimeUnit.MILLISECONDS)) {
        checkForError();
      }

      // Acquire a free root for next batch
      VectorSchemaRoot next = null;
      while (next == null) {
        next = freePool.poll(100, TimeUnit.MILLISECONDS);
        if (next == null) {
          checkForError();
        }
      }

      // Reset for reuse — preserves buffer capacity
      producerBatch = next;
      producerArrowWriter =
          org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(producerBatch, sparkSchema);
      producerArrowWriter.reset();
      producerRowCount = 0;
      currentVarBytes = 0;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while queuing batch", e);
    }
  }

  @Override
  public void setFinished() {
    if (producerFinished) {
      return;
    }

    try {
      if (producerRowCount > 0) {
        producerArrowWriter.finish();
        producerBatch.setRowCount(producerRowCount);
        while (!readyQueue.offer(producerBatch, 100, TimeUnit.MILLISECONDS)) {
          checkForError();
        }
      } else {
        freePool.offer(producerBatch);
      }
      producerBatch = null;
      producerArrowWriter = null;

      // Signal completion only after the final batch is safely in the queue
      producerFinished = true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while finishing", e);
    }
  }

  // ========== ArrowReader interface for consumer ==========

  @Override
  public boolean loadNextBatch() throws IOException {
    if (consumerFinished) {
      return false;
    }

    try {
      // Return previous batch to pool
      if (consumerBatch != null) {
        // Reset vectors for reuse
        consumerBatch.setRowCount(0);
        for (FieldVector v : consumerBatch.getFieldVectors()) {
          v.reset();
        }
        freePool.offer(consumerBatch);
        consumerBatch = null;
      }

      while (true) {
        VectorSchemaRoot batch = readyQueue.poll(100, TimeUnit.MILLISECONDS);
        if (batch != null) {
          consumerBatch = batch;
          return true;
        }
        if (producerFinished && readyQueue.isEmpty()) {
          consumerFinished = true;
          return false;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for batch", e);
    }
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() {
    if (consumerBatch != null) {
      return consumerBatch;
    }
    // Return an empty root for initial schema access
    try {
      return super.getVectorSchemaRoot();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get vector schema root", e);
    }
  }

  @Override
  protected void prepareLoadNextBatch() throws IOException {
    // No-op — batch is already prepared by producer
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() throws IOException {
    if (consumerBatch != null) {
      consumerBatch.close();
      consumerBatch = null;
    }
    VectorSchemaRoot r;
    while ((r = freePool.poll()) != null) {
      r.close();
    }
    while ((r = readyQueue.poll()) != null) {
      r.close();
    }
  }

  @Override
  protected Schema readSchema() {
    return this.schema;
  }

  /** Returns the pool size (for monitoring/debugging). */
  public int getPoolSize() {
    return poolSize;
  }
}
