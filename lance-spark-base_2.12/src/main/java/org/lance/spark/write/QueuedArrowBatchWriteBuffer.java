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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Queue-based buffer for Arrow batches that enables pipelined batch processing.
 *
 * <p>Unlike the semaphore-based {@link SemaphoreArrowBatchWriteBuffer} which blocks on every row
 * write, this implementation uses a bounded queue to allow multiple batches to be in flight
 * simultaneously. This enables better pipelining between Spark row ingestion and Lance fragment
 * creation.
 *
 * <p>Batches are flushed when either the row count reaches {@code batchSize} or the allocated
 * memory for the current batch exceeds {@code maxBatchBytes}, whichever comes first. This limits
 * the size of each individual batch when rows are very large (e.g., rows with large binary/string
 * columns or embeddings).
 *
 * <p>Because this implementation is queue-based, multiple completed batches can be buffered at the
 * same time. As a result, total in-flight Arrow memory can be roughly {@code queueDepth *
 * maxBatchBytes}, plus the current producer batch and allocator overhead. Users may need to tune
 * {@code queueDepth} and {@code maxBatchBytes} together to stay within memory limits.
 *
 * <p>Architecture:
 *
 * <pre>
 * Producer (Spark thread):           Consumer (Fragment creation thread):
 * - Writes rows to local batch       - Takes completed batches from queue
 * - When batch full, puts in queue   - Writes batches to Lance format
 * - Only blocks if queue is full     - Processes batches in parallel with producer
 * </pre>
 */
public class QueuedArrowBatchWriteBuffer extends ArrowBatchWriteBuffer {
  /** Default queue depth - number of batches that can be buffered. */
  private static final int DEFAULT_QUEUE_DEPTH = 8;

  private final Schema schema;
  private final StructType sparkSchema;
  private final int batchSize;
  private final long maxBatchBytes;
  private final int queueDepth;

  /**
   * Queue holding completed batches ready for consumption. Each entry pairs a VectorSchemaRoot with
   * its dedicated child allocator, so the consumer can free both when done.
   */
  private final BlockingQueue<BatchEntry> batchQueue;

  /** Child allocator for producer batches (shares root with consumer for buffer transfer). */
  private final BufferAllocator producerAllocator;

  /** Current batch being filled by producer. */
  private VectorSchemaRoot currentBatch;

  /** Child allocator dedicated to the current batch for accurate byte tracking. */
  private BufferAllocator currentBatchAllocator;

  /** Arrow writer for current batch. */
  private org.lance.spark.arrow.LanceArrowWriter currentArrowWriter;

  /** Count of rows in current batch. */
  private final AtomicInteger currentBatchRowCount = new AtomicInteger(0);

  /** Flag indicating producer has finished. */
  private volatile boolean producerFinished = false;

  /** Flag indicating consumer has consumed all batches. */
  private volatile boolean consumerFinished = false;

  /** Current batch being read by consumer (for ArrowReader interface). */
  private BatchEntry consumerCurrentEntry;

  /** Pairs a VectorSchemaRoot with its allocator for proper lifecycle management. */
  private static class BatchEntry {
    final VectorSchemaRoot batch;
    final BufferAllocator allocator;

    BatchEntry(VectorSchemaRoot batch, BufferAllocator allocator) {
      this.batch = batch;
      this.allocator = allocator;
    }

    void close() {
      try {
        batch.close();
      } finally {
        allocator.close();
      }
    }
  }

  public QueuedArrowBatchWriteBuffer(
      BufferAllocator allocator, Schema schema, StructType sparkSchema, int batchSize) {
    this(
        allocator,
        schema,
        sparkSchema,
        batchSize,
        DEFAULT_QUEUE_DEPTH,
        LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public QueuedArrowBatchWriteBuffer(StructType sparkSchema, int batchSize, int queueDepth) {
    this(sparkSchema, batchSize, queueDepth, false, LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  /** Constructor with large var types support, using LanceRuntime allocator. */
  public QueuedArrowBatchWriteBuffer(
      StructType sparkSchema, int batchSize, int queueDepth, boolean useLargeVarTypes) {
    this(
        sparkSchema,
        batchSize,
        queueDepth,
        useLargeVarTypes,
        LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  /** Constructor with all tuning parameters, using LanceRuntime allocator. */
  public QueuedArrowBatchWriteBuffer(
      StructType sparkSchema,
      int batchSize,
      int queueDepth,
      boolean useLargeVarTypes,
      long maxBatchBytes) {
    this(
        LanceRuntime.allocator(),
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, useLargeVarTypes),
        sparkSchema,
        batchSize,
        queueDepth,
        maxBatchBytes);
  }

  public QueuedArrowBatchWriteBuffer(
      BufferAllocator allocator,
      Schema schema,
      StructType sparkSchema,
      int batchSize,
      int queueDepth) {
    this(
        allocator,
        schema,
        sparkSchema,
        batchSize,
        queueDepth,
        LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  public QueuedArrowBatchWriteBuffer(
      BufferAllocator allocator,
      Schema schema,
      StructType sparkSchema,
      int batchSize,
      int queueDepth,
      long maxBatchBytes) {
    super(allocator);
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(batchSize > 0, "Batch size must be positive");
    Preconditions.checkArgument(queueDepth > 0, "Queue depth must be positive");
    Preconditions.checkArgument(maxBatchBytes > 0, "maxBatchBytes must be positive");

    this.schema = schema;
    this.sparkSchema = sparkSchema;
    this.batchSize = batchSize;
    this.maxBatchBytes = maxBatchBytes;
    this.queueDepth = queueDepth;
    this.batchQueue = new ArrayBlockingQueue<>(queueDepth);

    // Create a child allocator for producer that shares the same root as the consumer
    // allocator. This is required for Arrow buffer transfer between producer and consumer.
    this.producerAllocator =
        allocator.newChildAllocator("queued-buffer-producer", 0, Long.MAX_VALUE);

    // Initialize first batch for producer
    allocateNewBatch();
  }

  /** Allocates a new batch for the producer to fill. */
  private void allocateNewBatch() {
    // Each batch gets its own child allocator so getAllocatedMemory() accurately reflects
    // only this batch's memory, unaffected by concurrent consumer freeing of older batches.
    currentBatchAllocator = producerAllocator.newChildAllocator("batch", 0, Long.MAX_VALUE);
    try {
      currentBatch = VectorSchemaRoot.create(schema, currentBatchAllocator);
      currentBatch.allocateNew();
    } catch (Exception e) {
      if (currentBatch != null) {
        currentBatch.close();
      }
      currentBatchAllocator.close();
      throw e;
    }
    currentArrowWriter =
        org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(currentBatch, sparkSchema);
    currentBatchRowCount.set(0);
  }

  /** Returns whether the current batch should be flushed based on byte size. */
  private boolean isBatchFullByBytes() {
    if (maxBatchBytes == Long.MAX_VALUE) {
      return false;
    }
    return currentBatchAllocator.getAllocatedMemory() >= maxBatchBytes;
  }

  /**
   * Writes a row to the current batch. When the batch is full (by row count or byte size), it is
   * queued for consumption and a new batch is allocated.
   *
   * <p>This method only blocks if the queue is full, allowing the producer to continue writing
   * while the consumer processes previous batches.
   *
   * @param row The row to write
   */
  @Override
  public void write(InternalRow row) {
    Preconditions.checkNotNull(row);
    Preconditions.checkState(!producerFinished, "Cannot write after setFinished() is called");

    checkForError();

    try {
      currentArrowWriter.write(row);

      int count = currentBatchRowCount.incrementAndGet();

      if (count >= batchSize || (count > 0 && isBatchFullByBytes())) {
        // Batch is full - finalize and queue it
        currentArrowWriter.finish();
        currentBatch.setRowCount(count);

        BatchEntry entry = new BatchEntry(currentBatch, currentBatchAllocator);
        // Put in queue, periodically checking for consumer errors
        try {
          while (!batchQueue.offer(entry, 100, TimeUnit.MILLISECONDS)) {
            checkForError();
          }
        } catch (RuntimeException e) {
          entry.close();
          throw e;
        }

        // Allocate new batch for next writes
        allocateNewBatch();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while queuing batch", e);
    }
  }

  /**
   * Signals that the producer has finished writing. Any partial batch is queued for consumption.
   */
  @Override
  public void setFinished() {
    if (producerFinished) {
      return;
    }

    try {
      // Queue any remaining partial batch before signaling completion to avoid
      // a race where the consumer sees producerFinished=true with an empty queue
      // and exits before the final batch is enqueued.
      int remainingRows = currentBatchRowCount.get();
      if (remainingRows > 0) {
        currentArrowWriter.finish();
        currentBatch.setRowCount(remainingRows);
        BatchEntry entry = new BatchEntry(currentBatch, currentBatchAllocator);
        try {
          while (!batchQueue.offer(entry, 100, TimeUnit.MILLISECONDS)) {
            checkForError();
          }
        } catch (RuntimeException e) {
          entry.close();
          throw e;
        }
      } else {
        // No remaining rows, close the empty batch and its allocator
        currentBatch.close();
        currentBatchAllocator.close();
      }
      currentBatch = null;
      currentBatchAllocator = null;
      currentArrowWriter = null;

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
      // Close previous batch if any
      if (consumerCurrentEntry != null) {
        consumerCurrentEntry.close();
        consumerCurrentEntry = null;
      }

      // Try to get next batch from queue
      while (true) {
        // Use poll with timeout to check for producer finished
        BatchEntry entry = batchQueue.poll(100, TimeUnit.MILLISECONDS);

        if (entry != null) {
          consumerCurrentEntry = entry;
          return true;
        }

        // Check if producer is done and queue is empty
        if (producerFinished && batchQueue.isEmpty()) {
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
    if (consumerCurrentEntry != null) {
      return consumerCurrentEntry.batch;
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
    // No-op - batch is already prepared by producer
  }

  @Override
  public long bytesRead() {
    return 0; // Not tracked
  }

  @Override
  protected void closeReadSource() throws IOException {
    // Close any remaining batch
    if (consumerCurrentEntry != null) {
      consumerCurrentEntry.close();
      consumerCurrentEntry = null;
    }

    // Drain and close any batches left in queue
    BatchEntry entry;
    while ((entry = batchQueue.poll()) != null) {
      entry.close();
    }

    // Close producer allocator
    producerAllocator.close();
  }

  @Override
  protected Schema readSchema() {
    return this.schema;
  }

  /** Returns the queue depth (for monitoring/debugging). */
  public int getQueueDepth() {
    return queueDepth;
  }

  /** Returns the current queue size (for monitoring/debugging). */
  public int getCurrentQueueSize() {
    return batchQueue.size();
  }
}
