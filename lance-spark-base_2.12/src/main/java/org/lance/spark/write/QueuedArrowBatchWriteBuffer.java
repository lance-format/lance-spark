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
  private final int queueDepth;

  /** Queue holding completed batches ready for consumption. */
  private final BlockingQueue<VectorSchemaRoot> batchQueue;

  /** Child allocator for producer batches (shares root with consumer for buffer transfer). */
  private final BufferAllocator producerAllocator;

  /** Current batch being filled by producer. */
  private VectorSchemaRoot currentBatch;

  /** Arrow writer for current batch. */
  private org.lance.spark.arrow.LanceArrowWriter currentArrowWriter;

  /** Count of rows in current batch. */
  private final AtomicInteger currentBatchRowCount = new AtomicInteger(0);

  /** Flag indicating producer has finished. */
  private volatile boolean producerFinished = false;

  /** Flag indicating consumer has consumed all batches. */
  private volatile boolean consumerFinished = false;

  /** Current batch being read by consumer (for ArrowReader interface). */
  private VectorSchemaRoot consumerCurrentBatch;

  public QueuedArrowBatchWriteBuffer(
      BufferAllocator allocator, Schema schema, StructType sparkSchema, int batchSize) {
    this(allocator, schema, sparkSchema, batchSize, DEFAULT_QUEUE_DEPTH);
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public QueuedArrowBatchWriteBuffer(StructType sparkSchema, int batchSize) {
    this(sparkSchema, batchSize, DEFAULT_QUEUE_DEPTH);
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public QueuedArrowBatchWriteBuffer(StructType sparkSchema, int batchSize, int queueDepth) {
    this(
        LanceRuntime.allocator(),
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false),
        sparkSchema,
        batchSize,
        queueDepth);
  }

  public QueuedArrowBatchWriteBuffer(
      BufferAllocator allocator,
      Schema schema,
      StructType sparkSchema,
      int batchSize,
      int queueDepth) {
    super(allocator);
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(batchSize > 0, "Batch size must be positive");
    Preconditions.checkArgument(queueDepth > 0, "Queue depth must be positive");

    this.schema = schema;
    this.sparkSchema = sparkSchema;
    this.batchSize = batchSize;
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
    currentBatch = VectorSchemaRoot.create(schema, producerAllocator);
    currentBatch.allocateNew();
    currentArrowWriter =
        org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(currentBatch, sparkSchema);
    currentBatchRowCount.set(0);
  }

  /**
   * Writes a row to the current batch. When the batch is full, it is queued for consumption and a
   * new batch is allocated.
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

    try {
      currentArrowWriter.write(row);

      int count = currentBatchRowCount.incrementAndGet();

      if (count >= batchSize) {
        // Batch is full - finalize and queue it
        currentArrowWriter.finish();
        currentBatch.setRowCount(count);

        // Put in queue (blocks only if queue is full)
        batchQueue.put(currentBatch);

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
    producerFinished = true;

    try {
      // Queue any remaining partial batch
      int remainingRows = currentBatchRowCount.get();
      if (remainingRows > 0) {
        currentArrowWriter.finish();
        currentBatch.setRowCount(remainingRows);
        batchQueue.put(currentBatch);
      } else {
        // No remaining rows, close the empty batch
        currentBatch.close();
      }
      currentBatch = null;
      currentArrowWriter = null;
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
      if (consumerCurrentBatch != null) {
        consumerCurrentBatch.close();
        consumerCurrentBatch = null;
      }

      // Try to get next batch from queue
      while (true) {
        // Use poll with timeout to check for producer finished
        VectorSchemaRoot batch = batchQueue.poll(100, TimeUnit.MILLISECONDS);

        if (batch != null) {
          consumerCurrentBatch = batch;
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
    if (consumerCurrentBatch != null) {
      return consumerCurrentBatch;
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
    if (consumerCurrentBatch != null) {
      consumerCurrentBatch.close();
      consumerCurrentBatch = null;
    }

    // Drain and close any batches left in queue
    VectorSchemaRoot batch;
    while ((batch = batchQueue.poll()) != null) {
      batch.close();
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
