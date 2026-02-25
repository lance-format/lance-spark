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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffers Spark rows into Arrow batches for consumption by Lance fragment creation.
 *
 * <p>This class bridges the producer (Spark thread writing rows) and consumer (Lance native code
 * pulling batches via ArrowReader interface). It uses semaphores to synchronize between the two
 * threads - the producer blocks until the consumer is ready for more data, and vice versa.
 *
 * @see QueuedArrowBatchWriteBuffer for a queue-based alternative with better pipelining
 */
public class SemaphoreArrowBatchWriteBuffer extends ArrowBatchWriteBuffer {
  private final Schema schema;
  private final StructType sparkSchema;
  private final int batchSize;

  @GuardedBy("monitor")
  private volatile boolean finished = false;

  private org.lance.spark.arrow.LanceArrowWriter arrowWriter = null;
  private final AtomicInteger count = new AtomicInteger(0);
  private final Semaphore writeToken;
  private final Semaphore loadToken;

  public SemaphoreArrowBatchWriteBuffer(
      BufferAllocator allocator, Schema schema, StructType sparkSchema, int batchSize) {
    super(allocator);
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(batchSize > 0);
    this.schema = schema;
    this.sparkSchema = sparkSchema;
    this.batchSize = batchSize;
    this.writeToken = new Semaphore(0);
    this.loadToken = new Semaphore(0);
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public SemaphoreArrowBatchWriteBuffer(StructType sparkSchema, int batchSize) {
    this(
        LanceRuntime.allocator(),
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false),
        sparkSchema,
        batchSize);
  }

  @Override
  public void write(InternalRow row) {
    Preconditions.checkNotNull(row);
    try {
      // wait util prepareLoadNextBatch to release write token
      writeToken.acquire();

      // Write to Arrow buffer
      arrowWriter.write(row);

      if (count.incrementAndGet() == batchSize) {
        // notify loadNextBatch to take the batch
        loadToken.release();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setFinished() {
    finished = true;
    loadToken.release();
  }

  @Override
  public void prepareLoadNextBatch() throws IOException {
    super.prepareLoadNextBatch();
    arrowWriter =
        org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(
            this.getVectorSchemaRoot(), sparkSchema);
    // release batch size token for write
    writeToken.release(batchSize);
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    try {
      if (finished && count.get() == 0) {
        return false;
      }

      // wait util batch is full or finished
      loadToken.acquire();

      // Finish the batch (flush Arrow vectors)
      arrowWriter.finish();

      if (!finished) {
        count.set(0);
        return true;
      } else {
        // true if it has some rows and return false if there is no record
        if (count.get() > 0) {
          count.set(0);
          return true;
        } else {
          return false;
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long bytesRead() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void closeReadSource() throws IOException {
    // Implement if needed
  }

  @Override
  protected Schema readSchema() {
    return this.schema;
  }
}
