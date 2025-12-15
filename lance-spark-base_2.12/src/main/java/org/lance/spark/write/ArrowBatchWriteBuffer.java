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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Abstract base class for Arrow batch write buffers that bridge Spark row writing and Lance
 * fragment creation.
 *
 * <p>Both {@link SemaphoreArrowBatchWriteBuffer} (semaphore-based) and {@link
 * QueuedArrowBatchWriteBuffer} (queue-based) extend this class, allowing the write path to be
 * configured at runtime.
 */
public abstract class ArrowBatchWriteBuffer extends ArrowReader {

  protected ArrowBatchWriteBuffer(BufferAllocator allocator) {
    super(allocator);
  }

  /**
   * Writes a single row to the buffer.
   *
   * @param row the row to write
   */
  public abstract void write(InternalRow row);

  /** Signals that writing is complete. Any buffered data should be flushed. */
  public abstract void setFinished();
}
