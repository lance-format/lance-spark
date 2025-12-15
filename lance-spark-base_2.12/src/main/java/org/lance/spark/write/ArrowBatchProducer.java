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

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

/**
 * Interface for producing Arrow batches from Spark InternalRow data.
 *
 * <p>Both {@link ArrowBatchWriteBuffer} (semaphore-based) and {@link QueuedArrowBatchWriteBuffer}
 * (queue-based) implement this interface, allowing the write path to be configured at runtime.
 */
public interface ArrowBatchProducer {

  /**
   * Writes a single row to the buffer.
   *
   * @param row the row to write
   */
  void write(InternalRow row);

  /** Signals that writing is complete. Any buffered data should be flushed. */
  void setFinished();

  /**
   * Closes the producer and releases resources.
   *
   * @throws IOException if an I/O error occurs
   */
  void close() throws IOException;

  /**
   * Returns this producer as an ArrowReader for fragment creation.
   *
   * @return this producer as an ArrowReader
   */
  ArrowReader asArrowReader();
}
