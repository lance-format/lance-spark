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

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * Abstract base class for Arrow batch write buffers that bridge Spark row writing and Lance
 * fragment creation.
 *
 * <p>Both {@link SemaphoreArrowBatchWriteBuffer} (lock-based) and {@link
 * QueuedArrowBatchWriteBuffer} (queue-based) extend this class, allowing the write path to be
 * configured at runtime.
 */
public abstract class ArrowBatchWriteBuffer extends ArrowReader {

  private volatile Future<?> fragmentCreationTask;

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

  /**
   * Creates a {@link FutureTask} that is tracked by this buffer for error propagation. The returned
   * task's {@link FutureTask#done()} callback calls {@link #onTaskComplete()} so that subclasses
   * can wake blocked writers on task completion or failure.
   */
  public <V> FutureTask<V> createTrackedTask(Callable<V> callable) {
    Preconditions.checkState(
        this.fragmentCreationTask == null, "Fragment creation task already set");
    FutureTask<V> task =
        new FutureTask<>(callable) {
          @Override
          protected void done() {
            onTaskComplete();
          }
        };
    this.fragmentCreationTask = task;
    return task;
  }

  /**
   * Called when the fragment creation task completes. Subclasses override this to wake blocked
   * writer threads.
   */
  protected void onTaskComplete() {
    // No-op by default; subclasses override to signal blocked threads
  }

  /**
   * Checks if the fragment creation task has failed and throws the error if so. The {@link
   * java.util.concurrent.FutureTask} is the single source of truth for errors.
   */
  protected void checkForError() {
    if (fragmentCreationTask != null && fragmentCreationTask.isDone()) {
      try {
        fragmentCreationTask.get();
      } catch (ExecutionException e) {
        throw new RuntimeException("Failed to write data to lance", e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }
}
