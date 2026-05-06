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

import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class LanceDataWriter implements DataWriter<InternalRow> {
  private final Supplier<BufferAndTask> bufferTaskFactory;
  private final int[] partitionColumnIndices;
  private final DataType[] partitionColumnTypes;
  private final List<FragmentMetadata> completedFragments = new ArrayList<>();

  private ArrowBatchWriteBuffer writeBuffer;
  private FutureTask<List<FragmentMetadata>> fragmentCreationTask;
  private Thread fragmentCreationThread;
  private Object[] lastKey;
  private boolean hasRowsInCurrentFragment;

  public LanceDataWriter(
      ArrowBatchWriteBuffer writeBuffer,
      FutureTask<List<FragmentMetadata>> fragmentCreationTask,
      Thread fragmentCreationThread) {
    this(
        writeBuffer,
        fragmentCreationTask,
        fragmentCreationThread,
        null,
        new int[0],
        new DataType[0]);
  }

  LanceDataWriter(
      ArrowBatchWriteBuffer writeBuffer,
      FutureTask<List<FragmentMetadata>> fragmentCreationTask,
      Thread fragmentCreationThread,
      Supplier<BufferAndTask> bufferTaskFactory,
      int[] partitionColumnIndices,
      DataType[] partitionColumnTypes) {
    this.writeBuffer = writeBuffer;
    this.fragmentCreationThread = fragmentCreationThread;
    this.fragmentCreationTask = fragmentCreationTask;
    this.bufferTaskFactory = bufferTaskFactory;
    this.partitionColumnIndices = partitionColumnIndices;
    this.partitionColumnTypes = partitionColumnTypes;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    if (partitionColumnIndices.length > 0) {
      if (!hasRowsInCurrentFragment) {
        captureKey(record);
      } else if (!rowMatchesLastKey(record)) {
        rollFragment();
        captureKey(record);
      }
    }
    writeBuffer.write(record);
    hasRowsInCurrentFragment = true;
  }

  /** Compares the row's partition values against {@link #lastKey} without allocating. */
  private boolean rowMatchesLastKey(InternalRow row) {
    for (int k = 0; k < partitionColumnIndices.length; k++) {
      int idx = partitionColumnIndices[k];
      Object prev = lastKey[k];
      if (row.isNullAt(idx)) {
        if (prev != null) return false;
      } else {
        if (prev == null) return false;
        if (!prev.equals(row.get(idx, partitionColumnTypes[k]))) return false;
      }
    }
    return true;
  }

  /**
   * Snapshots the row's partition values into {@link #lastKey}. Only called on the first row of a
   * fragment, so the per-row hot path stays allocation-free.
   */
  private void captureKey(InternalRow row) {
    if (lastKey == null) {
      lastKey = new Object[partitionColumnIndices.length];
    }
    for (int k = 0; k < partitionColumnIndices.length; k++) {
      int idx = partitionColumnIndices[k];
      if (row.isNullAt(idx)) {
        lastKey[k] = null;
      } else {
        Object value = row.get(idx, partitionColumnTypes[k]);
        // UTF8String wraps a pointer into the row buffer, which may be reused
        // across calls — snapshot the bytes so the key stays stable.
        lastKey[k] = value instanceof UTF8String ? ((UTF8String) value).clone() : value;
      }
    }
  }

  /**
   * Closes the current fragment stream, collects its metadata, and spins up a fresh buffer + task
   * so subsequent rows land in a new fragment. Called at each partition-value transition.
   */
  private void rollFragment() throws IOException {
    writeBuffer.setFinished();
    try {
      completedFragments.addAll(fragmentCreationTask.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while rolling fragment on partition boundary", e);
    } catch (ExecutionException e) {
      throw new IOException("Exception rolling fragment on partition boundary", e);
    }
    writeBuffer.close();

    BufferAndTask next = bufferTaskFactory.get();
    this.writeBuffer = next.buffer;
    this.fragmentCreationTask = next.task;
    this.fragmentCreationThread = next.thread;
    this.fragmentCreationThread.start();
    this.hasRowsInCurrentFragment = false;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    writeBuffer.setFinished();

    try {
      completedFragments.addAll(fragmentCreationTask.get());
      return new LanceBatchWrite.TaskCommit(new ArrayList<>(completedFragments));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for fragment creation thread to finish", e);
    } catch (ExecutionException e) {
      throw new IOException("Exception in fragment creation thread", e);
    }
  }

  @Override
  public void abort() throws IOException {
    // Signal the write buffer that no more data will be produced.
    // This unblocks the fragment creation thread's consumer side
    // (which reads from the buffer via ArrowReader interface),
    // preventing a deadlock where the consumer waits for more data
    // while we wait for the consumer to finish.
    writeBuffer.setFinished();
    fragmentCreationThread.interrupt();
    try {
      // Have a timeout to avoid hanging in native method indefinitely
      fragmentCreationTask.get(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for fragment creation thread to finish", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IOException("Failed to abort the fragment creation thread", e);
    }
    close();
  }

  @Override
  public void close() throws IOException {
    writeBuffer.close();
  }

  /** A freshly-constructed buffer paired with the Fragment.create task that consumes from it. */
  static final class BufferAndTask {
    final ArrowBatchWriteBuffer buffer;
    final FutureTask<List<FragmentMetadata>> task;
    final Thread thread;

    BufferAndTask(
        ArrowBatchWriteBuffer buffer, FutureTask<List<FragmentMetadata>> task, Thread thread) {
      this.buffer = buffer;
      this.task = task;
      this.thread = thread;
    }
  }

  public static class WriterFactory implements DataWriterFactory {
    private final LanceSparkWriteOptions writeOptions;
    private final StructType schema;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;
    private final List<String> partitionColumns;

    public WriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this(
          schema,
          writeOptions,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId,
          Collections.emptyList());
    }

    public WriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        List<String> partitionColumns) {
      // Everything passed to writer factory should be serializable
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.partitionColumns = partitionColumns == null ? Collections.emptyList() : partitionColumns;
    }

    private BufferAndTask buildBufferAndTask() {
      int batchSize = writeOptions.getBatchSize();
      boolean useQueuedBuffer = writeOptions.isUseQueuedWriteBuffer();
      boolean useLargeVarTypes = writeOptions.isUseLargeVarTypes();
      long maxBatchBytes = writeOptions.getMaxBatchBytes();

      // Merge initial storage options with write options
      WriteParams params = writeOptions.toWriteParams(initialStorageOptions);

      ArrowBatchWriteBuffer writeBuffer;
      if (useQueuedBuffer) {
        int queueDepth = writeOptions.getQueueDepth();
        writeBuffer =
            new QueuedArrowBatchWriteBuffer(
                schema, batchSize, queueDepth, useLargeVarTypes, maxBatchBytes);
      } else {
        writeBuffer =
            new SemaphoreArrowBatchWriteBuffer(schema, batchSize, useLargeVarTypes, maxBatchBytes);
      }

      final ArrowBatchWriteBuffer bufferRef = writeBuffer;
      Callable<List<FragmentMetadata>> fragmentCreator =
          () -> {
            try (ArrowArrayStream arrowStream =
                ArrowArrayStream.allocateNew(LanceRuntime.allocator())) {
              Data.exportArrayStream(LanceRuntime.allocator(), bufferRef, arrowStream);
              return Fragment.create(writeOptions.getDatasetUri(), arrowStream, params);
            }
          };
      FutureTask<List<FragmentMetadata>> fragmentCreationTask =
          writeBuffer.createTrackedTask(fragmentCreator);
      Thread fragmentCreationThread = new Thread(fragmentCreationTask);
      return new BufferAndTask(writeBuffer, fragmentCreationTask, fragmentCreationThread);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      BufferAndTask initial = buildBufferAndTask();
      initial.thread.start();

      int[] indices = resolvePartitionColumnIndices();
      DataType[] types = resolvePartitionColumnTypes(indices);

      return new LanceDataWriter(
          initial.buffer, initial.task, initial.thread, this::buildBufferAndTask, indices, types);
    }

    private int[] resolvePartitionColumnIndices() {
      if (partitionColumns.isEmpty()) {
        return new int[0];
      }
      String[] names = schema.fieldNames();
      int[] indices = new int[partitionColumns.size()];
      for (int i = 0; i < partitionColumns.size(); i++) {
        String col = partitionColumns.get(i);
        int found = -1;
        for (int j = 0; j < names.length; j++) {
          if (names[j].equals(col)) {
            found = j;
            break;
          }
        }
        if (found < 0) {
          throw new IllegalArgumentException(
              "Partition column not found in schema: "
                  + col
                  + " (available: "
                  + Arrays.toString(names)
                  + ")");
        }
        indices[i] = found;
      }
      return indices;
    }

    private DataType[] resolvePartitionColumnTypes(int[] indices) {
      StructField[] fields = schema.fields();
      DataType[] types = new DataType[indices.length];
      for (int i = 0; i < indices.length; i++) {
        types[i] = fields[indices[i]].dataType();
      }
      return types;
    }
  }
}
