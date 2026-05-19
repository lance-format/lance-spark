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
import org.lance.spark.partition.PartitionTransform;
import org.lance.spark.utils.BucketHashUtil;

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
  private final List<PartitionTransform> partitionSpec;
  private final int[][] specColumnIndices;
  private final DataType[][] specColumnTypes;
  private final List<FragmentMetadata> completedFragments = new ArrayList<>();

  private ArrowBatchWriteBuffer writeBuffer;
  private FutureTask<List<FragmentMetadata>> fragmentCreationTask;
  private Thread fragmentCreationThread;
  private Object lastKey;
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
        Collections.emptyList(),
        new int[0][],
        new DataType[0][]);
  }

  LanceDataWriter(
      ArrowBatchWriteBuffer writeBuffer,
      FutureTask<List<FragmentMetadata>> fragmentCreationTask,
      Thread fragmentCreationThread,
      Supplier<BufferAndTask> bufferTaskFactory,
      List<PartitionTransform> partitionSpec,
      int[][] specColumnIndices,
      DataType[][] specColumnTypes) {
    this.writeBuffer = writeBuffer;
    this.fragmentCreationThread = fragmentCreationThread;
    this.fragmentCreationTask = fragmentCreationTask;
    this.bufferTaskFactory = bufferTaskFactory;
    this.partitionSpec = partitionSpec;
    this.specColumnIndices = specColumnIndices;
    this.specColumnTypes = specColumnTypes;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    if (!partitionSpec.isEmpty()) {
      Object key = computePartitionKey(record);
      if (!hasRowsInCurrentFragment) {
        lastKey = key;
      } else if (!key.equals(lastKey)) {
        rollFragment();
        lastKey = key;
      }
    }
    writeBuffer.write(record);
    hasRowsInCurrentFragment = true;
  }

  private Object computePartitionKey(InternalRow row) {
    if (partitionSpec.size() == 1) {
      return computeSingleKey(row, 0);
    }
    Object[] keys = new Object[partitionSpec.size()];
    for (int i = 0; i < partitionSpec.size(); i++) {
      keys[i] = computeSingleKey(row, i);
    }
    return Arrays.asList(keys);
  }

  private Object computeSingleKey(InternalRow row, int specIdx) {
    PartitionTransform t = partitionSpec.get(specIdx);
    int[] colIndices = specColumnIndices[specIdx];
    DataType[] colTypes = specColumnTypes[specIdx];

    if (t instanceof PartitionTransform.Bucket) {
      int numBuckets = ((PartitionTransform.Bucket) t).getNumBuckets();
      return BucketHashUtil.computeBucketId(row, colIndices, colTypes, numBuckets);
    }
    // Identity: use the raw column value
    int idx = colIndices[0];
    if (row.isNullAt(idx)) {
      return null;
    }
    Object value = row.get(idx, colTypes[0]);
    return value instanceof UTF8String ? ((UTF8String) value).clone() : value;
  }

  private void rollFragment() throws IOException {
    writeBuffer.setFinished();
    try {
      completedFragments.addAll(stripRowIdMeta(fragmentCreationTask.get()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while rolling fragment", e);
    } catch (ExecutionException e) {
      throw new IOException("Exception rolling fragment", e);
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
      completedFragments.addAll(stripRowIdMeta(fragmentCreationTask.get()));
      return new LanceBatchWrite.TaskCommit(new ArrayList<>(completedFragments));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for fragment creation", e);
    } catch (ExecutionException e) {
      throw new IOException("Exception in fragment creation thread", e);
    }
  }

  @Override
  public void abort() throws IOException {
    writeBuffer.setFinished();
    fragmentCreationThread.interrupt();
    try {
      fragmentCreationTask.get(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for fragment creation", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IOException("Failed to abort fragment creation", e);
    }
    close();
  }

  @Override
  public void close() throws IOException {
    writeBuffer.close();
  }

  static List<FragmentMetadata> stripRowIdMeta(List<FragmentMetadata> fragments) {
    List<FragmentMetadata> stripped = new ArrayList<>(fragments.size());
    for (FragmentMetadata fragment : fragments) {
      stripped.add(
          new FragmentMetadata(
              fragment.getId(),
              fragment.getFiles(),
              fragment.getPhysicalRows(),
              fragment.getDeletionFile(),
              null,
              fragment.getCreatedAtVersionMeta(),
              fragment.getLastUpdatedAtVersionMeta()));
    }
    return stripped;
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
    private final Map<String, String> initialStorageOptions;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;
    private final List<PartitionTransform> partitionSpec;

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
        List<PartitionTransform> partitionSpec) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.partitionSpec = partitionSpec == null ? Collections.emptyList() : partitionSpec;
    }

    private BufferAndTask buildBufferAndTask() {
      int batchSize = writeOptions.getBatchSize();
      boolean useQueuedBuffer = writeOptions.isUseQueuedWriteBuffer();
      boolean useLargeVarTypes = writeOptions.isUseLargeVarTypes();
      long maxBatchBytes = writeOptions.getMaxBatchBytes();

      LanceSparkWriteOptions fragmentWriteOptions =
          writeOptions.toBuilder().enableStableRowIds(false).build();
      WriteParams params = fragmentWriteOptions.toWriteParams(initialStorageOptions);

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
      FutureTask<List<FragmentMetadata>> task = writeBuffer.createTrackedTask(fragmentCreator);
      Thread thread = new Thread(task);
      return new BufferAndTask(writeBuffer, task, thread);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      BufferAndTask initial = buildBufferAndTask();
      initial.thread.start();

      int[][] colIndices = new int[partitionSpec.size()][];
      DataType[][] colTypes = new DataType[partitionSpec.size()][];
      for (int i = 0; i < partitionSpec.size(); i++) {
        String col = partitionSpec.get(i).getCol();
        int[] idx = resolveColumnIndices(Collections.singletonList(col));
        colIndices[i] = idx;
        colTypes[i] = resolveColumnTypes(idx);
      }

      return new LanceDataWriter(
          initial.buffer,
          initial.task,
          initial.thread,
          this::buildBufferAndTask,
          partitionSpec,
          colIndices,
          colTypes);
    }

    private int[] resolveColumnIndices(List<String> columns) {
      if (columns.isEmpty()) {
        return new int[0];
      }
      String[] names = schema.fieldNames();
      int[] indices = new int[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        String col = columns.get(i);
        int found = -1;
        for (int j = 0; j < names.length; j++) {
          if (names[j].equals(col)) {
            found = j;
            break;
          }
        }
        if (found < 0) {
          throw new IllegalArgumentException(
              "Column not found in schema: "
                  + col
                  + " (available: "
                  + Arrays.toString(names)
                  + ")");
        }
        indices[i] = found;
      }
      return indices;
    }

    private DataType[] resolveColumnTypes(int[] indices) {
      StructField[] fields = schema.fields();
      DataType[] types = new DataType[indices.length];
      for (int i = 0; i < indices.length; i++) {
        types[i] = fields[indices[i]].dataType();
      }
      return types;
    }
  }
}
