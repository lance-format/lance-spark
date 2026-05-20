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

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.memwal.MemWalIndexDetails;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.sharding.SparkLanceShardingAdapter;
import org.lance.spark.utils.Utils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class LanceDataWriter implements DataWriter<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(LanceDataWriter.class);

  private final Supplier<BufferAndTask> bufferTaskFactory;
  private final ShardingBatchKeyEvaluator shardingKeyEvaluator;
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
    this(writeBuffer, fragmentCreationTask, fragmentCreationThread, null, null);
  }

  LanceDataWriter(
      ArrowBatchWriteBuffer writeBuffer,
      FutureTask<List<FragmentMetadata>> fragmentCreationTask,
      Thread fragmentCreationThread,
      Supplier<BufferAndTask> bufferTaskFactory,
      ShardingBatchKeyEvaluator shardingKeyEvaluator) {
    this.writeBuffer = writeBuffer;
    this.fragmentCreationThread = fragmentCreationThread;
    this.fragmentCreationTask = fragmentCreationTask;
    this.bufferTaskFactory = bufferTaskFactory;
    this.shardingKeyEvaluator = shardingKeyEvaluator;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    if (shardingKeyEvaluator != null) {
      shardingKeyEvaluator.write(record, this::writePartitionedRow);
      return;
    }
    writeBuffer.write(record);
    hasRowsInCurrentFragment = true;
  }

  private void writePartitionedRow(InternalRow row, Object key) throws IOException {
    if (!hasRowsInCurrentFragment) {
      lastKey = key;
    } else if (!Objects.equals(key, lastKey)) {
      rollFragment();
      lastKey = key;
    }
    writeBuffer.write(row);
    hasRowsInCurrentFragment = true;
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
    if (shardingKeyEvaluator != null) {
      shardingKeyEvaluator.flush(this::writePartitionedRow);
    }
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
    try {
      if (shardingKeyEvaluator != null) {
        shardingKeyEvaluator.close();
      }
    } finally {
      writeBuffer.close();
    }
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
    private final List<SparkLanceShardingAdapter> partitionSpec;

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
        List<SparkLanceShardingAdapter> partitionSpec) {
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
      ShardingBatchKeyEvaluator shardingKeyEvaluator =
          partitionSpec.isEmpty()
              ? null
              : new ShardingBatchKeyEvaluator(schema, writeOptions, shardingBinding());

      BufferAndTask initial = buildBufferAndTask();
      initial.thread.start();

      return new LanceDataWriter(
          initial.buffer,
          initial.task,
          initial.thread,
          this::buildBufferAndTask,
          shardingKeyEvaluator);
    }

    private ShardingBatchKeyEvaluator.ShardingBinding shardingBinding() {
      try (Dataset dataset =
          Utils.openDatasetBuilder(
                  writeOptions.toBuilder()
                      .storageOptions(
                          LanceRuntime.mergeStorageOptions(
                              writeOptions.getStorageOptions(), initialStorageOptions))
                      .build())
              .build()) {
        Optional<MemWalIndexDetails> details = dataset.memWalIndexDetails();
        if (details.isPresent() && !details.get().shardingSpecs().isEmpty()) {
          return new ShardingBatchKeyEvaluator.ShardingBinding(
              details.get().shardingSpecs().get(0),
              SparkLanceShardingAdapter.sourceIdToColumnMap(dataset));
        }
      } catch (RuntimeException e) {
        // Staged creates initialize MemWAL after data files are written, so there may not be
        // dataset metadata to read yet. Fall back to an equivalent in-memory sharding binding.
        LOG.warn("Falling back to in-memory sharding metadata for partitioned write", e);
      }
      return ShardingBatchKeyEvaluator.ShardingBinding.fromPartitionSpec(partitionSpec);
    }
  }
}
