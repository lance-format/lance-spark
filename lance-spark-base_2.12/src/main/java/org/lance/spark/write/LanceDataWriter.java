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
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.sharding.SparkLanceShardingUtils;
import org.lance.spark.utils.BlobReferenceResolver;
import org.lance.spark.utils.BlobSourceContext;
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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

  /**
   * Resolves blob references to actual bytes during writes. Shared across all batches/fragments of
   * this write task and closed at teardown to release the source datasets it opens. Null when blob
   * resolution is not needed (e.g. the test-only constructor).
   */
  private final BlobReferenceResolver blobResolver;

  private ArrowBatchWriteBuffer writeBuffer;
  private FutureTask<List<FragmentMetadata>> fragmentCreationTask;
  private Thread fragmentCreationThread;
  private Object lastKey;
  private boolean hasRowsInCurrentFragment;

  public LanceDataWriter(
      ArrowBatchWriteBuffer writeBuffer,
      FutureTask<List<FragmentMetadata>> fragmentCreationTask,
      Thread fragmentCreationThread) {
    this(writeBuffer, fragmentCreationTask, fragmentCreationThread, null, null, null);
  }

  LanceDataWriter(
      ArrowBatchWriteBuffer writeBuffer,
      FutureTask<List<FragmentMetadata>> fragmentCreationTask,
      Thread fragmentCreationThread,
      Supplier<BufferAndTask> bufferTaskFactory,
      ShardingBatchKeyEvaluator shardingKeyEvaluator,
      BlobReferenceResolver blobResolver) {
    this.writeBuffer = writeBuffer;
    this.fragmentCreationThread = fragmentCreationThread;
    this.fragmentCreationTask = fragmentCreationTask;
    this.bufferTaskFactory = bufferTaskFactory;
    this.shardingKeyEvaluator = shardingKeyEvaluator;
    this.blobResolver = blobResolver;
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
      try {
        writeBuffer.close();
      } finally {
        // Release any source datasets opened to resolve blob references. Spark always calls close()
        // (after commit, and after abort), so this is the single teardown point for the resolver.
        if (blobResolver != null) {
          blobResolver.close();
        }
      }
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
    private final ShardingSpecSnapshot shardingSpec;

    /**
     * Per-source blob credential/open contexts keyed by source dataset URI, captured on the driver
     * (see {@code LanceBlobSourceContextRule}). Used by the per-task resolver to reopen source
     * datasets and fetch blob bytes for references that flowed through the shuffle. Empty when no
     * blob sources were detected or the SQL extension that captures them is not enabled.
     */
    private final Map<String, BlobSourceContext> blobSourceContexts;

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
          null,
          Collections.emptyMap());
    }

    public WriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        ShardingSpec shardingSpec,
        Map<String, BlobSourceContext> blobSourceContexts) {
      // Everything passed to writer factory should be serializable
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.shardingSpec =
          SparkLanceShardingUtils.isEmpty(shardingSpec)
              ? null
              : ShardingSpecSnapshot.from(shardingSpec);
      this.blobSourceContexts =
          blobSourceContexts == null ? Collections.emptyMap() : blobSourceContexts;
    }

    private BufferAndTask buildBufferAndTask(BlobReferenceResolver resolver) {
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
                schema, batchSize, queueDepth, useLargeVarTypes, maxBatchBytes, resolver);
      } else {
        writeBuffer =
            new SemaphoreArrowBatchWriteBuffer(
                schema, batchSize, useLargeVarTypes, maxBatchBytes, resolver);
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
          shardingSpec == null
              ? null
              : new ShardingBatchKeyEvaluator(schema, writeOptions, shardingBinding());

      // One resolver per write task, shared across all batches and fragments (rolled by
      // bufferTaskFactory) and closed when the LanceDataWriter is closed. Always created so blob
      // references can be resolved even without captured contexts (it falls back to open-by-URI).
      BlobReferenceResolver resolver = new BlobReferenceResolver(blobSourceContexts);
      BufferAndTask initial = buildBufferAndTask(resolver);
      initial.thread.start();

      return new LanceDataWriter(
          initial.buffer,
          initial.task,
          initial.thread,
          () -> buildBufferAndTask(resolver),
          shardingKeyEvaluator,
          resolver);
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
              details.get().shardingSpecs().get(0), dataset.getLanceSchema());
        }
      } catch (RuntimeException e) {
        if (shardingSpec.hasSourceIds()) {
          throw e;
        }
        // Staged creates initialize MemWAL after data files are written, so there may not be
        // dataset metadata to read yet. Fall back to an equivalent in-memory sharding binding.
        LOG.warn("Falling back to in-memory sharding metadata for sharded write", e);
      }
      return new ShardingBatchKeyEvaluator.ShardingBinding(shardingSpec.toShardingSpec(), null);
    }

    private static final class ShardingSpecSnapshot implements Serializable {
      private static final long serialVersionUID = 1L;

      private final int specId;
      private final List<ShardingFieldSnapshot> fields;

      private ShardingSpecSnapshot(int specId, List<ShardingFieldSnapshot> fields) {
        this.specId = specId;
        this.fields = fields;
      }

      private static ShardingSpecSnapshot from(ShardingSpec spec) {
        List<ShardingFieldSnapshot> fields = new ArrayList<>();
        for (ShardingField field : spec.fields()) {
          fields.add(ShardingFieldSnapshot.from(field));
        }
        return new ShardingSpecSnapshot(spec.specId(), fields);
      }

      private ShardingSpec toShardingSpec() {
        List<ShardingField> restored = new ArrayList<>();
        for (ShardingFieldSnapshot field : fields) {
          restored.add(field.toShardingField());
        }
        return new ShardingSpec(specId, restored);
      }

      private boolean hasSourceIds() {
        for (ShardingFieldSnapshot field : fields) {
          if (!field.sourceIds.isEmpty()) {
            return true;
          }
        }
        return false;
      }
    }

    private static final class ShardingFieldSnapshot implements Serializable {
      private static final long serialVersionUID = 1L;

      private final String fieldId;
      private final List<Integer> sourceIds;
      private final String transform;
      private final String expression;
      private final String resultType;
      private final Map<String, String> parameters;

      private ShardingFieldSnapshot(
          String fieldId,
          List<Integer> sourceIds,
          String transform,
          String expression,
          String resultType,
          Map<String, String> parameters) {
        this.fieldId = fieldId;
        this.sourceIds = sourceIds;
        this.transform = transform;
        this.expression = expression;
        this.resultType = resultType;
        this.parameters = parameters;
      }

      private static ShardingFieldSnapshot from(ShardingField field) {
        return new ShardingFieldSnapshot(
            field.fieldId(),
            new ArrayList<>(field.sourceIds()),
            field.transform().orElse(null),
            field.expression().orElse(null),
            field.resultType(),
            new HashMap<>(field.parameters()));
      }

      private ShardingField toShardingField() {
        return new ShardingField(fieldId, sourceIds, transform, expression, resultType, parameters);
      }
    }
  }
}
