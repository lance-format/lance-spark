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
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.BlobReferenceResolver;
import org.lance.spark.utils.BlobSourceContext;
import org.lance.spark.utils.Utils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Abstract base class for backfill writers that stream rows for each fragment through bounded Arrow
 * record batches and process the stream via a Lance fragment operation (merge or update).
 *
 * <p>Subclasses implement {@link #processFragment} to perform the specific operation and {@link
 * #buildCommitMessage} to construct the appropriate commit message.
 */
public abstract class AbstractBackfillWriter implements DataWriter<InternalRow> {
  private final LanceSparkWriteOptions writeOptions;
  private final StructType schema;
  private final int fragmentIdField;
  private final StructType writerSchema;
  private final int[] writerInputOrdinals;

  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;
  private final BlobReferenceResolver blobResolver;
  private Dataset dataset;
  private FragmentStream currentStream;
  private Integer lastCompletedFragmentId;

  private static class FragmentStream {
    final int fragmentId;
    final SemaphoreArrowBatchWriteBuffer buffer;
    final FutureTask<Void> task;

    FragmentStream(int fragmentId, SemaphoreArrowBatchWriteBuffer buffer, FutureTask<Void> task) {
      this.fragmentId = fragmentId;
      this.buffer = buffer;
      this.task = task;
    }
  }

  protected AbstractBackfillWriter(
      LanceSparkWriteOptions writeOptions,
      StructType schema,
      List<String> targetColumns,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this.writeOptions = writeOptions;
    this.schema = schema;
    this.fragmentIdField = schema.fieldIndex(LanceDataset.FRAGMENT_ID_COLUMN.name());
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.blobResolver = new BlobReferenceResolver(blobSourceContexts);

    StructType ws = new StructType();
    for (org.apache.spark.sql.types.StructField f : schema.fields()) {
      if (targetColumns.contains(f.name())
          || f.name().equals(LanceDataset.ROW_ADDRESS_COLUMN.name())) {
        ws = ws.add(f);
      }
    }
    this.writerSchema = ws;
    this.writerInputOrdinals =
        Arrays.stream(writerSchema.fields())
            .mapToInt(field -> schema.fieldIndex(field.name()))
            .toArray();
  }

  @Override
  public void write(InternalRow record) throws IOException {
    int fragId = record.getInt(fragmentIdField);
    if (currentStream == null || currentStream.fragmentId != fragId) {
      finishCurrentFragment();
      if (lastCompletedFragmentId != null && fragId <= lastCompletedFragmentId) {
        throw new IOException(
            "Backfill rows must be ordered by "
                + LanceDataset.FRAGMENT_ID_COLUMN.name()
                + "; saw fragment "
                + fragId
                + " after fragment "
                + lastCompletedFragmentId);
      }
      currentStream = startFragment(fragId);
    }
    currentStream.buffer.write(record);
  }

  private FragmentStream startFragment(int fragmentId) {
    if (dataset == null) {
      dataset =
          Utils.openDatasetBuilder(writeOptions)
              .initialStorageOptions(initialStorageOptions)
              .build();
    }
    BufferAllocator allocator = LanceRuntime.allocator();
    SemaphoreArrowBatchWriteBuffer buffer =
        new SemaphoreArrowBatchWriteBuffer(
            allocator,
            LanceArrowUtils.toArrowSchema(writerSchema, "UTC", false),
            writerSchema,
            writeOptions.getBatchSize(),
            writeOptions.getMaxBatchBytes(),
            blobResolver,
            writerInputOrdinals);
    FutureTask<Void> task =
        buffer.createTrackedTask(
            () -> {
              try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
                Data.exportArrayStream(allocator, buffer, stream);
                processFragment(new Fragment(dataset, fragmentId), stream);
              }
              return null;
            });
    Thread thread = new Thread(task, "lance-backfill-fragment-" + fragmentId);
    thread.start();
    return new FragmentStream(fragmentId, buffer, task);
  }

  private void finishCurrentFragment() throws IOException {
    if (currentStream == null) {
      return;
    }
    FragmentStream stream = currentStream;
    currentStream = null;
    stream.buffer.setFinished();
    try {
      stream.task.get();
      lastCompletedFragmentId = stream.fragmentId;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while backfilling fragment " + stream.fragmentId, e);
    } catch (ExecutionException e) {
      throw new IOException("Failed to backfill fragment " + stream.fragmentId, e.getCause());
    } finally {
      stream.buffer.close();
    }
  }

  /**
   * Process a single fragment's buffered data. Subclasses call the appropriate Lance fragment
   * operation (e.g. mergeColumns or updateColumns) and store the results.
   */
  protected abstract void processFragment(Fragment fragment, ArrowArrayStream stream);

  /** Build the commit message from accumulated results after all fragments have been processed. */
  protected abstract WriterCommitMessage buildCommitMessage();

  @Override
  public WriterCommitMessage commit() throws IOException {
    finishCurrentFragment();
    closeDataset();
    return buildCommitMessage();
  }

  @Override
  public void abort() throws IOException {
    finishCurrentFragment();
    closeDataset();
  }

  @Override
  public void close() throws IOException {
    try {
      finishCurrentFragment();
    } finally {
      try {
        closeDataset();
      } finally {
        blobResolver.close();
      }
    }
  }

  private void closeDataset() {
    if (dataset != null) {
      dataset.close();
      dataset = null;
    }
  }
}
