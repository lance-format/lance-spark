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
import org.lance.spark.utils.Utils;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/** Streams fragment-ordered rows into batched Lance column operations. */
public abstract class AbstractBackfillWriter implements DataWriter<InternalRow> {
  private final LanceSparkWriteOptions writeOptions;
  private final int fragmentIdField;
  private final StructType writerSchema;
  private final int[] fieldIndices;
  private final GenericInternalRow projectedRow;
  private final Map<String, String> initialStorageOptions;

  private Dataset dataset;
  private int currentFragmentId;
  private ArrowBatchWriteBuffer writeBuffer;
  private FutureTask<Void> fragmentTask;
  private Thread fragmentThread;

  protected AbstractBackfillWriter(
      LanceSparkWriteOptions writeOptions,
      StructType schema,
      List<String> targetColumns,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.writeOptions = writeOptions;
    this.fragmentIdField = schema.fieldIndex(LanceDataset.FRAGMENT_ID_COLUMN.name());
    this.initialStorageOptions = initialStorageOptions;

    StructType ws = new StructType();
    for (org.apache.spark.sql.types.StructField f : schema.fields()) {
      if (targetColumns.contains(f.name())
          || f.name().equals(LanceDataset.ROW_ADDRESS_COLUMN.name())) {
        ws = ws.add(f);
      }
    }
    this.writerSchema = ws;
    this.fieldIndices = Arrays.stream(ws.fieldNames()).mapToInt(schema::fieldIndex).toArray();
    this.projectedRow = new GenericInternalRow(fieldIndices.length);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    int fragId = record.getInt(fragmentIdField);
    if (writeBuffer != null && fragId != currentFragmentId) {
      finishFragment();
    }
    if (writeBuffer == null) {
      startFragment(fragId);
    }

    for (int i = 0; i < fieldIndices.length; i++) {
      projectedRow.update(i, record.get(fieldIndices[i], writerSchema.fields()[i].dataType()));
    }
    writeBuffer.write(projectedRow);
  }

  private void startFragment(int fragmentId) {
    if (dataset == null) {
      dataset =
          Utils.openDatasetBuilder(writeOptions)
              .initialStorageOptions(initialStorageOptions)
              .build();
    }
    currentFragmentId = fragmentId;
    writeBuffer =
        new SemaphoreArrowBatchWriteBuffer(
            writerSchema,
            writeOptions.getBatchSize(),
            false,
            writeOptions.getMaxBatchBytes(),
            null);
    ArrowBatchWriteBuffer buffer = writeBuffer;
    Fragment fragment = new Fragment(dataset, fragmentId);
    fragmentTask =
        buffer.createTrackedTask(
            () -> {
              try (ArrowArrayStream stream =
                  ArrowArrayStream.allocateNew(LanceRuntime.allocator())) {
                Data.exportArrayStream(LanceRuntime.allocator(), buffer, stream);
                processFragment(fragment, stream);
              }
              return null;
            });
    fragmentThread = new Thread(fragmentTask, "lance-backfill-" + fragmentId);
    fragmentThread.setDaemon(true);
    fragmentThread.start();
  }

  private void finishFragment() throws IOException {
    if (writeBuffer == null) {
      return;
    }
    writeBuffer.setFinished();
    try {
      fragmentTask.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for column backfill", e);
    } catch (ExecutionException e) {
      throw new IOException("Failed to backfill fragment " + currentFragmentId, e.getCause());
    }
    closeFragmentBuffer();
  }

  private void closeFragmentBuffer() throws IOException {
    ArrowBatchWriteBuffer buffer = writeBuffer;
    writeBuffer = null;
    fragmentTask = null;
    fragmentThread = null;
    buffer.close();
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
    finishFragment();
    return buildCommitMessage();
  }

  @Override
  public void abort() throws IOException {
    close();
  }

  @Override
  public void close() throws IOException {
    try {
      if (writeBuffer != null) {
        writeBuffer.setFinished();
        fragmentThread.interrupt();
        // Native code must release exported Arrow buffers before their allocator is closed.
        Uninterruptibles.joinUninterruptibly(fragmentThread);
        closeFragmentBuffer();
      }
    } finally {
      if (dataset != null) {
        dataset.close();
        dataset = null;
      }
    }
  }
}
