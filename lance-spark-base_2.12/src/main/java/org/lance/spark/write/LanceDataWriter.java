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
import org.lance.io.StorageOptionsProvider;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LanceDataWriter implements DataWriter<InternalRow> {
  private ArrowBatchWriteBuffer writeBuffer;
  private FutureTask<List<FragmentMetadata>> fragmentCreationTask;
  private Thread fragmentCreationThread;

  public LanceDataWriter(
      ArrowBatchWriteBuffer writeBuffer,
      FutureTask<List<FragmentMetadata>> fragmentCreationTask,
      Thread fragmentCreationThread) {
    this.writeBuffer = writeBuffer;
    this.fragmentCreationThread = fragmentCreationThread;
    this.fragmentCreationTask = fragmentCreationTask;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    writeBuffer.write(record);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    writeBuffer.setFinished();

    try {
      List<FragmentMetadata> fragmentMetadata = fragmentCreationTask.get();
      return new LanceBatchWrite.TaskCommit(fragmentMetadata);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for fragment creation thread to finish", e);
    } catch (ExecutionException e) {
      throw new IOException("Exception in fragment creation thread", e);
    }
  }

  @Override
  public void abort() throws IOException {
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

    protected WriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      // Everything passed to writer factory should be serializable
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      int batchSize = writeOptions.getBatchSize();
      boolean useQueuedBuffer = writeOptions.isUseQueuedWriteBuffer();

      // Merge initial storage options with write options
      WriteParams params = buildWriteParams();

      // Select buffer type based on configuration
      ArrowBatchWriteBuffer writeBuffer;
      if (useQueuedBuffer) {
        int queueDepth = writeOptions.getQueueDepth();
        writeBuffer = new QueuedArrowBatchWriteBuffer(schema, batchSize, queueDepth);
      } else {
        writeBuffer = new SemaphoreArrowBatchWriteBuffer(schema, batchSize);
      }

      // Get storage options provider for credential refresh
      StorageOptionsProvider storageOptionsProvider = getStorageOptionsProvider();

      // Create fragment in background thread
      Callable<List<FragmentMetadata>> fragmentCreator =
          () -> {
            try (ArrowArrayStream arrowStream =
                ArrowArrayStream.allocateNew(LanceRuntime.allocator())) {
              Data.exportArrayStream(LanceRuntime.allocator(), writeBuffer, arrowStream);
              return Fragment.create(
                  writeOptions.getDatasetUri(), arrowStream, params, storageOptionsProvider);
            }
          };
      FutureTask<List<FragmentMetadata>> fragmentCreationTask = new FutureTask<>(fragmentCreator);
      Thread fragmentCreationThread = new Thread(fragmentCreationTask);
      fragmentCreationThread.start();

      return new LanceDataWriter(writeBuffer, fragmentCreationTask, fragmentCreationThread);
    }

    private WriteParams buildWriteParams() {
      Map<String, String> merged =
          LanceRuntime.mergeStorageOptions(writeOptions.getStorageOptions(), initialStorageOptions);

      WriteParams.Builder builder = new WriteParams.Builder();
      builder.withMode(writeOptions.getWriteMode());
      if (writeOptions.getMaxRowsPerFile() != null) {
        builder.withMaxRowsPerFile(writeOptions.getMaxRowsPerFile());
      }
      if (writeOptions.getMaxRowsPerGroup() != null) {
        builder.withMaxRowsPerGroup(writeOptions.getMaxRowsPerGroup());
      }
      if (writeOptions.getMaxBytesPerFile() != null) {
        builder.withMaxBytesPerFile(writeOptions.getMaxBytesPerFile());
      }
      if (writeOptions.getDataStorageVersion() != null) {
        builder.withDataStorageVersion(writeOptions.getDataStorageVersion());
      }
      builder.withStorageOptions(merged);
      return builder.build();
    }

    private StorageOptionsProvider getStorageOptionsProvider() {
      return LanceRuntime.getOrCreateStorageOptionsProvider(
          namespaceImpl, namespaceProperties, tableId);
    }
  }
}
