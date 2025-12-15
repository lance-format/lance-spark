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

import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.spark.LanceConfig;
import org.lance.spark.SparkOptions;
import org.lance.spark.internal.LanceDatasetAdapter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;
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
    private final LanceConfig config;
    private final StructType schema;

    protected WriterFactory(StructType schema, LanceConfig config) {
      // Everything passed to writer factory should be serializable
      this.schema = schema;
      this.config = config;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      int batchSize = SparkOptions.getBatchSize(config);
      boolean useQueuedBuffer = SparkOptions.useQueuedWriteBuffer(config);
      WriteParams params = SparkOptions.genWriteParamsFromConfig(config);

      // Select buffer type based on configuration
      ArrowBatchWriteBuffer writeBuffer;
      if (useQueuedBuffer) {
        int queueDepth = SparkOptions.getQueueDepth(config);
        writeBuffer =
            LanceDatasetAdapter.getQueuedArrowBatchWriteBuffer(schema, batchSize, queueDepth);
      } else {
        writeBuffer = LanceDatasetAdapter.getSemaphoreArrowBatchWriteBuffer(schema, batchSize);
      }

      // Create fragment in background thread
      Callable<List<FragmentMetadata>> fragmentCreator =
          () -> LanceDatasetAdapter.createFragment(config.getDatasetUri(), writeBuffer, params);
      FutureTask<List<FragmentMetadata>> fragmentCreationTask = new FutureTask<>(fragmentCreator);
      Thread fragmentCreationThread = new Thread(fragmentCreationTask);
      fragmentCreationThread.start();

      return new LanceDataWriter(writeBuffer, fragmentCreationTask, fragmentCreationThread);
    }
  }
}
