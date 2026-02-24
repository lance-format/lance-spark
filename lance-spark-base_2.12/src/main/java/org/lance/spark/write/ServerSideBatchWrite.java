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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.InsertIntoTableRequest;
import org.lance.namespace.model.InsertIntoTableResponse;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * BatchWrite implementation for server-side writes.
 *
 * <p>Instead of writing data directly to object storage, this sends Arrow IPC data to the Lance
 * Namespace API via {@link LanceNamespace#insertIntoTable}, which handles the actual writing on the
 * server side.
 */
public class ServerSideBatchWrite implements BatchWrite {
  private static final Logger logger = LoggerFactory.getLogger(ServerSideBatchWrite.class);

  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
  private final boolean overwrite;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  public ServerSideBatchWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new ServerSideWriterFactory(
        schema, writeOptions, overwrite, namespaceImpl, namespaceProperties, tableId);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    long totalRowsWritten =
        Arrays.stream(messages)
            .map(m -> (ServerSideTaskCommit) m)
            .mapToLong(ServerSideTaskCommit::getRowsWritten)
            .sum();
    logger.info("Server-side write completed. Total rows written: {}", totalRowsWritten);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    logger.warn("Server-side write aborted.");
  }

  @Override
  public String toString() {
    return String.format("ServerSideBatchWrite(tableId=%s)", String.join(".", tableId));
  }

  /** Factory for creating server-side data writers. */
  public static class ServerSideWriterFactory implements DataWriterFactory, Serializable {
    private static final long serialVersionUID = 1L;

    private final StructType schema;
    private final LanceSparkWriteOptions writeOptions;
    private final boolean overwrite;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    public ServerSideWriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        boolean overwrite,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.overwrite = overwrite;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new ServerSideDataWriter(
          schema, writeOptions, overwrite, namespaceImpl, namespaceProperties, tableId);
    }
  }

  /** Data writer that buffers rows and sends them to the namespace on commit. */
  public static class ServerSideDataWriter implements DataWriter<InternalRow> {
    private static final Logger logger = LoggerFactory.getLogger(ServerSideDataWriter.class);

    private final StructType sparkSchema;
    private final LanceSparkWriteOptions writeOptions;
    private final boolean overwrite;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;
    private final int batchSize;

    private VectorSchemaRoot root;
    private org.lance.spark.arrow.LanceArrowWriter arrowWriter;
    private ByteArrayOutputStream outputStream;
    private ArrowStreamWriter ipcWriter;
    private int rowCount = 0;
    private long totalRowsWritten = 0;
    private boolean closed = false;

    public ServerSideDataWriter(
        StructType sparkSchema,
        LanceSparkWriteOptions writeOptions,
        boolean overwrite,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this.sparkSchema = sparkSchema;
      this.writeOptions = writeOptions;
      this.overwrite = overwrite;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.batchSize = writeOptions.getBatchSize();

      initializeBuffer();
    }

    private void initializeBuffer() {
      BufferAllocator allocator = LanceRuntime.allocator();
      Schema arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false);
      root = VectorSchemaRoot.create(arrowSchema, allocator);
      arrowWriter = org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(root, sparkSchema);
      outputStream = new ByteArrayOutputStream();

      try {
        ipcWriter = new ArrowStreamWriter(root, null, outputStream);
        ipcWriter.start();
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize Arrow IPC writer", e);
      }
    }

    @Override
    public void write(InternalRow record) throws IOException {
      arrowWriter.write(record);
      rowCount++;

      if (rowCount >= batchSize) {
        flushBatch();
      }
    }

    private void flushBatch() throws IOException {
      if (rowCount > 0) {
        arrowWriter.finish();
        ipcWriter.writeBatch();
        totalRowsWritten += rowCount;
        rowCount = 0;
        root.clear();
        arrowWriter = org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(root, sparkSchema);
      }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      flushBatch();
      ipcWriter.end();

      byte[] arrowIpcData = outputStream.toByteArray();

      if (arrowIpcData.length > 0 && totalRowsWritten > 0) {
        sendToNamespace(arrowIpcData);
      }

      close();
      return new ServerSideTaskCommit(totalRowsWritten);
    }

    private void sendToNamespace(byte[] arrowIpcData) throws IOException {
      LanceNamespace namespace =
          LanceNamespace.connect(namespaceImpl, namespaceProperties, LanceRuntime.allocator());

      String mode = overwrite ? "overwrite" : "append";
      InsertIntoTableRequest request = new InsertIntoTableRequest().id(tableId).mode(mode);

      try {
        InsertIntoTableResponse response = namespace.insertIntoTable(request, arrowIpcData);
        logger.debug(
            "Server-side insert completed for table {}, transaction_id: {}",
            tableId,
            response.getTransactionId());
      } catch (UnsupportedOperationException e) {
        throw new IOException(
            String.format(
                "Namespace implementation '%s' does not support insertIntoTable. "
                    + "Server-side write requires a namespace that implements write operations.",
                namespaceImpl),
            e);
      } catch (Exception e) {
        throw new IOException(
            String.format("Failed to insert data for table %s: %s", tableId, e.getMessage()), e);
      }
    }

    @Override
    public void abort() throws IOException {
      close();
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        if (ipcWriter != null) {
          try {
            ipcWriter.close();
          } catch (Exception e) {
            // Ignore close errors
          }
        }
        if (root != null) {
          root.close();
        }
        if (outputStream != null) {
          outputStream.close();
        }
      }
    }
  }

  /** Commit message for server-side writes. */
  public static class ServerSideTaskCommit implements WriterCommitMessage, Serializable {
    private static final long serialVersionUID = 1L;
    private final long rowsWritten;

    public ServerSideTaskCommit(long rowsWritten) {
      this.rowsWritten = rowsWritten;
    }

    public long getRowsWritten() {
      return rowsWritten;
    }
  }
}
