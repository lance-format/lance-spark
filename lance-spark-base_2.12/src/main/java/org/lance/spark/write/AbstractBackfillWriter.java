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

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for backfill writers that buffer rows per fragment and then process each
 * fragment's data via a Lance fragment operation (merge or update).
 *
 * <p>Subclasses implement {@link #processFragment} to perform the specific operation and {@link
 * #buildCommitMessage} to construct the appropriate commit message.
 */
public abstract class AbstractBackfillWriter implements DataWriter<InternalRow> {
  private final LanceSparkWriteOptions writeOptions;
  private final StructType schema;
  private final int fragmentIdField;
  private final StructType writerSchema;
  private final Map<Integer, FragmentBuffer> buffers = new HashMap<>();

  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  private static class FragmentBuffer {
    final VectorSchemaRoot data;
    final org.lance.spark.arrow.LanceArrowWriter writer;

    FragmentBuffer(VectorSchemaRoot data, org.lance.spark.arrow.LanceArrowWriter writer) {
      this.data = data;
      this.writer = writer;
    }
  }

  protected AbstractBackfillWriter(
      LanceSparkWriteOptions writeOptions,
      StructType schema,
      List<String> targetColumns,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.writeOptions = writeOptions;
    this.schema = schema;
    this.fragmentIdField = schema.fieldIndex(LanceDataset.FRAGMENT_ID_COLUMN.name());
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;

    StructType ws = new StructType();
    for (org.apache.spark.sql.types.StructField f : schema.fields()) {
      if (targetColumns.contains(f.name())
          || f.name().equals(LanceDataset.ROW_ADDRESS_COLUMN.name())) {
        ws = ws.add(f);
      }
    }
    this.writerSchema = ws;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    int fragId = record.getInt(fragmentIdField);

    FragmentBuffer buffer =
        buffers.computeIfAbsent(
            fragId,
            id -> {
              BufferAllocator allocator = LanceRuntime.allocator();
              VectorSchemaRoot data =
                  VectorSchemaRoot.create(
                      LanceArrowUtils.toArrowSchema(writerSchema, "UTC", false), allocator);
              org.lance.spark.arrow.LanceArrowWriter writer =
                  org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(data, writerSchema);
              return new FragmentBuffer(data, writer);
            });

    for (int i = 0; i < writerSchema.fields().length; i++) {
      buffer.writer.field(i).write(record, schema.fieldIndex(writerSchema.fields()[i].name()));
    }
  }

  private void flushFragment(int fragmentId, FragmentBuffer buffer) {
    try {
      buffer.writer.finish();
      BufferAllocator allocator = LanceRuntime.allocator();

      try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
          ArrowReader reader = new SingleBatchArrowReader(allocator, buffer.data)) {
        Data.exportArrayStream(allocator, reader, stream);

        try (Dataset dataset =
            Utils.openDatasetBuilder(writeOptions)
                .initialStorageOptions(initialStorageOptions)
                .build()) {
          Fragment fragment = new Fragment(dataset, fragmentId);
          processFragment(fragment, stream);
        }
      } catch (Exception e) {
        throw new RuntimeException("Cannot read arrow stream.", e);
      }
    } finally {
      buffer.data.close();
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
  public WriterCommitMessage commit() {
    for (Map.Entry<Integer, FragmentBuffer> entry : buffers.entrySet()) {
      flushFragment(entry.getKey(), entry.getValue());
    }

    return buildCommitMessage();
  }

  @Override
  public void abort() {}

  @Override
  public void close() throws IOException {
    for (FragmentBuffer buffer : buffers.values()) {
      buffer.data.close();
    }
    buffers.clear();
  }
}
