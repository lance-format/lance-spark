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
import org.lance.ReadOptions;
import org.lance.fragment.FragmentUpdateResult;
import org.lance.io.StorageOptionsProvider;
import org.lance.operation.Update;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * BatchWrite implementation for UPDATE COLUMNS FROM command.
 *
 * <p>This updates existing columns in a Lance table by computing their values from a source
 * TABLE/VIEW. Only rows that match (by _rowaddr) are updated; rows in source that don't exist in
 * target are ignored.
 */
public class UpdateColumnsBackfillBatchWrite implements BatchWrite {
  private static final Logger logger =
      LoggerFactory.getLogger(UpdateColumnsBackfillBatchWrite.class);

  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
  private final List<String> updateColumns;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  public UpdateColumnsBackfillBatchWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      List<String> updateColumns,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.updateColumns = updateColumns;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new UpdateColumnsWriterFactory(
        schema,
        writeOptions,
        updateColumns,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<FragmentMetadata> updatedFragments =
        Arrays.stream(messages)
            .map(m -> (TaskCommit) m)
            .map(TaskCommit::getUpdatedFragments)
            .flatMap(List::stream)
            .collect(Collectors.toList());

    // Collect fieldsModified from all tasks (they should be the same)
    long[] fieldsModified =
        Arrays.stream(messages)
            .map(m -> (TaskCommit) m)
            .map(TaskCommit::getFieldsModified)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(new long[0]);

    if (updatedFragments.isEmpty()) {
      logger.info("No updated fragments to commit.");
      return;
    }

    // Get fragments that were not updated (no matching rows in source)
    Set<Integer> updatedFragmentIds =
        updatedFragments.stream().map(FragmentMetadata::getId).collect(Collectors.toSet());

    try (Dataset dataset = openDataset(writeOptions)) {
      // Add unmodified fragments back
      dataset.getFragments().stream()
          .filter(f -> !updatedFragmentIds.contains(f.getId()))
          .map(Fragment::metadata)
          .forEach(updatedFragments::add);
    }

    // Commit update operation using transaction builder
    try (Dataset dataset = openDataset(writeOptions)) {
      Update update =
          Update.builder()
              .updatedFragments(updatedFragments)
              .fieldsModified(fieldsModified)
              .updateMode(Optional.of(Update.UpdateMode.RewriteColumns))
              .build();
      dataset.newTransactionBuilder().operation(update).build().commit();
    }
  }

  private static Dataset openDataset(LanceSparkWriteOptions writeOptions) {
    if (writeOptions.hasNamespace()) {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .namespace(writeOptions.getNamespace())
          .tableId(writeOptions.getTableId())
          .build();
    } else {
      ReadOptions readOptions =
          new ReadOptions.Builder().setStorageOptions(writeOptions.getStorageOptions()).build();
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(writeOptions.getDatasetUri())
          .readOptions(readOptions)
          .build();
    }
  }

  private static Dataset openDatasetWithCredentialRefresh(
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    Map<String, String> merged =
        LanceRuntime.mergeStorageOptions(writeOptions.getStorageOptions(), initialStorageOptions);
    StorageOptionsProvider provider =
        LanceRuntime.getOrCreateStorageOptionsProvider(namespaceImpl, namespaceProperties, tableId);

    ReadOptions.Builder builder = new ReadOptions.Builder().setStorageOptions(merged);
    if (provider != null) {
      builder.setStorageOptionsProvider(provider);
    }

    return Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(writeOptions.getDatasetUri())
        .readOptions(builder.build())
        .build();
  }

  public static class UpdateColumnsWriter implements DataWriter<InternalRow> {
    private final LanceSparkWriteOptions writeOptions;
    private final StructType schema;
    private final int fragmentIdField;
    private final List<FragmentMetadata> updatedFragments;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    private long[] fieldsModified;
    private StructType writerSchema;
    private int fragmentId = -1;
    private VectorSchemaRoot data;
    private org.lance.spark.arrow.LanceArrowWriter writer = null;

    public UpdateColumnsWriter(
        LanceSparkWriteOptions writeOptions,
        StructType schema,
        List<String> updateColumns,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this.writeOptions = writeOptions;
      this.schema = schema;
      this.fragmentIdField = schema.fieldIndex(LanceDataset.FRAGMENT_ID_COLUMN.name());
      this.updatedFragments = new ArrayList<>();
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;

      // Build writer schema: only include columns to update + _rowaddr for matching
      this.writerSchema = new StructType();
      Arrays.stream(schema.fields())
          .filter(
              f ->
                  updateColumns.contains(f.name())
                      || f.name().equals(LanceDataset.ROW_ADDRESS_COLUMN.name()))
          .forEach(f -> writerSchema = writerSchema.add(f));

      createWriter();
    }

    @Override
    public void write(InternalRow record) throws IOException {
      int fragId = record.getInt(fragmentIdField);

      if (fragmentId == -1) {
        fragmentId = fragId;
      }

      if (fragId != fragmentId && data != null) {
        // New fragment's data is coming, update the current fragment.
        updateFragment();

        fragmentId = fragId;
        createWriter();
      }

      for (int i = 0; i < writerSchema.fields().length; i++) {
        writer.field(i).write(record, schema.fieldIndex(writerSchema.fields()[i].name()));
      }
    }

    private void createWriter() {
      BufferAllocator allocator = LanceRuntime.allocator();
      data =
          VectorSchemaRoot.create(
              LanceArrowUtils.toArrowSchema(writerSchema, "UTC", false, false), allocator);

      writer = org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(data, writerSchema);
    }

    private void updateFragment() {
      writer.finish();

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(data, null, out)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        throw new RuntimeException("Cannot write schema root", e);
      }

      byte[] arrowData = out.toByteArray();
      ByteArrayInputStream in = new ByteArrayInputStream(arrowData);
      BufferAllocator allocator = LanceRuntime.allocator();

      try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
          ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, stream);

        // Use Dataset to get the fragment and update columns
        try (Dataset dataset =
            openDatasetWithCredentialRefresh(
                writeOptions, initialStorageOptions, namespaceImpl, namespaceProperties, tableId)) {
          Fragment fragment = new Fragment(dataset, fragmentId);
          FragmentUpdateResult result =
              fragment.updateColumns(
                  stream,
                  LanceDataset.ROW_ADDRESS_COLUMN.name(),
                  LanceDataset.ROW_ADDRESS_COLUMN.name());

          updatedFragments.add(result.getUpdatedFragment());
          fieldsModified = result.getFieldsModified();
        }
      } catch (Exception e) {
        throw new RuntimeException("Cannot read arrow stream.", e);
      }

      data.close();
    }

    @Override
    public WriterCommitMessage commit() {
      if (fragmentId >= 0 && data != null) {
        updateFragment();
      }

      return new TaskCommit(updatedFragments, fieldsModified);
    }

    @Override
    public void abort() {}

    @Override
    public void close() throws IOException {}
  }

  public static class UpdateColumnsWriterFactory implements DataWriterFactory {
    private final LanceSparkWriteOptions writeOptions;
    private final StructType schema;
    private final List<String> updateColumns;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    protected UpdateColumnsWriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        List<String> updateColumns,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      // Everything passed to writer factory should be serializable
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.updateColumns = updateColumns;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new UpdateColumnsWriter(
          writeOptions,
          schema,
          updateColumns,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId);
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return String.format("UpdateColumnsWriterFactory(datasetUri=%s)", writeOptions.getDatasetUri());
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final List<FragmentMetadata> updatedFragments;
    private final long[] fieldsModified;

    TaskCommit(List<FragmentMetadata> updatedFragments, long[] fieldsModified) {
      this.updatedFragments = updatedFragments;
      this.fieldsModified = fieldsModified;
    }

    List<FragmentMetadata> getUpdatedFragments() {
      return updatedFragments != null ? updatedFragments : Collections.emptyList();
    }

    long[] getFieldsModified() {
      return fieldsModified;
    }
  }
}
