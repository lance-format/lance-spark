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
import org.lance.RowAddress;
import org.lance.WriteParams;
import org.lance.io.StorageOptionsProvider;
import org.lance.operation.Update;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class SparkPositionDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {
  private final StructType sparkSchema;
  private final LanceSparkWriteOptions writeOptions;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  public SparkPositionDeltaWrite(
      StructType sparkSchema,
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.sparkSchema = sparkSchema;
    this.writeOptions = writeOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    return Distributions.clustered(new NamedReference[] {segmentId});
  }

  @Override
  public SortOrder[] requiredOrdering() {
    NamedReference segmentId = Expressions.column(LanceConstant.ROW_ADDRESS);
    SortValue sortValue =
        new SortValue(segmentId, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    return new SortValue[] {sortValue};
  }

  @Override
  public DeltaBatchWrite toBatch() {
    return new PositionDeltaBatchWrite();
  }

  private class PositionDeltaBatchWrite implements DeltaBatchWrite {

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new PositionDeltaWriteFactory(
          sparkSchema,
          writeOptions,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      List<Long> removedFragmentIds = new ArrayList<>();
      List<FragmentMetadata> updatedFragments = new ArrayList<>();
      List<FragmentMetadata> newFragments = new ArrayList<>();

      Arrays.stream(messages)
          .map(m -> (DeltaWriteTaskCommit) m)
          .forEach(
              m -> {
                removedFragmentIds.addAll(m.removedFragmentIds());
                updatedFragments.addAll(m.updatedFragments());
                newFragments.addAll(m.newFragments());
              });

      // Use SDK directly to update fragments
      try (Dataset dataset = openDataset(writeOptions)) {
        Update update =
            Update.builder()
                .removedFragmentIds(removedFragmentIds)
                .updatedFragments(updatedFragments)
                .newFragments(newFragments)
                .build();

        dataset.newTransactionBuilder().operation(update).build().commit();
      }
    }

    private Dataset openDataset(LanceSparkWriteOptions options) {
      if (options.hasNamespace()) {
        return Dataset.open()
            .allocator(LanceRuntime.allocator())
            .namespace(options.getNamespace())
            .tableId(options.getTableId())
            .build();
      } else {
        ReadOptions readOptions =
            new ReadOptions.Builder().setStorageOptions(options.getStorageOptions()).build();
        return Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(options.getDatasetUri())
            .readOptions(readOptions)
            .build();
      }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {}
  }

  private static class PositionDeltaWriteFactory implements DeltaWriterFactory {
    private final StructType sparkSchema;
    private final LanceSparkWriteOptions writeOptions;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    PositionDeltaWriteFactory(
        StructType sparkSchema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this.sparkSchema = sparkSchema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      int batchSize = writeOptions.getBatchSize();
      boolean useQueuedBuffer = writeOptions.isUseQueuedWriteBuffer();

      // Merge initial storage options with write options
      WriteParams params = buildWriteParams();

      // Select buffer type based on configuration
      ArrowBatchWriteBuffer writeBuffer;
      if (useQueuedBuffer) {
        int queueDepth = writeOptions.getQueueDepth();
        writeBuffer = new QueuedArrowBatchWriteBuffer(sparkSchema, batchSize, queueDepth);
      } else {
        writeBuffer = new SemaphoreArrowBatchWriteBuffer(sparkSchema, batchSize);
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

      return new LanceDeltaWriter(
          writeOptions,
          new LanceDataWriter(writeBuffer, fragmentCreationTask, fragmentCreationThread),
          initialStorageOptions);
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

  private static class LanceDeltaWriter implements DeltaWriter<InternalRow> {
    private final LanceSparkWriteOptions writeOptions;
    private final LanceDataWriter writer;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    // Key is fragmentId, Value is fragment's deleted row indexes
    private final Map<Integer, RoaringBitmap> deletedRows;

    private LanceDeltaWriter(
        LanceSparkWriteOptions writeOptions,
        LanceDataWriter writer,
        Map<String, String> initialStorageOptions) {
      this.writeOptions = writeOptions;
      this.writer = writer;
      this.initialStorageOptions = initialStorageOptions;
      this.deletedRows = new HashMap<>();
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      int fragmentId = metadata.getInt(0);
      deletedRows.compute(
          fragmentId,
          (k, v) -> {
            if (v == null) {
              v = new RoaringBitmap();
            }
            // Get the row index which is low 32 bits of row address.
            // See
            // https://github.com/lance-format/lance/blob/main/rust/lance-core/src/utils/address.rs#L36
            v.add(RowAddress.rowIndex(id.getLong(0)));
            return v;
          });
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) throws IOException {
      throw new UnsupportedOperationException("Update is not supported");
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      writer.write(row);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      // Write new fragments to store new updated rows.
      LanceBatchWrite.TaskCommit append = (LanceBatchWrite.TaskCommit) writer.commit();
      List<FragmentMetadata> newFragments = append.getFragments();

      List<Long> removedFragmentIds = new ArrayList<>();
      List<FragmentMetadata> updatedFragments = new ArrayList<>();

      // Deleting updated rows from old fragments using SDK directly.
      try (Dataset dataset = openDataset(writeOptions)) {
        this.deletedRows.forEach(
            (fragmentId, rowIndexes) -> {
              FragmentMetadata updatedFragment =
                  dataset.getFragment(fragmentId).deleteRows(ImmutableList.copyOf(rowIndexes));
              if (updatedFragment != null) {
                updatedFragments.add(updatedFragment);
              } else {
                removedFragmentIds.add(Long.valueOf(fragmentId));
              }
            });
      }

      return new DeltaWriteTaskCommit(removedFragmentIds, updatedFragments, newFragments);
    }

    private Dataset openDataset(LanceSparkWriteOptions options) {
      // Note: options.hasNamespace() is false on workers (namespace is transient)
      Map<String, String> merged =
          LanceRuntime.mergeStorageOptions(options.getStorageOptions(), initialStorageOptions);
      ReadOptions readOptions = new ReadOptions.Builder().setStorageOptions(merged).build();
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(options.getDatasetUri())
          .readOptions(readOptions)
          .build();
    }

    @Override
    public void abort() throws IOException {
      writer.abort();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  private static class DeltaWriteTaskCommit implements WriterCommitMessage {
    private List<Long> removedFragmentIds;
    private List<FragmentMetadata> updatedFragments;
    private List<FragmentMetadata> newFragments;

    DeltaWriteTaskCommit(
        List<Long> removedFragmentIds,
        List<FragmentMetadata> updatedFragments,
        List<FragmentMetadata> newFragments) {
      this.removedFragmentIds = removedFragmentIds;
      this.updatedFragments = updatedFragments;
      this.newFragments = newFragments;
    }

    public List<Long> removedFragmentIds() {
      return removedFragmentIds == null ? Collections.emptyList() : removedFragmentIds;
    }

    public List<FragmentMetadata> updatedFragments() {
      return updatedFragments == null ? Collections.emptyList() : updatedFragments;
    }

    public List<FragmentMetadata> newFragments() {
      return newFragments == null ? Collections.emptyList() : newFragments;
    }
  }
}
