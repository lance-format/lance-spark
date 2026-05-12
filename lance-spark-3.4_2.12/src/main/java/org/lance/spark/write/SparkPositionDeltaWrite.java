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

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.operation.Update;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.function.LanceFragmentIdWithDefaultFunction;
import org.lance.spark.utils.Utils;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static org.lance.spark.join.FragmentAwareJoinUtils.*;

public class SparkPositionDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {
  private static final Logger logger = LoggerFactory.getLogger(SparkPositionDeltaWrite.class);

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
    try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
      this.writeOptions = writeOptions.withVersion(ds.version());
      logger.debug(
          "Resolved dataset version for position delta write: {}", this.writeOptions.getVersion());
    }
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    // Avoid skew by spreading null segment_id rows across tasks.
    Expression clusteredExpr =
        Expressions.apply(LanceFragmentIdWithDefaultFunction.NAME, segmentId);
    return Distributions.clustered(new Expression[] {clusteredExpr});
  }

  @Override
  public SortOrder[] requiredOrdering() {
    NamedReference segmentId = Expressions.column(LanceConstant.ROW_ADDRESS);
    // Use Expressions.sort() instead of SortValue (SortValue was added in Spark 3.5)
    SortOrder sortOrder =
        Expressions.sort(segmentId, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    return new SortOrder[] {sortOrder};
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
      List<FragmentMetadata> newFragments = new ArrayList<>();
      Map<Integer, RoaringBitmap> aggregatedDeletions = new HashMap<>();

      for (WriterCommitMessage msg : messages) {
        DeltaWriteTaskCommit taskCommit = (DeltaWriteTaskCommit) msg;
        newFragments.addAll(taskCommit.newFragments());
        taskCommit
            .deletionMap()
            .forEach(
                (fragId, bitmap) ->
                    aggregatedDeletions.merge(
                        fragId,
                        bitmap.clone(),
                        (existing, incoming) -> {
                          existing.or(incoming);
                          return existing;
                        }));
      }

      long version =
          Objects.requireNonNull(
              writeOptions.getVersion(),
              "version must be set (resolved in SparkPositionDeltaWrite constructor)");
      try (Dataset dataset = Utils.openDatasetBuilder(writeOptions).build()) {
        // Parallel stream is safe: each deleteRows() operates on an independent
        // FileFragment value writing to a distinct object store path (see lance-core).
        // Collectors.toList() handles thread-safe accumulation internally.
        List<Map.Entry<Integer, FragmentMetadata>> deletionResults =
            aggregatedDeletions.entrySet().parallelStream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> deleteFragmentRows(dataset, entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        List<Long> removedFragmentIds = new ArrayList<>();
        List<FragmentMetadata> updatedFragments = new ArrayList<>();
        for (Map.Entry<Integer, FragmentMetadata> deletionResult : deletionResults) {
          if (deletionResult.getValue() != null) {
            updatedFragments.add(deletionResult.getValue());
          } else {
            removedFragmentIds.add(Long.valueOf(deletionResult.getKey()));
          }
        }

        Update update =
            Update.builder()
                .removedFragmentIds(removedFragmentIds)
                .updatedFragments(updatedFragments)
                .newFragments(newFragments)
                .build();

        CommitBuilder commitBuilder =
            new CommitBuilder(dataset).writeParams(writeOptions.getStorageOptions());
        if (dataset.hasStableRowIds()) {
          commitBuilder.useStableRowIds(true);
        }
        try (Transaction txn =
                new Transaction.Builder().readVersion(version).operation(update).build();
            Dataset committed = commitBuilder.execute(txn)) {
          // auto-close txn and committed dataset
        }
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
      boolean useLargeVarTypes = writeOptions.isUseLargeVarTypes();

      // Merge initial storage options with write options
      WriteParams params = writeOptions.toWriteParams(initialStorageOptions);

      // Select buffer type based on configuration
      ArrowBatchWriteBuffer writeBuffer;
      if (useQueuedBuffer) {
        int queueDepth = writeOptions.getQueueDepth();
        writeBuffer =
            new QueuedArrowBatchWriteBuffer(sparkSchema, batchSize, queueDepth, useLargeVarTypes);
      } else {
        writeBuffer = new SemaphoreArrowBatchWriteBuffer(sparkSchema, batchSize, useLargeVarTypes);
      }

      // Create fragment in background thread
      Callable<List<FragmentMetadata>> fragmentCreator =
          () -> {
            try (ArrowArrayStream arrowStream =
                ArrowArrayStream.allocateNew(LanceRuntime.allocator())) {
              Data.exportArrayStream(LanceRuntime.allocator(), writeBuffer, arrowStream);
              return Fragment.create(writeOptions.getDatasetUri(), arrowStream, params);
            }
          };
      FutureTask<List<FragmentMetadata>> fragmentCreationTask =
          writeBuffer.createTrackedTask(fragmentCreator);
      Thread fragmentCreationThread = new Thread(fragmentCreationTask);
      fragmentCreationThread.start();

      return new LanceDeltaWriter(
          writeOptions,
          new LanceDataWriter(writeBuffer, fragmentCreationTask, fragmentCreationThread),
          initialStorageOptions);
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

    private final Map<Integer, RoaringBitmap> deletionMap;

    private LanceDeltaWriter(
        LanceSparkWriteOptions writeOptions,
        LanceDataWriter writer,
        Map<String, String> initialStorageOptions) {
      this.writeOptions = writeOptions;
      this.writer = writer;
      this.initialStorageOptions = initialStorageOptions;
      this.deletionMap = new HashMap<>();
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      long rowAddr = id.getLong(0);
      deletionMap
          .computeIfAbsent(extractFragmentId(rowAddr), fragmentId -> new RoaringBitmap())
          .add(extractRowIndex(rowAddr));
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
      LanceBatchWrite.TaskCommit append = (LanceBatchWrite.TaskCommit) writer.commit();
      List<FragmentMetadata> newFragments = append.getFragments();
      return new DeltaWriteTaskCommit(newFragments, deletionMap);
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

  /**
   * Writes deletion metadata for a single fragment. Returns the fragment ID paired with the updated
   * fragment metadata, or {@code null} if all rows were deleted (fragment removed).
   */
  private static Map.Entry<Integer, FragmentMetadata> deleteFragmentRows(
      Dataset dataset, int fragmentId, RoaringBitmap bitmap) {
    List<Integer> rowIndexes = new ArrayList<>();
    IntIterator it = bitmap.getIntIterator();
    while (it.hasNext()) {
      rowIndexes.add(it.next());
    }
    FragmentMetadata updatedFragment =
        dataset.getFragment(fragmentId).deleteRows(ImmutableList.copyOf(rowIndexes));
    return new AbstractMap.SimpleEntry<>(fragmentId, updatedFragment);
  }

  static class DeltaWriteTaskCommit implements WriterCommitMessage {
    private static final long serialVersionUID = 1L;

    private final List<FragmentMetadata> newFragments;
    private final Map<Integer, RoaringBitmap> deletionMap;

    DeltaWriteTaskCommit(
        List<FragmentMetadata> newFragments, Map<Integer, RoaringBitmap> deletionMap) {
      this.newFragments = newFragments;
      this.deletionMap = deletionMap;
    }

    public List<FragmentMetadata> newFragments() {
      return newFragments == null ? Collections.emptyList() : newFragments;
    }

    public Map<Integer, RoaringBitmap> deletionMap() {
      return deletionMap == null ? Collections.emptyMap() : deletionMap;
    }
  }
}
