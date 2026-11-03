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
import org.lance.fragment.RowIdMeta;
import org.lance.namespace.LanceNamespace;
import org.lance.operation.Update;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkCatalogConfig;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.function.LanceFragmentIdWithDefaultFunction;
import org.lance.spark.internal.ExecutorNamespace;
import org.lance.spark.utils.BlobReferenceResolver;
import org.lance.spark.utils.BlobSourceContext;
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
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static org.lance.spark.join.FragmentAwareJoinUtils.*;

public class SparkPositionDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {
  private static final Logger LOG = LoggerFactory.getLogger(SparkPositionDeltaWrite.class);

  // The `id` InternalRow passed to delete()/update() follows the order declared by
  // LancePositionDeltaOperation.rowId(): {_rowaddr, _rowid}.
  private static final int ID_COL_ROW_ADDR = 0;
  private static final int ID_COL_ROW_ID = 1;

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
  private final boolean managedVersioning;
  private final boolean hasStableRowIds;

  /** Blob source contexts for resolving copy tokens, keyed by source dataset URI. */
  private final Map<String, BlobSourceContext> blobSourceContexts;

  private final boolean deleteOnly;

  public SparkPositionDeltaWrite(
      StructType sparkSchema,
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      boolean managedVersioning,
      List<String> tableId,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this(
        sparkSchema,
        writeOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        managedVersioning,
        tableId,
        blobSourceContexts,
        false);
  }

  public SparkPositionDeltaWrite(
      StructType sparkSchema,
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      boolean managedVersioning,
      List<String> tableId,
      Map<String, BlobSourceContext> blobSourceContexts,
      boolean deleteOnly) {
    try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
      this.sparkSchema = LanceArrowUtils.markUnsignedForWrite(sparkSchema, ds.getSchema());
      this.writeOptions = writeOptions.withRef(LanceRef.ofMain(ds.version()));
      this.hasStableRowIds = hasStableRowIds(ds, writeOptions);
      LOG.debug(
          "Resolved dataset ref for position delta write: {}, stableRowIds={}",
          this.writeOptions.getRef(),
          this.hasStableRowIds);
    }
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.blobSourceContexts = blobSourceContexts;
    this.deleteOnly = deleteOnly;
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    if (deleteOnly) {
      // Co-locate every row of a fragment onto one task. Clustered distribution does not give
      // each fragment its own task: AQE can coalesce a small shuffle into one task that owns many
      // fragments, and LanceDeleteWriter writes those deletion files itself. Two tasks still must
      // not own the same fragment, because their deletion files cannot be merged. DELETE rows
      // always come from the scan, so `_fragid` is never null here and needs no spreading function.
      return Distributions.clustered(new Expression[] {segmentId});
    }
    // Avoid skew by spreading null segment_id rows across tasks.
    Expression clusteredExpr =
        Expressions.apply(LanceFragmentIdWithDefaultFunction.NAME, segmentId);
    return Distributions.clustered(new Expression[] {clusteredExpr});
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
          tableId,
          hasStableRowIds,
          blobSourceContexts,
          deleteOnly);
    }

    @Override
    public boolean useCommitCoordinator() {
      // Lance commits are atomic at the dataset level via CommitBuilder/Transaction on the
      // driver; Spark's OutputCommitCoordinator is unnecessary and can break under speculation or
      // retries.
      return false;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      List<FragmentMetadata> newFragments = new ArrayList<>();
      Map<Integer, RoaringBitmap> aggregatedDeletions = new HashMap<>();
      List<Long> removedFragmentIds = new ArrayList<>();
      List<FragmentMetadata> updatedFragments = new ArrayList<>();
      Set<Long> claimedFragmentIds = new HashSet<>();
      boolean useStableRowIds = hasStableRowIds;

      for (WriterCommitMessage msg : messages) {
        if (deleteOnly) {
          DeleteTaskCommit taskCommit = (DeleteTaskCommit) msg;
          for (FragmentMetadata fragment : taskCommit.updatedFragments()) {
            requireUniqueFragmentOwner(claimedFragmentIds, (long) fragment.getId());
            updatedFragments.add(fragment);
          }
          for (Long fragmentId : taskCommit.removedFragmentIds()) {
            requireUniqueFragmentOwner(claimedFragmentIds, fragmentId);
            removedFragmentIds.add(fragmentId);
          }
          continue;
        }
        DeltaWriteTaskCommit taskCommit = (DeltaWriteTaskCommit) msg;
        newFragments.addAll(taskCommit.newFragments());
        if (taskCommit.useStableRowIds()) {
          useStableRowIds = true;
        }
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
                  writeOptions.getRef(),
                  "ref must be set (resolved in SparkPositionDeltaWrite constructor)")
              .getVersionNumber()
              .get();
      try (Dataset dataset = Utils.openDatasetBuilder(writeOptions).build()) {
        List<Map.Entry<Integer, FragmentMetadata>> deletionResults =
            aggregatedDeletions.entrySet().parallelStream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> deleteFragmentRows(dataset, entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

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
                .updateMode(Optional.of(Update.UpdateMode.RewriteRows))
                .build();

        CommitBuilder commitBuilder =
            new CommitBuilder(dataset)
                .writeParams(
                    LanceRuntime.mergeStorageOptions(
                        writeOptions.getStorageOptions(), initialStorageOptions));
        String fileFormatVersion = writeOptions.getFileFormatVersion();
        if (fileFormatVersion != null) {
          commitBuilder.storageFormat(fileFormatVersion);
        }
        if (useStableRowIds) {
          commitBuilder.useStableRowIds(true);
        }
        if (managedVersioning) {
          LanceNamespace namespace =
              LanceRuntime.getOrCreateNamespace(namespaceImpl, namespaceProperties);
          commitBuilder
              .namespaceClient(namespace)
              .tableId(tableId)
              .namespaceClientManagedVersioning(true);
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

  private static void requireUniqueFragmentOwner(Set<Long> claimedFragmentIds, long fragmentId) {
    if (!claimedFragmentIds.add(fragmentId)) {
      throw new IllegalStateException(
          "Multiple DELETE task commits claimed fragment "
              + fragmentId
              + ". Deletion files from two owners of one fragment cannot be merged, so the commit"
              + " is rejected. This means the _fragid clustering from requiredDistribution() was"
              + " not applied to this write.");
    }
  }

  /** Records one deleted row address into a per-fragment deletion map. */
  private static void recordDeletion(Map<Integer, RoaringBitmap> deletionMap, long rowAddr) {
    deletionMap
        .computeIfAbsent(extractFragmentId(rowAddr), fragmentId -> new RoaringBitmap())
        .add(extractRowIndex(rowAddr));
  }

  private static class LanceDeleteWriter implements DeltaWriter<InternalRow> {
    private final LanceSparkReadOptions readOptions;
    private final Map<String, String> initialStorageOptions;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;
    private final Map<Integer, RoaringBitmap> deletionMap = new HashMap<>();

    private LanceDeleteWriter(
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties) {
      this.readOptions = writeOptions.toReadOptions();
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) {
      recordDeletion(deletionMap, id.getLong(ID_COL_ROW_ADDR));
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) {
      throw new UnsupportedOperationException("A DELETE writer cannot update rows");
    }

    @Override
    public void insert(InternalRow row) {
      throw new UnsupportedOperationException("A DELETE writer cannot insert rows");
    }

    @Override
    public WriterCommitMessage commit() {
      List<FragmentMetadata> updatedFragments = new ArrayList<>();
      List<Long> removedFragmentIds = new ArrayList<>();
      if (!deletionMap.isEmpty()) {
        try (ExecutorNamespace ignored =
                ExecutorNamespace.acquire(readOptions, namespaceImpl, namespaceProperties);
            Dataset dataset =
                Utils.openDatasetBuilder(readOptions)
                    .initialStorageOptions(initialStorageOptions)
                    .build()) {
          for (Map.Entry<Integer, FragmentMetadata> result :
              deleteFragments(dataset, deletionMap)) {
            if (result.getValue() == null) {
              removedFragmentIds.add((long) result.getKey());
            } else {
              updatedFragments.add(result.getValue());
            }
          }
        }
      }
      // Only the driver's version-pinned transaction publishes these deletion files. Retried or
      // discarded tasks can leave unreferenced files, but cannot change the visible dataset.
      return new DeleteTaskCommit(updatedFragments, removedFragmentIds);
    }

    @Override
    public void abort() {}

    @Override
    public void close() {
      deletionMap.clear();
    }
  }

  static class DeleteTaskCommit implements WriterCommitMessage {
    private static final long serialVersionUID = 1L;

    private final List<FragmentMetadata> updatedFragments;
    private final List<Long> removedFragmentIds;

    DeleteTaskCommit(List<FragmentMetadata> updatedFragments, List<Long> removedFragmentIds) {
      this.updatedFragments = updatedFragments;
      this.removedFragmentIds = removedFragmentIds;
    }

    List<FragmentMetadata> updatedFragments() {
      return updatedFragments;
    }

    List<Long> removedFragmentIds() {
      return removedFragmentIds;
    }
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
    private final boolean hasStableRowIds;
    private final Map<String, BlobSourceContext> blobSourceContexts;
    private final boolean deleteOnly;

    PositionDeltaWriteFactory(
        StructType sparkSchema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        boolean hasStableRowIds,
        Map<String, BlobSourceContext> blobSourceContexts,
        boolean deleteOnly) {
      this.sparkSchema = sparkSchema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.hasStableRowIds = hasStableRowIds;
      this.blobSourceContexts = blobSourceContexts;
      this.deleteOnly = deleteOnly;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      if (deleteOnly) {
        return new LanceDeleteWriter(
            writeOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
      }
      int batchSize = writeOptions.getBatchSize();
      boolean useQueuedBuffer = writeOptions.isUseQueuedWriteBuffer();
      boolean useLargeVarTypes = writeOptions.isUseLargeVarTypes();
      long maxBatchBytes = writeOptions.getMaxBatchBytes();

      LanceSparkWriteOptions fragmentWriteOptions =
          writeOptions.toBuilder().enableStableRowIds(false).build();
      WriteParams params = fragmentWriteOptions.toWriteParams(initialStorageOptions);

      // One resolver per write task, closed via LanceDataWriter when the task finishes. Always
      // created so copy tokens resolve even without captured contexts (falls back to open-by-URI).
      BlobReferenceResolver blobResolver = new BlobReferenceResolver(blobSourceContexts);

      // Select buffer type based on configuration
      ArrowBatchWriteBuffer writeBuffer;
      if (useQueuedBuffer) {
        int queueDepth = writeOptions.getQueueDepth();
        writeBuffer =
            new QueuedArrowBatchWriteBuffer(
                sparkSchema, batchSize, queueDepth, useLargeVarTypes, maxBatchBytes, blobResolver);
      } else {
        writeBuffer =
            new SemaphoreArrowBatchWriteBuffer(
                sparkSchema, batchSize, useLargeVarTypes, maxBatchBytes, blobResolver);
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
          new LanceDataWriter(
              writeBuffer, fragmentCreationTask, fragmentCreationThread, null, null, blobResolver),
          initialStorageOptions,
          hasStableRowIds);
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

    // Captured _rowid values in write order from update() for task-level RowIdMeta attachment.
    // In the native UPDATE path, update() provides both id and row in the same task, so
    // capturedRowIds order matches fragment write order by construction.
    private final List<Long> capturedRowIds;

    private final Map<Integer, RoaringBitmap> deletionMap;

    private final boolean hasStableRowIds;

    private LanceDeltaWriter(
        LanceSparkWriteOptions writeOptions,
        LanceDataWriter writer,
        Map<String, String> initialStorageOptions,
        boolean hasStableRowIds) {
      this.writeOptions = writeOptions;
      this.writer = writer;
      this.initialStorageOptions = initialStorageOptions;
      this.capturedRowIds = new ArrayList<>();
      this.deletionMap = new HashMap<>();
      this.hasStableRowIds = hasStableRowIds;
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      recordDeletion(deletionMap, id.getLong(ID_COL_ROW_ADDR));
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) throws IOException {
      long rowId = id.getLong(ID_COL_ROW_ID);
      long rowAddr = id.getLong(ID_COL_ROW_ADDR);
      if (hasStableRowIds) {
        capturedRowIds.add(rowId);
      }
      recordDeletion(deletionMap, rowAddr);
      writer.write(row);
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      writer.write(row);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      LanceBatchWrite.TaskCommit append = (LanceBatchWrite.TaskCommit) writer.commit();
      List<FragmentMetadata> newFragments = append.getFragments();

      if (hasStableRowIds && !capturedRowIds.isEmpty() && !newFragments.isEmpty()) {
        long totalPhysicalRows =
            newFragments.stream().mapToLong(FragmentMetadata::getPhysicalRows).sum();
        if (capturedRowIds.size() == totalPhysicalRows) {
          newFragments = attachRowIdMeta(newFragments, capturedRowIds);
        } else {
          LOG.warn(
              "Skipping RowIdMeta attachment: captured {} row IDs but new fragments have {}"
                  + " physical rows. This is unexpected in the native UPDATE path where only"
                  + " update() calls produce rows; it may indicate a bug or a future code path"
                  + " mixing update and insert rows in the same task.",
              capturedRowIds.size(),
              totalPhysicalRows);
        }
      }

      return new DeltaWriteTaskCommit(newFragments, deletionMap, hasStableRowIds);
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
   * Writes one deletion file per fragment. A single fragment stays on the caller thread. Several
   * fragments use a daemon pool bounded by the processor count, because a small DELETE shuffle can
   * be coalesced into one task and this pool is not the JVM common pool shared by every task.
   */
  private static List<Map.Entry<Integer, FragmentMetadata>> deleteFragments(
      Dataset dataset, Map<Integer, RoaringBitmap> deletionMap) {
    int parallelism = Math.min(deletionMap.size(), Runtime.getRuntime().availableProcessors());
    if (parallelism <= 1) {
      List<Map.Entry<Integer, FragmentMetadata>> results = new ArrayList<>(deletionMap.size());
      for (Map.Entry<Integer, RoaringBitmap> entry : deletionMap.entrySet()) {
        results.add(deleteFragmentRows(dataset, entry.getKey(), entry.getValue()));
      }
      return results;
    }

    ExecutorService pool =
        Executors.newFixedThreadPool(
            parallelism,
            runnable -> {
              Thread thread = new Thread(runnable, "lance-delete-rows");
              thread.setDaemon(true);
              return thread;
            });
    try {
      List<Future<Map.Entry<Integer, FragmentMetadata>>> futures =
          new ArrayList<>(deletionMap.size());
      for (Map.Entry<Integer, RoaringBitmap> entry : deletionMap.entrySet()) {
        futures.add(
            pool.submit(() -> deleteFragmentRows(dataset, entry.getKey(), entry.getValue())));
      }
      List<Map.Entry<Integer, FragmentMetadata>> results = new ArrayList<>(futures.size());
      RuntimeException failure = null;
      for (Future<Map.Entry<Integer, FragmentMetadata>> future : futures) {
        if (failure != null) {
          future.cancel(true);
          continue;
        }
        try {
          results.add(future.get());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          failure = new RuntimeException("Interrupted while writing deletion files", e);
        } catch (ExecutionException e) {
          Throwable cause = e.getCause() == null ? e : e.getCause();
          if (cause instanceof RuntimeException) {
            failure = (RuntimeException) cause;
          } else if (cause instanceof Error) {
            throw (Error) cause;
          } else {
            failure = new RuntimeException(cause);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
      return results;
    } finally {
      pool.shutdownNow();
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

  // ---------------------------------------------------------------------------
  // Row ID meta helpers
  // ---------------------------------------------------------------------------

  private static boolean hasStableRowIds(Dataset dataset, LanceSparkWriteOptions writeOptions) {
    if (dataset.hasStableRowIds() || Boolean.TRUE.equals(writeOptions.getEnableStableRowIds())) {
      return true;
    }
    String configured =
        dataset.getConfig().get(LanceSparkCatalogConfig.TABLE_OPT_ENABLE_STABLE_ROW_IDS);
    return Boolean.parseBoolean(configured);
  }

  private static List<FragmentMetadata> attachRowIdMeta(
      List<FragmentMetadata> fragments, List<Long> rowIds) {
    List<FragmentMetadata> result = new ArrayList<>(fragments.size());
    int offset = 0;
    for (FragmentMetadata fragment : fragments) {
      int count = (int) fragment.getPhysicalRows();
      long[] ids = new long[count];
      for (int i = 0; i < count; i++) {
        ids[i] = rowIds.get(offset + i);
      }
      offset += count;
      result.add(
          new FragmentMetadata(
              fragment.getId(),
              fragment.getFiles(),
              fragment.getPhysicalRows(),
              fragment.getDeletionFile(),
              RowIdMeta.fromRowIds(ids),
              fragment.getCreatedAtVersionMeta(),
              fragment.getLastUpdatedAtVersionMeta()));
    }
    return result;
  }

  static class DeltaWriteTaskCommit implements WriterCommitMessage {
    private static final long serialVersionUID = 1L;

    private final List<FragmentMetadata> newFragments;
    private final Map<Integer, RoaringBitmap> deletionMap;
    private final boolean useStableRowIds;

    DeltaWriteTaskCommit(
        List<FragmentMetadata> newFragments,
        Map<Integer, RoaringBitmap> deletionMap,
        boolean useStableRowIds) {
      this.newFragments = newFragments;
      this.deletionMap = deletionMap;
      this.useStableRowIds = useStableRowIds;
    }

    DeltaWriteTaskCommit(
        List<FragmentMetadata> newFragments, Map<Integer, RoaringBitmap> deletionMap) {
      this(newFragments, deletionMap, false);
    }

    public List<FragmentMetadata> newFragments() {
      return newFragments == null ? Collections.emptyList() : newFragments;
    }

    public Map<Integer, RoaringBitmap> deletionMap() {
      return deletionMap == null ? Collections.emptyMap() : deletionMap;
    }

    public boolean useStableRowIds() {
      return useStableRowIds;
    }
  }
}
