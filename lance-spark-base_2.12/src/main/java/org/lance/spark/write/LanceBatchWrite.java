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
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.memwal.ShardingSpec;
import org.lance.namespace.LanceNamespace;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.operation.Update;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.BlobSourceContext;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.lance.spark.join.FragmentAwareJoinUtils.extractFragmentId;
import static org.lance.spark.join.FragmentAwareJoinUtils.extractRowIndex;

public class LanceBatchWrite implements BatchWrite {
  private static final Logger logger = LoggerFactory.getLogger(LanceBatchWrite.class);

  private final StructType schema;
  private LanceSparkWriteOptions writeOptions;
  private final boolean overwrite;

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

  private final StagedCommit stagedCommit;

  /** Sharding spec controlling how data is distributed across fragments. */
  private final ShardingSpec shardingSpec;

  /**
   * Per-source blob credential/open contexts keyed by source dataset URI, captured on the driver
   * and passed to write tasks so they can reopen source datasets to resolve blob references.
   */
  private final Map<String, BlobSourceContext> blobSourceContexts;

  public LanceBatchWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean managedVersioning,
      StagedCommit stagedCommit) {
    this(
        schema,
        writeOptions,
        overwrite,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId,
        managedVersioning,
        stagedCommit,
        null,
        java.util.Collections.emptyMap());
  }

  public LanceBatchWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean managedVersioning,
      StagedCommit stagedCommit,
      ShardingSpec shardingSpec,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this.schema = schema;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.stagedCommit = stagedCommit;
    this.shardingSpec = shardingSpec;
    this.blobSourceContexts =
        blobSourceContexts == null ? java.util.Collections.emptyMap() : blobSourceContexts;

    // For staged operations, the dataset is managed by StagedCommit.
    // For non-staged operations, pin the dataset version for OCC.
    if (stagedCommit != null) {
      this.writeOptions = writeOptions;
    } else {
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        this.writeOptions = writeOptions.withVersion(ds.version());
        logger.debug(
            "Resolved dataset version for batch write: {}", this.writeOptions.getVersion());
      }
    }
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new LanceDataWriter.WriterFactory(
        schema,
        writeOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId,
        shardingSpec,
        blobSourceContexts);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<FragmentMetadata> fragments =
        Arrays.stream(messages)
            .map(m -> (TaskCommit) m)
            .map(TaskCommit::getFragments)
            .map(LanceDataWriter::stripRowIdMeta)
            .flatMap(List::stream)
            .collect(Collectors.toList());

    Schema arrowSchema =
        LanceArrowUtils.toArrowSchema(schema, "UTC", true, writeOptions.isUseLargeVarTypes());
    boolean isOverwrite = overwrite || writeOptions.isOverwrite();

    // Boxed: null means unset (inherit in lance-core); see LanceSparkWriteOptions.
    final Boolean enableStableRowIds = writeOptions.getEnableStableRowIds();

    if (stagedCommit != null) {
      // For staged tables, update the eagerly-created StagedCommit with fragments and schema.
      // commitStagedChanges() will perform the actual commit.
      stagedCommit.setFragments(fragments);
      stagedCommit.setSchema(arrowSchema);
      if (enableStableRowIds != null) {
        stagedCommit.setEnableStableRowIds(enableStableRowIds);
      }
      String fileFormatVersion = writeOptions.getFileFormatVersion();
      if (fileFormatVersion != null) {
        stagedCommit.setFileFormatVersion(fileFormatVersion);
      }
      // For a path-based staged create, StagedCommit only has the catalog's static storage
      // options at this point. Merge in write-time and namespace-vended options now so
      // StagedCommit.commit() uses them. Mirrors the non-staged merge below.
      stagedCommit.mergeStorageOptions(
          LanceRuntime.mergeStorageOptions(
              writeOptions.getStorageOptions(), initialStorageOptions));
    } else {
      // For non-staged tables, commit immediately
      long version =
          Objects.requireNonNull(
              writeOptions.getVersion(),
              "version must be set (resolved in LanceBatchWrite constructor)");
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        Operation operation;
        if (writeOptions.getReplaceWhere() != null) {
          operation =
              buildReplaceOperation(
                  ds,
                  writeOptions.getReplaceWhere(),
                  writeOptions.getReplaceWhereEqualities(),
                  fragments);
        } else if (isOverwrite) {
          operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
        } else {
          operation = Append.builder().fragments(fragments).build();
        }
        CommitBuilder commitBuilder =
            new CommitBuilder(ds)
                .writeParams(
                    LanceRuntime.mergeStorageOptions(
                        writeOptions.getStorageOptions(), initialStorageOptions));
        String fileFormatVersion = writeOptions.getFileFormatVersion();
        if (fileFormatVersion != null) {
          commitBuilder.storageFormat(fileFormatVersion);
        }
        // When enableStableRowIds is null (user didn't pass the option),
        // lance-core auto-inherits the flag from the existing manifest.
        // Appending to a table with stable row IDs works without
        // re-specifying the option.
        if (enableStableRowIds != null) {
          commitBuilder.useStableRowIds(enableStableRowIds);
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
                new Transaction.Builder().readVersion(version).operation(operation).build();
            Dataset committed = commitBuilder.execute(txn)) {
          // auto-close txn and committed dataset
        }
      }
    }
  }

  /**
   * Builds an atomic {@link Update} that replaces the rows matching {@code predicate} with the
   * newly written {@code newFragments}. The existing rows are found by scanning the open dataset
   * for their physical row addresses; each affected fragment is rewritten with those rows deleted
   * (added to {@code updatedFragments}), or dropped entirely when all of its rows match (added to
   * {@code removedFragmentIds}). Deletes and the append land in a single table version.
   *
   * <p>This is correct regardless of physical layout: a fragment that only partially matches the
   * predicate keeps its non-matching rows via a deletion vector, while a fragment fully covered by
   * the predicate is removed outright.
   */
  private static Operation buildReplaceOperation(
      Dataset ds, String predicate, String equalitiesJson, List<FragmentMetadata> newFragments) {
    List<Long> removedFragmentIds = new ArrayList<>();
    List<FragmentMetadata> updatedFragments = new ArrayList<>();

    // Metadata-only fast path: for an equality predicate, use zonemap statistics to identify
    // fragments whose every live row provably matches (each indexed equality column has all zones
    // pinned to the required value), and drop them by id without reading any rows. Fragments that
    // cannot be proven fully covered are handled by the exact scan below, so this only ever avoids
    // work — it never changes which rows are replaced.
    Set<Integer> fullyCoveredFragmentIds =
        ReplaceCoverage.fullyCoveredFragmentIds(ds, equalitiesJson);
    for (int fragmentId : fullyCoveredFragmentIds) {
      removedFragmentIds.add((long) fragmentId);
    }

    // Scan only the fragments not already dropped by metadata, restricting the scan to their ids.
    // When every fragment is proven covered, this list is empty and the scan is skipped entirely —
    // the whole-partition case reads no data rows at all (O(#fragments), not O(rows)).
    List<Integer> scanFragmentIds = new ArrayList<>();
    for (Fragment fragment : ds.getFragments()) {
      if (!fullyCoveredFragmentIds.contains(fragment.getId())) {
        scanFragmentIds.add(fragment.getId());
      }
    }

    Map<Integer, RoaringBitmap> deletionsByFragment =
        scanFragmentIds.isEmpty()
            ? java.util.Collections.emptyMap()
            : matchingDeletionsByFragment(ds, predicate, scanFragmentIds);

    for (Map.Entry<Integer, RoaringBitmap> entry : deletionsByFragment.entrySet()) {
      int fragmentId = entry.getKey();
      Fragment fragment = ds.getFragment(fragmentId);
      RoaringBitmap matched = entry.getValue();
      // Fast path: when every live row in the fragment matches, drop the fragment outright without
      // materializing any per-row list. This covers the common partition-overwrite case (a whole
      // partition living in its own fragment) and keeps driver memory independent of fragment size.
      if (matched.getLongCardinality() == fragment.metadata().getNumRows()) {
        removedFragmentIds.add((long) fragmentId);
        continue;
      }
      // Partial match: the native deleteRows takes a List<Integer>, so the surviving indexes for
      // this one fragment are materialized here. This list is bounded by a single fragment's row
      // count, not by the total matched row count across the table.
      List<Integer> rowIndexes = new ArrayList<>(matched.getCardinality());
      matched.forEach((org.roaringbitmap.IntConsumer) rowIndexes::add);
      FragmentMetadata updated = fragment.deleteRows(rowIndexes);
      if (updated == null) {
        removedFragmentIds.add((long) fragmentId);
      } else {
        updatedFragments.add(updated);
      }
    }

    return Update.builder()
        .removedFragmentIds(removedFragmentIds)
        .updatedFragments(updatedFragments)
        .newFragments(newFragments)
        .build();
  }

  /**
   * Scans the dataset for rows matching {@code predicate} and collects their physical row indexes
   * per fragment as {@link RoaringBitmap}s, decoding the 64-bit {@code _rowaddr} (fragment id in
   * the high 32 bits, row index in the low 32 bits). A compressed bitmap per fragment keeps driver
   * memory bounded regardless of how many rows match. Returns an empty map when no row matches.
   */
  private static Map<Integer, RoaringBitmap> matchingDeletionsByFragment(
      Dataset ds, String predicate, List<Integer> scanFragmentIds) {
    Map<Integer, RoaringBitmap> deletionsByFragment = new java.util.HashMap<>();
    ScanOptions scanOptions =
        new ScanOptions.Builder()
            .columns(java.util.Collections.emptyList())
            .withRowAddress(true)
            .filter(predicate)
            .fragmentIds(scanFragmentIds)
            .build();
    try (LanceScanner scanner = ds.newScan(scanOptions);
        ArrowReader reader = scanner.scanBatches()) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        FieldVector rowAddrVector = batch.getVector(LanceConstant.ROW_ADDRESS);
        for (int i = 0; i < batch.getRowCount(); i++) {
          long rowAddress = ((Number) rowAddrVector.getObject(i)).longValue();
          deletionsByFragment
              .computeIfAbsent(extractFragmentId(rowAddress), k -> new RoaringBitmap())
              .add(extractRowIndex(rowAddress));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to scan rows for REPLACE ... WHERE " + predicate, e);
    }
    return deletionsByFragment;
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // For staged tables, the dataset is managed by StagedCommit (via abortStagedChanges)
    // For non-staged tables, no resources to clean up (dataset opened fresh at commit time)
  }

  @Override
  public String toString() {
    return String.format("LanceBatchWrite(datasetUri=%s)", writeOptions.getDatasetUri());
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final List<FragmentMetadata> fragments;

    TaskCommit(List<FragmentMetadata> fragments) {
      this.fragments = fragments;
    }

    List<FragmentMetadata> getFragments() {
      return fragments;
    }
  }
}
