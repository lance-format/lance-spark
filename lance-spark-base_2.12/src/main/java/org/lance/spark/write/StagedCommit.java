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
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.DatasetConfigUtils;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Holds the state needed to commit a staged table operation (CREATE, REPLACE, CREATE_OR_REPLACE).
 * This is created eagerly in the catalog's stage methods so that schema-only operations (no data
 * written) can still commit successfully. Writers update fragments/schema via setters. Staged
 * commits always use Overwrite operation.
 */
public class StagedCommit {
  private static final Logger LOG = LoggerFactory.getLogger(StagedCommit.class);

  private static final String NO_DATASET_URI = null;
  private static final List<FragmentMetadata> NO_INITIAL_FRAGMENTS = Collections.emptyList();

  private Boolean enableStableRowIds;
  private List<FragmentMetadata> fragments;
  private Schema schema;

  /** Dataset for existing tables. Empty for new tables (staged create). */
  private final Optional<Dataset> dataset;

  // For new tables - info needed to create the dataset at commit time
  private final String datasetUri;
  private final Map<String, String> storageOptions;

  private final boolean isNewTable;
  private final LanceNamespace namespace;
  private final List<String> tableId;
  private final boolean managedVersioning;

  /** Creates a StagedCommit for an existing table (REPLACE or CREATE_OR_REPLACE on existing). */
  public static StagedCommit forExistingTable(
      final Dataset dataset, final Schema schema, final StagedCommitOptions options) {
    return new StagedCommit(
        Optional.of(dataset), NO_INITIAL_FRAGMENTS, schema, NO_DATASET_URI, options);
  }

  /** Creates a StagedCommit for a new table (CREATE or CREATE_OR_REPLACE on non-existing). */
  public static StagedCommit forNewTable(
      final Schema schema, final String datasetUri, final StagedCommitOptions options) {
    return new StagedCommit(Optional.empty(), NO_INITIAL_FRAGMENTS, schema, datasetUri, options);
  }

  private StagedCommit(
      final Optional<Dataset> dataset,
      final List<FragmentMetadata> fragments,
      final Schema schema,
      final String datasetUri,
      final StagedCommitOptions options) {
    this.dataset = dataset;
    this.fragments = new ArrayList<>(fragments);
    this.schema = schema;
    this.datasetUri = datasetUri;
    this.storageOptions = options.getStorageOptions();
    this.isNewTable = datasetUri != null;
    this.enableStableRowIds = options.getEnableStableRowIds();
    this.namespace = options.getNamespace();
    this.tableId = options.getTableId();
    this.managedVersioning = options.isManagedVersioning();
  }

  public void setFragments(List<FragmentMetadata> fragments) {
    this.fragments = fragments;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public void setEnableStableRowIds(Boolean enableStableRowIds) {
    this.enableStableRowIds = enableStableRowIds;
  }

  /** Performs the actual commit using the stored dataset and fragments. */
  public void commit() {
    if (dataset.isEmpty()) {
      commitNewTable();
    } else {
      commitExistingTable();
    }
  }

  private void commitNewTable() {
    if (Boolean.TRUE.equals(enableStableRowIds)) {
      createTableWithStableRowIds();
      appendFragmentsIfPresent();
    } else {
      createTableViaOverwrite();
    }
  }

  private void createTableWithStableRowIds() {
    try (Dataset created =
        Dataset.write()
            .allocator(LanceRuntime.allocator())
            .uri(datasetUri)
            .schema(schema)
            .mode(WriteParams.WriteMode.CREATE)
            .enableStableRowIds(true)
            .storageOptions(storageOptions)
            .execute()) {
      // enableStableRowIds(true) sets the internal Rust manifest flag but
      // does not populate the user-facing config map. Explicitly set it so
      // that Dataset.getConfig().get("enable_stable_row_ids") returns "true".
      DatasetConfigUtils.setConfigEntry(
          created, LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS, "true");
    }
  }

  private void appendFragmentsIfPresent() {
    if (fragments.isEmpty()) {
      return;
    }
    try (Dataset ds = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      final Append operation = Append.builder().fragments(fragments).build();
      final CommitBuilder builder = newCommitBuilder(ds);
      builder.useStableRowIds(true);
      commitOperation(builder, ds.version(), operation);
    }
  }

  private void createTableViaOverwrite() {
    final Overwrite operation = Overwrite.builder().fragments(fragments).schema(schema).build();
    final CommitBuilder builder =
        new CommitBuilder(datasetUri, LanceRuntime.allocator()).writeParams(storageOptions);
    applyManagedVersioning(builder);
    try (Transaction txn = new Transaction.Builder().operation(operation).build();
        Dataset committed = builder.execute(txn)) {
      // auto-close
    }
  }

  private void commitExistingTable() {
    final Dataset ds = dataset.get();
    final String uri = ds.uri();
    final long version = ds.version();
    ds.close();

    final Overwrite operation = Overwrite.builder().fragments(fragments).schema(schema).build();
    final CommitBuilder builder =
        new CommitBuilder(uri, LanceRuntime.allocator()).writeParams(storageOptions);
    if (enableStableRowIds != null) {
      builder.useStableRowIds(enableStableRowIds);
    }
    applyManagedVersioning(builder);
    commitOperation(builder, version, operation);
  }

  private CommitBuilder newCommitBuilder(final Dataset dataset) {
    final CommitBuilder builder = new CommitBuilder(dataset).writeParams(storageOptions);
    applyManagedVersioning(builder);
    return builder;
  }

  private void applyManagedVersioning(final CommitBuilder builder) {
    if (managedVersioning) {
      builder.namespace(namespace).tableId(tableId);
    }
  }

  private static void commitOperation(
      final CommitBuilder builder,
      final long readVersion,
      final org.lance.operation.Operation operation) {
    try (Transaction txn =
            new Transaction.Builder().readVersion(readVersion).operation(operation).build();
        Dataset committed = builder.execute(txn)) {
      // auto-close txn and committed dataset
    }
  }

  /** Closes the dataset without committing. Used for abort scenarios. */
  public void close() {
    dataset.ifPresent(Dataset::close);
  }

  /**
   * Aborts the staged operation by closing the dataset and deregistering the table if it was newly
   * created.
   */
  public void abort() {
    close();
    if (isNewTable && namespace != null) {
      DeregisterTableRequest req = new DeregisterTableRequest();
      tableId.forEach(req::addIdItem);
      try {
        namespace.deregisterTable(req);
      } catch (Exception e) {
        LOG.warn(
            "Failed to deregister table {} during abort. Manual cleanup may be required.",
            tableId,
            e);
      }
    }
  }
}
