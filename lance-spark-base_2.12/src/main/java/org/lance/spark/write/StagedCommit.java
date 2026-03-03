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
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;

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

  /** Creates a StagedCommit for an existing table (REPLACE or CREATE_OR_REPLACE on existing). */
  public static StagedCommit forExistingTable(
      Dataset dataset, Schema schema, LanceNamespace namespace, List<String> tableId) {
    return new StagedCommit(
        Optional.of(dataset),
        Collections.emptyList(),
        schema,
        null,
        null,
        false,
        namespace,
        tableId);
  }

  /** Creates a StagedCommit for a new table (CREATE or CREATE_OR_REPLACE on non-existing). */
  public static StagedCommit forNewTable(
      Schema schema,
      String datasetUri,
      Map<String, String> storageOptions,
      LanceNamespace namespace,
      List<String> tableId) {
    return new StagedCommit(
        Optional.empty(),
        Collections.emptyList(),
        schema,
        datasetUri,
        storageOptions,
        true,
        namespace,
        tableId);
  }

  private StagedCommit(
      Optional<Dataset> dataset,
      List<FragmentMetadata> fragments,
      Schema schema,
      String datasetUri,
      Map<String, String> storageOptions,
      boolean isNewTable,
      LanceNamespace namespace,
      List<String> tableId) {
    this.dataset = dataset;
    this.fragments = new ArrayList<>(fragments);
    this.schema = schema;
    this.datasetUri = datasetUri;
    this.storageOptions = storageOptions;
    this.isNewTable = isNewTable;
    this.namespace = namespace;
    this.tableId = tableId;
  }

  public void setFragments(List<FragmentMetadata> fragments) {
    this.fragments = fragments;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
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
    Overwrite operation = Overwrite.builder().fragments(fragments).schema(schema).build();
    try (Transaction txn = new Transaction.Builder().operation(operation).build();
        Dataset committed =
            new CommitBuilder(datasetUri, LanceRuntime.allocator())
                .writeParams(storageOptions)
                .execute(txn)) {
      // auto-close txn and committed dataset
    }
  }

  private void commitExistingTable() {
    Dataset ds = dataset.get();
    Overwrite operation = Overwrite.builder().fragments(fragments).schema(schema).build();
    try (Transaction txn =
            new Transaction.Builder().readVersion(ds.version()).operation(operation).build();
        Dataset committed = new CommitBuilder(ds).execute(txn)) {
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
