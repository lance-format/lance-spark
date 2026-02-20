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
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Holds the state needed to commit a staged table operation (CREATE, REPLACE, CREATE_OR_REPLACE).
 * This is used to defer the actual commit from LanceBatchWrite.commit() to
 * LanceDataset.commitStagedChanges(). Staged commits always use Overwrite operation.
 */
public class StagedCommit {
  private final List<FragmentMetadata> fragments;
  private final Schema schema;

  /** Dataset for existing tables. Empty for new tables (staged create). */
  private final Optional<Dataset> dataset;

  // For new tables - info needed to create the dataset at commit time
  private final String datasetUri;
  private final Map<String, String> storageOptions;

  /** Creates a StagedCommit for an existing table (REPLACE or CREATE_OR_REPLACE on existing). */
  public static StagedCommit forExistingTable(
      Dataset dataset, List<FragmentMetadata> fragments, Schema schema) {
    return new StagedCommit(Optional.of(dataset), fragments, schema, null, null);
  }

  /** Creates a StagedCommit for a new table (CREATE or CREATE_OR_REPLACE on non-existing). */
  public static StagedCommit forNewTable(
      List<FragmentMetadata> fragments,
      Schema schema,
      String datasetUri,
      Map<String, String> storageOptions) {
    return new StagedCommit(Optional.empty(), fragments, schema, datasetUri, storageOptions);
  }

  private StagedCommit(
      Optional<Dataset> dataset,
      List<FragmentMetadata> fragments,
      Schema schema,
      String datasetUri,
      Map<String, String> storageOptions) {
    this.dataset = dataset;
    this.fragments = fragments;
    this.schema = schema;
    this.datasetUri = datasetUri;
    this.storageOptions = storageOptions;
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
    // TODO: This should use namespace and tableId with the Transaction API to create the table.
    // Currently using URI-based creation as a workaround because:
    // 1. Transaction API doesn't support creating new datasets
    //    (throws UnsupportedOperationException)
    // 2. Namespace API doesn't have a method to finalize a declared table with fragments
    // Once the SDK supports Transaction.commit() for new datasets with
    // LanceNamespaceStorageOptionsProvider, switch to that approach for proper credential
    // refresh support. The table was already declared via namespace.declareTable().
    try (Dataset ds =
        Dataset.write()
            .allocator(LanceRuntime.allocator())
            .uri(datasetUri)
            .schema(schema)
            .mode(WriteParams.WriteMode.CREATE)
            .storageOptions(storageOptions)
            .execute()) {
      Operation operation = Overwrite.builder().fragments(fragments).schema(schema).build();
      ds.newTransactionBuilder().operation(operation).build().commit();
    }
  }

  private void commitExistingTable() {
    Operation operation = Overwrite.builder().fragments(fragments).schema(schema).build();
    dataset.get().newTransactionBuilder().operation(operation).build().commit();
  }

  /** Closes the dataset without committing. Used for abort scenarios. */
  public void close() {
    dataset.ifPresent(Dataset::close);
  }
}
