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
import org.lance.ReadOptions;
import org.lance.namespace.LanceNamespaceStorageOptionsProvider;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LanceBatchWrite implements BatchWrite {
  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
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

  /**
   * Dataset opened at start for existing tables to ensure version consistency. Empty for staged
   * operations (the dataset is managed by StagedCommit).
   */
  private final Optional<Dataset> dataset;

  private final StagedCommit stagedCommit;

  public LanceBatchWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      StagedCommit stagedCommit) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.stagedCommit = stagedCommit;

    // For staged operations, the dataset is managed by StagedCommit.
    // For non-staged operations, open to capture version for commit.
    this.dataset = (stagedCommit != null) ? Optional.empty() : Optional.of(openDataset());
  }

  private Dataset openDataset() {
    String uri = writeOptions.getDatasetUri();
    ReadOptions readOptions = buildReadOptions();
    return Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(uri)
        .readOptions(readOptions)
        .build();
  }

  private ReadOptions buildReadOptions() {
    Map<String, String> merged =
        LanceRuntime.mergeStorageOptions(writeOptions.getStorageOptions(), initialStorageOptions);
    LanceNamespaceStorageOptionsProvider provider =
        LanceRuntime.getOrCreateStorageOptionsProvider(namespaceImpl, namespaceProperties, tableId);

    ReadOptions.Builder builder = new ReadOptions.Builder().setStorageOptions(merged);
    if (provider != null) {
      builder.setStorageOptionsProvider(provider);
    }
    return builder.build();
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new LanceDataWriter.WriterFactory(
        schema, writeOptions, initialStorageOptions, namespaceImpl, namespaceProperties, tableId);
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
            .flatMap(List::stream)
            .collect(Collectors.toList());

    Schema arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", true, false);
    boolean isOverwrite = overwrite || writeOptions.isOverwrite();

    if (stagedCommit != null) {
      // For staged tables, update the eagerly-created StagedCommit with fragments and schema.
      // commitStagedChanges() will perform the actual commit.
      stagedCommit.setFragments(fragments);
      stagedCommit.setSchema(arrowSchema);
    } else {
      // For non-staged tables, commit immediately
      Dataset ds = dataset.get();
      try {
        Operation operation;
        if (isOverwrite) {
          operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
        } else {
          operation = Append.builder().fragments(fragments).build();
        }
        ds.newTransactionBuilder().operation(operation).build().commit();
      } finally {
        ds.close();
      }
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // For staged tables, the dataset is managed by StagedCommit (via abortStagedChanges)
    // For non-staged tables, close it here
    if (stagedCommit == null) {
      dataset.ifPresent(Dataset::close);
    }
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
