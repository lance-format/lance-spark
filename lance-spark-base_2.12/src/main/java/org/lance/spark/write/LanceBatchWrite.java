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
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

  /**
   * Partition column names — fragments will be rolled at transitions so each fragment contains
   * exactly one partition value. Empty when no partitioning is requested.
   */
  private final List<String> partitionColumns;

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
        Collections.emptyList());
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
      List<String> partitionColumns) {
    this.schema = schema;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.stagedCommit = stagedCommit;
    this.partitionColumns = partitionColumns == null ? Collections.emptyList() : partitionColumns;

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
        partitionColumns);
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
    } else {
      // For non-staged tables, commit immediately
      long version =
          Objects.requireNonNull(
              writeOptions.getVersion(),
              "version must be set (resolved in LanceBatchWrite constructor)");
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        Operation operation;
        if (isOverwrite) {
          operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
        } else {
          operation = Append.builder().fragments(fragments).build();
        }
        CommitBuilder commitBuilder =
            new CommitBuilder(ds).writeParams(writeOptions.getStorageOptions());
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
          commitBuilder.namespaceClient(namespace).tableId(tableId);
        }
        try (Transaction txn =
                new Transaction.Builder().readVersion(version).operation(operation).build();
            Dataset committed = commitBuilder.execute(txn)) {
          // auto-close txn and committed dataset
        }
      }
    }
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
