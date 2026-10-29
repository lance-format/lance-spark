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
import org.lance.memwal.ShardingSpec;
import org.lance.namespace.LanceNamespace;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.BlobSourceContext;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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

    // A staged write creates or replaces the table, so its columns take Spark's types and the
    // dataset is managed by StagedCommit. Any other write goes into the existing dataset: pin its
    // version for OCC and keep its unsigned integer columns unsigned.
    if (stagedCommit != null) {
      this.schema = schema;
      this.writeOptions = writeOptions;
    } else {
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        this.schema = LanceArrowUtils.markUnsignedForWrite(schema, ds.getSchema());
        this.writeOptions = writeOptions.withRef(LanceRef.ofMain(ds.version()));
        logger.debug("Resolved dataset ref for batch write: {}", this.writeOptions.getRef());
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

    Schema arrowSchema = getCommitSchema();

    // Lance-Java rejects empty fragments, so empty appends are a noop; empty overwrites truncate.
    if (fragments.isEmpty() && !isOverwrite()) {
      return;
    }

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
                  writeOptions.getRef(),
                  "ref must be set (resolved in LanceBatchWrite constructor)")
              .getVersionNumber()
              .get();
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        Operation operation;
        if (isOverwrite()) {
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

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // For staged tables, the dataset is managed by StagedCommit (via abortStagedChanges)
    // For non-staged tables, no resources to clean up (dataset opened fresh at commit time)
  }

  @Override
  public String toString() {
    return String.format("LanceBatchWrite(datasetUri=%s)", writeOptions.getDatasetUri());
  }

  /**
   * Returns whether {@link #commit(WriterCommitMessage[])} replaces the table's schema with {@link
   * #getCommitSchema()}. This is true for an overwrite, including one requested through the {@code
   * write_mode} option while the query appends, and for a staged create or replace, which always
   * commits as an overwrite. Otherwise the commit appends and keeps the table's schema.
   */
  @Evolving
  public boolean isOverwrite() {
    return stagedCommit != null || overwrite || writeOptions.isOverwrite();
  }

  /**
   * Returns the Arrow schema that {@link #commit(WriterCommitMessage[])} commits when {@link
   * #isOverwrite()} is true. It is derived from the write's Spark schema, the unsigned integer
   * types of an existing table's columns, and resolved options, such as {@code
   * use_large_var_types}, which an external writer cannot see otherwise.
   */
  @Evolving
  public Schema getCommitSchema() {
    return LanceArrowUtils.toArrowSchema(schema, "UTC", true, writeOptions.isUseLargeVarTypes());
  }

  /**
   * Returns the Lance write parameters {@link #commit(WriterCommitMessage[])} expects for fragments
   * an external writer produces, such as the resolved {@code file_format_version}. See {@link
   * FragmentWriteSpec#toWriteParams()} for what they contain.
   */
  @Evolving
  public FragmentWriteSpec getFragmentWriteSpec() {
    LanceSparkWriteOptions fragmentWriteOptions =
        writeOptions.toBuilder()
            .writeMode(
                isOverwrite() ? WriteParams.WriteMode.OVERWRITE : WriteParams.WriteMode.APPEND)
            .enableStableRowIds(false)
            .fileFormatVersion(effectiveFileFormatVersion())
            .build();
    return new FragmentWriteSpec(fragmentWriteOptions, initialStorageOptions);
  }

  /**
   * The file format version this write resolves: the write option, or for a staged commit the
   * version resolved when it was staged. Null means none was resolved, so fragments get Lance's
   * default.
   */
  private String effectiveFileFormatVersion() {
    String version = writeOptions.getFileFormatVersion();
    if (version == null && stagedCommit != null) {
      version = stagedCommit.getFileFormatVersion();
    }
    return version;
  }

  /**
   * Creates an opaque commit message for uncommitted fragments written for a target dataset.
   *
   * <p>The returned message must be passed to {@link #commit(WriterCommitMessage[])} on a {@code
   * LanceBatchWrite} for the same target dataset.
   *
   * <p>Write the fragments with the parameters of {@link #getFragmentWriteSpec()}. When {@link
   * #isOverwrite()} is true, do not pass the table's schema to {@code
   * WriteFragmentBuilder.schema(...)}, and give the stream the column types of {@link
   * #getCommitSchema()}. Timestamps must use the same unit, integers the same width and signedness,
   * and strings and binaries the same offset width. The stream may leave out a timestamp's time
   * zone.
   *
   * <p>{@code commit} does not validate these fragments: not their file format version, field ids
   * or column types. Fragments written in the wrong mode, or with the wrong schema or types, can
   * commit and then read back as nulls or wrong values, or leave the table unreadable.
   *
   * <p>Whether or not {@link #isOverwrite()} is true, reject a value outside an unsigned integer
   * column's range, as {@link LanceDataWriter} does: narrowing it stores a different value, which
   * Lance accepts.
   *
   * @param fragments uncommitted fragments produced by a task
   * @return a message accepted by {@link #commit(WriterCommitMessage[])}
   * @throws NullPointerException if {@code fragments} is null or contains a null element
   */
  @Evolving
  public static WriterCommitMessage taskCommit(List<FragmentMetadata> fragments) {
    Objects.requireNonNull(fragments, "fragments must not be null");
    List<FragmentMetadata> copy = new ArrayList<>(fragments.size());
    for (FragmentMetadata fragment : fragments) {
      copy.add(Objects.requireNonNull(fragment, "fragments must not contain null"));
    }
    return new TaskCommit(copy);
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
