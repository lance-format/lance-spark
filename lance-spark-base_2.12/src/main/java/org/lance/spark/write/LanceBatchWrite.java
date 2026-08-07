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
import org.lance.memwal.ShardingSpec;
import org.lance.namespace.LanceNamespace;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.BlobSourceContext;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
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
   * Original Arrow Schema from the existing dataset. Used in overwrite mode to preserve the exact
   * schema (including unsigned types, FixedSizeList, etc.) that would otherwise be lost during
   * Spark to Arrow type conversion.
   */
  private final Schema originalArrowSchema;

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

    // Always read original schema to preserve unsigned/FSL types on overwrite.
    // For staged operations, the dataset is managed by StagedCommit.
    // For non-staged operations, pin the dataset version for OCC.
    if (stagedCommit != null) {
      this.writeOptions = writeOptions;
      Schema fetchedSchema = null;
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        fetchedSchema = ds.getSchema();
      } catch (IllegalArgumentException e) {
        // New dataset — no original schema to preserve
      }
      this.originalArrowSchema = fetchedSchema;
    } else {
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        this.originalArrowSchema =
            Objects.requireNonNull(ds.getSchema(), "Failed to get schema from existing dataset");
        this.writeOptions = writeOptions.withVersion(ds.version());
        logger.debug(
            "Resolved dataset version for batch write: {}", this.writeOptions.getVersion());
      }
    }
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    // In explicit truncate-overwrite mode, pass original schema JSON so executor writes with
    // correct Arrow types.
    String originalSchemaJson = null;
    if (overwrite && originalArrowSchema != null) {
      originalSchemaJson = originalArrowSchema.toJson();
    }
    return new LanceDataWriter.WriterFactory(
        schema,
        writeOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId,
        shardingSpec,
        blobSourceContexts,
        originalSchemaJson);
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
    boolean isOverwrite = overwrite;

    // In overwrite mode, original schema must exist and must remain compatible.
    if (isOverwrite) {
      if (originalArrowSchema == null) {
        throw new IllegalStateException(
            "Overwrite requires existing Lance schema, but none was found.");
      }
      if (!isTypeCompatible(originalArrowSchema, arrowSchema)) {
        throw new IllegalArgumentException(
            "Overwrite schema is incompatible with existing Lance schema. "
                + "Overwrite must not change schema type families.");
      }
      arrowSchema = originalArrowSchema;
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
        if (isOverwrite) {
          operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
        } else {
          operation = Append.builder().fragments(fragments).build();
        }
        CommitBuilder commitBuilder =
            new CommitBuilder(ds)
                .writeParams(
                    LanceRuntime.mergeStorageOptions(
                        writeOptions.getStorageOptions(), initialStorageOptions));
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

  // ==================== Schema compatibility helpers ====================

  /**
   * Checks whether the original schema is structurally compatible with the Spark-derived schema.
   * Compatible means same number of fields and each field pair is in the same "type family" (e.g.
   * int32 signed and int32 unsigned, List and FixedSizeList, Utf8 and LargeUtf8).
   */
  static boolean isTypeCompatible(Schema original, Schema spark) {
    if (original.getFields().size() != spark.getFields().size()) {
      return false;
    }
    for (int i = 0; i < original.getFields().size(); i++) {
      if (!isFieldCompatible(original.getFields().get(i), spark.getFields().get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isFieldCompatible(Field orig, Field spark) {
    ArrowType ot = orig.getType();
    ArrowType st = spark.getType();

    // Same type is always compatible
    if (ot.equals(st)) {
      return childrenCompatible(orig, spark);
    }

    // Integer family: allow Spark read-side widening for unsigned Lance ints,
    // while still requiring same field shape.
    if (ot instanceof ArrowType.Int && st instanceof ArrowType.Int) {
      ArrowType.Int oi = (ArrowType.Int) ot;
      ArrowType.Int si = (ArrowType.Int) st;
      if (oi.getBitWidth() == si.getBitWidth()) {
        return childrenCompatible(orig, spark);
      }
      // Unsigned widening mappings produced by LanceArrowUtils.fromArrowField:
      // uint8 -> int16, uint16 -> int32, uint32 -> int64.
      if (!oi.getIsSigned() && si.getIsSigned()) {
        if ((oi.getBitWidth() == 8 && si.getBitWidth() == 16)
            || (oi.getBitWidth() == 16 && si.getBitWidth() == 32)
            || (oi.getBitWidth() == 32 && si.getBitWidth() == 64)) {
          return childrenCompatible(orig, spark);
        }
      }
      return false;
    }

    // List family: List <-> FixedSizeList
    if (isListFamily(ot) && isListFamily(st)) {
      return childrenCompatible(orig, spark);
    }

    // String family: Utf8 <-> LargeUtf8
    if (isStringFamily(ot) && isStringFamily(st)) {
      return true;
    }

    // Binary family: Binary <-> LargeBinary <-> FixedSizeBinary
    if (isBinaryFamily(ot) && isBinaryFamily(st)) {
      return true;
    }

    return false;
  }

  private static boolean childrenCompatible(Field orig, Field spark) {
    if (orig.getChildren().size() != spark.getChildren().size()) {
      return false;
    }
    for (int i = 0; i < orig.getChildren().size(); i++) {
      if (!isFieldCompatible(orig.getChildren().get(i), spark.getChildren().get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isListFamily(ArrowType type) {
    return type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList;
  }

  private static boolean isStringFamily(ArrowType type) {
    return type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8;
  }

  private static boolean isBinaryFamily(ArrowType type) {
    return type instanceof ArrowType.Binary
        || type instanceof ArrowType.LargeBinary
        || type instanceof ArrowType.FixedSizeBinary;
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
