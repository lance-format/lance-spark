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
import org.lance.memwal.MemWalIndexDetails;
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.InsertIntoTableRequest;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.sharding.SparkLanceShardingUtils;
import org.lance.spark.utils.BlobReferenceResolver;
import org.lance.spark.utils.BlobSourceContext;
import org.lance.spark.utils.Utils;

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Writes Spark rows through the Lance Namespace insert API. */
public class NamespaceInsertBatchWrite implements BatchWrite {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceInsertBatchWrite.class);

  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;
  private final ShardingSpec shardingSpec;
  private final Map<String, BlobSourceContext> blobSourceContexts;

  public NamespaceInsertBatchWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      ShardingSpec shardingSpec,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.shardingSpec = shardingSpec;
    this.blobSourceContexts =
        blobSourceContexts == null ? Collections.emptyMap() : blobSourceContexts;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new WriterFactory(
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
    // The namespace insert API commits every request from executor tasks.
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // No compensating transaction is available from the current namespace insert API.
  }

  @Override
  public String toString() {
    return String.format("NamespaceInsertBatchWrite(tableId=%s)", tableId);
  }

  static final class TaskCommit implements WriterCommitMessage {
    private final long rowsInserted;
    private final int insertRequests;

    TaskCommit(long rowsInserted, int insertRequests) {
      this.rowsInserted = rowsInserted;
      this.insertRequests = insertRequests;
    }

    long rowsInserted() {
      return rowsInserted;
    }

    int insertRequests() {
      return insertRequests;
    }
  }

  static class WriterFactory implements DataWriterFactory {
    private final StructType schema;
    private final LanceSparkWriteOptions writeOptions;
    private final Map<String, String> initialStorageOptions;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;
    private final ShardingSpecSnapshot shardingSpec;
    private final Map<String, BlobSourceContext> blobSourceContexts;

    WriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        ShardingSpec shardingSpec,
        Map<String, BlobSourceContext> blobSourceContexts) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.shardingSpec =
          SparkLanceShardingUtils.isEmpty(shardingSpec)
              ? null
              : ShardingSpecSnapshot.from(shardingSpec);
      this.blobSourceContexts =
          blobSourceContexts == null ? Collections.emptyMap() : blobSourceContexts;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      Preconditions.checkArgument(
          namespaceImpl != null && tableId != null,
          "namespace insert writes require a namespace-backed table");

      ShardingBatchKeyEvaluator shardingKeyEvaluator =
          shardingSpec == null
              ? null
              : new ShardingBatchKeyEvaluator(schema, writeOptions, shardingBinding());
      BlobReferenceResolver resolver = new BlobReferenceResolver(blobSourceContexts);
      LanceNamespace namespace =
          LanceRuntime.getOrCreateNamespace(namespaceImpl, namespaceProperties);
      return new NamespaceInsertDataWriter(
          schema, writeOptions, namespace, tableId, shardingKeyEvaluator, resolver);
    }

    private ShardingBatchKeyEvaluator.ShardingBinding shardingBinding() {
      try (Dataset dataset =
          Utils.openDatasetBuilder(
                  writeOptions.toBuilder()
                      .storageOptions(
                          LanceRuntime.mergeStorageOptions(
                              writeOptions.getStorageOptions(), initialStorageOptions))
                      .build())
              .build()) {
        Optional<MemWalIndexDetails> details = dataset.memWalIndexDetails();
        if (details.isPresent() && !details.get().shardingSpecs().isEmpty()) {
          return new ShardingBatchKeyEvaluator.ShardingBinding(
              details.get().shardingSpecs().get(0), dataset.getLanceSchema());
        }
      } catch (RuntimeException e) {
        if (shardingSpec.hasSourceIds()) {
          throw e;
        }
        LOG.warn("Falling back to in-memory sharding metadata for namespace insert write", e);
      }
      return new ShardingBatchKeyEvaluator.ShardingBinding(shardingSpec.toShardingSpec(), null);
    }
  }

  static class NamespaceInsertDataWriter implements DataWriter<InternalRow> {
    private final StructType sparkSchema;
    private final LanceSparkWriteOptions writeOptions;
    private final LanceNamespace namespace;
    private final List<String> tableId;
    private final ShardingBatchKeyEvaluator shardingKeyEvaluator;
    private final BlobReferenceResolver blobResolver;
    private final Schema arrowSchema;

    private BufferAllocator batchAllocator;
    private VectorSchemaRoot root;
    private org.lance.spark.arrow.LanceArrowWriter arrowWriter;
    private int rowCount;
    private long rowsInserted;
    private int insertRequests;
    private Object lastKey;
    private boolean hasRowsInCurrentBatch;

    NamespaceInsertDataWriter(
        StructType sparkSchema,
        LanceSparkWriteOptions writeOptions,
        LanceNamespace namespace,
        List<String> tableId,
        ShardingBatchKeyEvaluator shardingKeyEvaluator,
        BlobReferenceResolver blobResolver) {
      this.sparkSchema = sparkSchema;
      this.writeOptions = writeOptions;
      this.namespace = Objects.requireNonNull(namespace, "namespace");
      this.tableId = new ArrayList<>(Objects.requireNonNull(tableId, "tableId"));
      this.shardingKeyEvaluator = shardingKeyEvaluator;
      this.blobResolver = blobResolver;
      this.arrowSchema =
          LanceArrowUtils.toArrowSchema(
              sparkSchema, "UTC", false, writeOptions.isUseLargeVarTypes());
      allocateBatch();
    }

    @Override
    public void write(InternalRow record) throws IOException {
      if (shardingKeyEvaluator != null) {
        shardingKeyEvaluator.write(record, this::writePartitionedRow);
        return;
      }
      writeRow(record);
    }

    private void writePartitionedRow(InternalRow row, Object key) throws IOException {
      if (!hasRowsInCurrentBatch) {
        lastKey = key;
      } else if (!Objects.equals(key, lastKey)) {
        flush();
        lastKey = key;
      }
      writeRow(row);
      hasRowsInCurrentBatch = rowCount > 0;
    }

    private void writeRow(InternalRow row) throws IOException {
      if (rowCount >= writeOptions.getBatchSize()) {
        flush();
      }
      arrowWriter.write(row);
      rowCount++;

      long currentBatchBytes =
          batchAllocator.getAllocatedMemory()
              + org.lance.spark.arrow.LanceArrowWriteBridge$.MODULE$.estimatedBufferedBytes(
                  arrowWriter);
      if (rowCount >= writeOptions.getBatchSize()
          || (rowCount > 0 && currentBatchBytes >= writeOptions.getMaxBatchBytes())) {
        flush();
      }
    }

    private void flush() throws IOException {
      if (rowCount == 0) {
        return;
      }

      arrowWriter.finish();
      root.setRowCount(rowCount);
      byte[] requestData = serializeCurrentBatch();
      InsertIntoTableRequest request = new InsertIntoTableRequest().id(tableId).mode("append");
      try {
        namespace.insertIntoTable(request, requestData);
      } catch (RuntimeException e) {
        throw new IOException("Failed to insert rows through Lance namespace", e);
      }

      rowsInserted += rowCount;
      insertRequests++;
      hasRowsInCurrentBatch = false;
      allocateBatch();
    }

    private byte[] serializeCurrentBatch() throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, null, out)) {
        streamWriter.start();
        streamWriter.writeBatch();
        streamWriter.end();
      } finally {
        closeBatch();
      }
      return out.toByteArray();
    }

    private void allocateBatch() {
      closeBatch();
      batchAllocator =
          LanceRuntime.allocator()
              .newChildAllocator("namespace-insert-write-batch", 0, Long.MAX_VALUE);
      root = VectorSchemaRoot.create(arrowSchema, batchAllocator);
      arrowWriter =
          org.lance.spark.arrow.LanceArrowWriteBridge$.MODULE$.createWithResolver(
              root, sparkSchema, blobResolver);
      rowCount = 0;
    }

    private void closeBatch() {
      if (root != null) {
        root.close();
        root = null;
        arrowWriter = null;
      }
      if (batchAllocator != null) {
        batchAllocator.close();
        batchAllocator = null;
      }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      if (shardingKeyEvaluator != null) {
        shardingKeyEvaluator.flush(this::writePartitionedRow);
      }
      flush();
      return new TaskCommit(rowsInserted, insertRequests);
    }

    @Override
    public void abort() throws IOException {
      close();
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      try {
        if (shardingKeyEvaluator != null) {
          shardingKeyEvaluator.close();
        }
      } catch (RuntimeException e) {
        failure = new IOException("Failed to close sharding evaluator", e);
      }
      try {
        closeBatch();
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = new IOException("Failed to close Arrow batch", e);
        } else {
          failure.addSuppressed(e);
        }
      }
      try {
        blobResolver.close();
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = new IOException("Failed to close blob resolver", e);
        } else {
          failure.addSuppressed(e);
        }
      }
      try {
        if (namespace instanceof Closeable) {
          ((Closeable) namespace).close();
        }
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  private static final class ShardingSpecSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int specId;
    private final List<ShardingFieldSnapshot> fields;

    private ShardingSpecSnapshot(int specId, List<ShardingFieldSnapshot> fields) {
      this.specId = specId;
      this.fields = fields;
    }

    private static ShardingSpecSnapshot from(ShardingSpec spec) {
      List<ShardingFieldSnapshot> fields = new ArrayList<>();
      for (ShardingField field : spec.fields()) {
        fields.add(ShardingFieldSnapshot.from(field));
      }
      return new ShardingSpecSnapshot(spec.specId(), fields);
    }

    private ShardingSpec toShardingSpec() {
      List<ShardingField> restored = new ArrayList<>();
      for (ShardingFieldSnapshot field : fields) {
        restored.add(field.toShardingField());
      }
      return new ShardingSpec(specId, restored);
    }

    private boolean hasSourceIds() {
      for (ShardingFieldSnapshot field : fields) {
        if (!field.sourceIds.isEmpty()) {
          return true;
        }
      }
      return false;
    }
  }

  private static final class ShardingFieldSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String fieldId;
    private final List<Integer> sourceIds;
    private final String transform;
    private final String expression;
    private final String resultType;
    private final Map<String, String> parameters;

    private ShardingFieldSnapshot(
        String fieldId,
        List<Integer> sourceIds,
        String transform,
        String expression,
        String resultType,
        Map<String, String> parameters) {
      this.fieldId = fieldId;
      this.sourceIds = sourceIds;
      this.transform = transform;
      this.expression = expression;
      this.resultType = resultType;
      this.parameters = parameters;
    }

    private static ShardingFieldSnapshot from(ShardingField field) {
      return new ShardingFieldSnapshot(
          field.fieldId(),
          new ArrayList<>(field.sourceIds()),
          field.transform().orElse(null),
          field.expression().orElse(null),
          field.resultType(),
          new HashMap<>(field.parameters()));
    }

    private ShardingField toShardingField() {
      return new ShardingField(fieldId, sourceIds, transform, expression, resultType, parameters);
    }
  }
}
