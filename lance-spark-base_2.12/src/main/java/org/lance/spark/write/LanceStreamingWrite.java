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

import org.lance.FragmentMetadata;
import org.lance.memwal.ShardingSpec;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.BlobSourceContext;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Spark Structured Streaming sink for Lance.
 *
 * <p>Each micro-batch produces a single Lance transaction stamped with the streaming queryId and
 * epochId. Replay dedupe scans recent transaction history for the same (queryId, epochId) pair; see
 * {@link StreamingCommitProtocol}.
 */
public class LanceStreamingWrite implements StreamingWrite {
  private static final Logger LOG = LoggerFactory.getLogger(LanceStreamingWrite.class);

  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
  private final boolean overwrite;
  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;
  private final boolean managedVersioning;
  private final ShardingSpec shardingSpec;
  private final Map<String, BlobSourceContext> blobSourceContexts;
  private final String queryId;

  public LanceStreamingWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean managedVersioning,
      ShardingSpec shardingSpec,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this.queryId = requireStreamingQueryId(writeOptions.getStreamingQueryId());
    this.schema = schema;
    // Streaming intentionally does NOT pin the dataset version. A long-running query produces
    // many epochs; each commit must open the dataset at the current latest version so the
    // dedupe scan window and the transaction's readVersion both reflect on-disk reality.
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.shardingSpec = shardingSpec;
    this.blobSourceContexts =
        blobSourceContexts == null ? Collections.emptyMap() : blobSourceContexts;
  }

  private static String requireStreamingQueryId(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Structured Streaming writes to Lance require the '"
              + LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID
              + "' option (a stable, globally unique query id used as the idempotency key).");
    }
    // Trim once and use the trimmed value as the durable idempotency key, so the value validated
    // here is byte-for-byte the value stamped into and compared against the transaction
    // properties. Otherwise a queryId differing only by surrounding whitespace would silently
    // fail to dedupe against itself.
    return value.trim();
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    LanceDataWriter.WriterFactory inner =
        new LanceDataWriter.WriterFactory(
            schema,
            writeOptions,
            initialStorageOptions,
            namespaceImpl,
            namespaceProperties,
            tableId,
            shardingSpec,
            blobSourceContexts);
    return new EpochAwareWriterFactory(inner);
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    List<FragmentMetadata> fragments =
        Arrays.stream(messages)
            .filter(m -> m != null)
            .map(m -> (LanceBatchWrite.TaskCommit) m)
            .map(LanceBatchWrite.TaskCommit::getFragments)
            .map(LanceDataWriter::stripRowIdMeta)
            .flatMap(List::stream)
            .collect(Collectors.toList());

    Schema arrowSchema =
        LanceArrowUtils.toArrowSchema(schema, "UTC", true, writeOptions.isUseLargeVarTypes());
    // On the streaming path the output mode is the ONLY overwrite signal: Complete mode drives
    // Spark's truncate() which sets `overwrite`. We deliberately ignore writeOptions.isOverwrite()
    // (a batch write_mode option / catalog default) — otherwise a stray `write_mode=overwrite`
    // would silently turn an Append stream into a per-epoch truncate, wiping the table on every
    // (even empty) epoch.
    boolean isOverwrite = overwrite;

    StreamingCommitProtocol.commitEpoch(
        writeOptions,
        initialStorageOptions,
        fragments,
        arrowSchema,
        isOverwrite,
        queryId,
        epochId,
        namespaceImpl,
        namespaceProperties,
        tableId,
        managedVersioning);
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    // No driver-side resources to release. Per-task writers release their own buffers/resolvers on
    // close(); any fragment data files already written by a task whose epoch then aborts are left
    // unreferenced (no transaction committed) and reclaimed by Lance cleanup/VACUUM — identical to
    // the batch abort model (LanceBatchWrite#abort is likewise a no-op).
    LOG.debug("streaming epoch aborted queryId={} epochId={}", queryId, epochId);
  }

  /**
   * Lance commits are atomic via {@link org.lance.CommitBuilder}; we do not need Spark's commit
   * coordinator. Returning {@code false} here keeps the commit path identical to {@link
   * LanceBatchWrite#useCommitCoordinator()}.
   *
   * <p>Annotation intentionally omitted: this method is a {@code default} on {@link StreamingWrite}
   * in Spark 3.5+ but not declared at all in 3.4. Omitting {@code @Override} keeps this class
   * compilable across the supported version matrix. On 3.4 the method is absent from the interface,
   * so this {@code false} is inert there; it only takes effect on 3.5+. Lance's atomic commit makes
   * the coordinator immaterial either way.
   */
  public boolean useCommitCoordinator() {
    return false;
  }

  /**
   * Adapter that wraps the existing batch {@link LanceDataWriter.WriterFactory} so it can fulfill
   * the streaming factory contract. The {@code epochId} is ignored on the writer side — Lance
   * fragments produced by a task are written under fresh UUIDs each epoch and are surfaced back to
   * the driver via the {@link WriterCommitMessage}; the streaming identity is applied at commit
   * time, not per-row.
   */
  static final class EpochAwareWriterFactory implements StreamingDataWriterFactory {
    private static final long serialVersionUID = 1L;
    private final LanceDataWriter.WriterFactory delegate;

    EpochAwareWriterFactory(LanceDataWriter.WriterFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
      return delegate.createWriter(partitionId, taskId);
    }
  }
}
