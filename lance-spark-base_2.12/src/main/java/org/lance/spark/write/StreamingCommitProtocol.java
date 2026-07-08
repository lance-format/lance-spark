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
import org.lance.delta.DatasetDelta;
import org.lance.delta.DatasetDeltaBuilder;
import org.lance.namespace.LanceNamespace;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Driver-side commit helper for Structured Streaming.
 *
 * <p>Each non-empty micro-batch is committed in a <b>single</b> Lance {@link Transaction} stamped
 * with {@link LanceSparkWriteOptions#STREAMING_QUERY_ID_PROP} and {@link
 * LanceSparkWriteOptions#STREAMING_EPOCH_ID_PROP} in the transaction properties. Replay dedupe
 * scans recent transaction history via {@link DatasetDelta#listTransactions()} for an existing
 * (queryId, epochId) pair — finding one means the previous attempt already committed, so the
 * current call is a no-op.
 *
 * <p>Empty <b>append</b> epochs are skipped entirely (no transaction): Spark's checkpoint advances
 * regardless, and replays find no prior commit, see no fragments, and skip again. Empty
 * <b>overwrite</b> (Complete-mode) epochs still commit one empty Overwrite to truncate the table to
 * the new empty result — skipping would leave the prior epoch's rows on disk.
 *
 * <p><b>Failure window:</b> the scan window covers the last {@code streamingDedupeLookbackVersions}
 * versions, so a duplicate can slip through once {@code streamingDedupeLookbackVersions} or more
 * unrelated commits land between a crash and the restart — that many intervening commits push the
 * prior commit out of the window. Users with very high commit churn can raise the lookback up to
 * {@link LanceSparkWriteOptions#MAX_STREAMING_DEDUPE_LOOKBACK_VERSIONS}.
 */
final class StreamingCommitProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingCommitProtocol.class);

  private StreamingCommitProtocol() {}

  /**
   * Commits a streaming micro-batch. Empty {@code fragments} are skipped in append mode, but in
   * overwrite (Complete) mode still commit an empty Overwrite to truncate the table.
   *
   * @return {@code true} if a transaction was issued, {@code false} on dedupe or empty-append skip.
   */
  static boolean commitEpoch(
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      List<FragmentMetadata> fragments,
      Schema arrowSchema,
      boolean isOverwrite,
      String queryId,
      long epochId,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean managedVersioning) {
    Boolean enableStableRowIds = writeOptions.getEnableStableRowIds();

    try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
      long currentVersion = ds.version();

      Optional<Long> replayVersion = findReplay(ds, currentVersion, queryId, epochId, writeOptions);
      if (replayVersion.isPresent()) {
        LOG.info(
            "streaming epoch already committed (replay); queryId={} epochId={} foundInVersion={}",
            queryId,
            epochId,
            replayVersion.get());
        return false;
      }

      // Append: an empty epoch issues no transaction — the version does not advance, and a replay
      // of an empty epoch finds no prior commit and skips again. Overwrite (Complete output mode):
      // an empty epoch must STILL commit an empty Overwrite to truncate the table to the new
      // (empty) result; skipping would leave the previous epoch's rows on disk, silently violating
      // Complete-mode semantics. Both branches run only after the replay-dedupe check above, so a
      // replayed empty overwrite remains a no-op.
      if (fragments.isEmpty() && !isOverwrite) {
        LOG.info(
            "streaming empty append epoch skipped (no fragments); queryId={} epochId={}",
            queryId,
            epochId);
        return false;
      }

      Operation operation;
      if (isOverwrite) {
        operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
      } else {
        operation = Append.builder().fragments(fragments).build();
      }

      Map<String, String> txnProps = new HashMap<>(2);
      txnProps.put(LanceSparkWriteOptions.STREAMING_QUERY_ID_PROP, queryId);
      txnProps.put(LanceSparkWriteOptions.STREAMING_EPOCH_ID_PROP, Long.toString(epochId));

      // Merge the namespace-vended credentials (from describeTable, carried in
      // initialStorageOptions) on top of the static write storage options — mirroring
      // LanceBatchWrite.commit. Without this, streaming commits to a credential-vending
      // namespace/object-store table would authenticate with the static options only.
      CommitBuilder commitBuilder =
          new CommitBuilder(ds)
              .writeParams(
                  LanceRuntime.mergeStorageOptions(
                      writeOptions.getStorageOptions(), initialStorageOptions));
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
              new Transaction.Builder()
                  .readVersion(currentVersion)
                  .operation(operation)
                  .transactionProperties(txnProps)
                  .build();
          Dataset committed = commitBuilder.execute(txn)) {
        LOG.info(
            "streaming epoch committed queryId={} epochId={} fragments={} version={}",
            queryId,
            epochId,
            fragments.size(),
            committed.version());
        return true;
      }
    }
  }

  /**
   * Scans transaction history backwards from {@code currentVersion} for at most {@code lookback}
   * versions, returning the version where {@code (queryId, epochId)} was committed if found.
   *
   * <p>The Lance Transaction API does not expose the exact committed version, and {@code
   * listTransactions()} may be sparse (lance-core omits versions that have no transaction file), so
   * the index-derived version is a best-effort estimate used only for logging — the skip decision
   * keys solely on the {@code (queryId, epochId)} property match, which is unaffected by
   * sparseness.
   *
   * <p>If the scan fails (e.g. a version in the window was removed by VACUUM/cleanup, which makes
   * {@code listTransactions()} throw), this returns {@link Optional#empty()} and logs a WARN —
   * degrading to the documented bounded at-least-once fallback rather than failing the epoch and
   * wedging the query on retry.
   */
  private static Optional<Long> findReplay(
      Dataset ds,
      long currentVersion,
      String queryId,
      long epochId,
      LanceSparkWriteOptions writeOptions) {
    int lookback = writeOptions.getStreamingDedupeLookbackVersions();
    if (lookback <= 0 || currentVersion <= 0) {
      return Optional.empty();
    }
    long beginExclusive = Math.max(0L, currentVersion - lookback);
    if (beginExclusive >= currentVersion) {
      return Optional.empty();
    }
    String epochIdStr = Long.toString(epochId);
    try (DatasetDelta delta =
        new DatasetDeltaBuilder(ds)
            .withBeginVersion(beginExclusive)
            .withEndVersion(currentVersion)
            .build()) {
      List<Transaction> transactions = delta.listTransactions();
      try {
        // Walk newest-first. listTransactions() returns ascending-version order, but the list may
        // be sparse, so the index is not a reliable version number (see findReplay's javadoc).
        for (int i = transactions.size() - 1; i >= 0; i--) {
          Transaction txn = transactions.get(i);
          Optional<Map<String, String>> props = txn.transactionProperties();
          if (!props.isPresent()) {
            continue;
          }
          Map<String, String> p = props.get();
          if (queryId.equals(p.get(LanceSparkWriteOptions.STREAMING_QUERY_ID_PROP))
              && epochIdStr.equals(p.get(LanceSparkWriteOptions.STREAMING_EPOCH_ID_PROP))) {
            return Optional.of(beginExclusive + (long) i + 1L);
          }
        }
      } finally {
        // Each history Transaction is AutoCloseable and may hold a native Arrow schema handle;
        // close them all so a long-running query does not leak one handle per micro-batch.
        closeQuietly(transactions);
      }
    } catch (RuntimeException e) {
      // A version inside the lookback window may have been removed by VACUUM/cleanup since it was
      // written; lance-core's listTransactions() checks out every version in the range and throws
      // when one is gone. Failing here would fail the epoch — and because the version stays gone,
      // Spark would retry the same micro-batch forever, permanently wedging the query. Instead we
      // degrade to the documented bounded at-least-once fallback: skip dedupe for this epoch (a
      // replay may then produce a duplicate) and let the commit proceed. The window self-heals as
      // currentVersion advances past the removed versions.
      LOG.warn(
          "streaming dedupe scan failed for versions ({}, {}] (a version may have been removed "
              + "by VACUUM/cleanup); skipping dedupe for this epoch (at-least-once fallback, a "
              + "replay may duplicate). queryId={} epochId={}",
          beginExclusive,
          currentVersion,
          queryId,
          epochId,
          e);
      return Optional.empty();
    }
    return Optional.empty();
  }

  private static void closeQuietly(List<Transaction> transactions) {
    for (Transaction txn : transactions) {
      try {
        txn.close();
      } catch (RuntimeException e) {
        LOG.debug("failed to close history transaction during dedupe scan", e);
      }
    }
  }
}
