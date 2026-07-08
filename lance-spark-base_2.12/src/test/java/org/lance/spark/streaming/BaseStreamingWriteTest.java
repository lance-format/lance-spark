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
package org.lance.spark.streaming;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.write.LanceStreamingWrite;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for {@link LanceStreamingWrite}. Concrete extensions live in each Spark
 * version-specific test module so the same suite runs on Spark 3.4, 3.5, 4.0, and 4.1.
 *
 * <p>The streaming sink is exercised both through Spark's streaming engine ({@code writeStream ...
 * toTable}) and through direct {@code commit} calls — the latter lets us deterministically verify
 * dedupe and empty-epoch behavior without depending on Spark's checkpoint advancement.
 */
public abstract class BaseStreamingWriteTest {

  protected SparkSession spark;
  protected TableCatalog catalog;
  protected final String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-streaming-write-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    // Create the "default" namespace in manifest mode so deregisterTable works.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  // ---------- end-to-end tests ----------

  @Test
  public void testAppendHappyPath() throws Exception {
    String tableName = uniqueTable("stream_append");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    Path src = tempDir.resolve("src-" + tableName);
    Path checkpoint = tempDir.resolve("ckpt-" + tableName);
    String queryId = "qid-" + tableName;

    // Batch 1: 3 rows.
    writeBatchParquet(src, new int[] {1, 2, 3});
    runStreamingBatch(src, checkpoint, fullName, queryId);
    assertEquals(3L, countRows(fullName));

    // Batch 2: 2 more rows on the same checkpoint.
    writeBatchParquet(src, new int[] {4, 5});
    runStreamingBatch(src, checkpoint, fullName, queryId);
    assertEquals(5L, countRows(fullName));
  }

  @Test
  public void testMissingStreamingQueryIdFails() throws Exception {
    String tableName = uniqueTable("stream_no_qid");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    Path src = tempDir.resolve("src-" + tableName);
    Path checkpoint = tempDir.resolve("ckpt-" + tableName);
    writeBatchParquet(src, new int[] {1});

    StreamingQueryException ex =
        assertThrows(
            StreamingQueryException.class,
            () -> {
              StreamingQuery q =
                  spark
                      .readStream()
                      .schema(rowSchema())
                      .option("recursiveFileLookup", "true")
                      .parquet(src.toString())
                      .writeStream()
                      .format("lance")
                      .option("checkpointLocation", checkpoint.toString())
                      .trigger(Trigger.AvailableNow())
                      .toTable(fullName);
              q.processAllAvailable();
              q.stop();
            });
    assertTrue(
        ex.getMessage().contains(LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID),
        "expected error to mention streamingQueryId, got: " + ex.getMessage());
  }

  @Test
  public void testDedupeOnReplayedEpoch() throws Exception {
    String tableName = uniqueTable("stream_dedupe");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    String queryId = "qid-" + tableName;
    Path src = tempDir.resolve("src-" + tableName);
    Path checkpoint = tempDir.resolve("ckpt-" + tableName);
    writeBatchParquet(src, new int[] {10, 20});
    runStreamingBatch(src, checkpoint, fullName, queryId);
    assertEquals(2L, countRows(fullName));
    long versionAfterFirst = currentVersion(tableName);

    // Re-issue the SAME (queryId, epochId=0) directly — the very first epoch in a fresh
    // checkpoint is 0, so this matches what the previous Spark query committed. NOTE: this replays
    // an EMPTY epoch, so it only proves the no-op path is reached; it cannot distinguish a dedupe
    // hit from the empty-epoch skip. testDedupeReplayWithNonEmptyEpoch is the real dedupe guard.
    LanceStreamingWrite sink = directSink(tableName, queryId, 100);
    sink.commit(0L, new WriterCommitMessage[0]);
    assertEquals(2L, countRows(fullName));
    assertEquals(versionAfterFirst, currentVersion(tableName));
  }

  @Test
  public void testMultipleEpochsOnSameSinkAdvanceVersionMonotonically() throws Exception {
    // End-to-end regression guard for the stale-pinned-version bug: each epoch must open the
    // dataset at the current latest version and advance the manifest. maxFilesPerTrigger=1 forces
    // three separate micro-batches. NOTE: Spark may rebuild the StreamingWrite per micro-batch, so
    // this does not guarantee one sink instance spans all epochs — the
    // single-instance-across-epochs
    // path is pinned directly by testDedupeReplayWithNonEmptyEpoch and
    // testOverwriteEpochReplacesAndEmptyEpochTruncates, which reuse one directSink across commits.
    String tableName = uniqueTable("stream_multi_epoch");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    Path src = tempDir.resolve("src-" + tableName);
    Path checkpoint = tempDir.resolve("ckpt-" + tableName);
    String queryId = "qid-" + tableName;

    // Three separate parquet files → three epochs at maxFilesPerTrigger=1.
    writeBatchParquet(src, new int[] {1});
    writeBatchParquet(src, new int[] {2});
    writeBatchParquet(src, new int[] {3});

    long versionBefore = currentVersion(tableName);
    StreamingQuery q =
        spark
            .readStream()
            .schema(rowSchema())
            .option("recursiveFileLookup", "true")
            .option("maxFilesPerTrigger", "1")
            .parquet(src.toString())
            .writeStream()
            .format("lance")
            .option(LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID, queryId)
            .option("checkpointLocation", checkpoint.toString())
            .trigger(Trigger.AvailableNow())
            .toTable(fullName);
    q.processAllAvailable();
    q.stop();

    assertEquals(3L, countRows(fullName), "all three rows should be visible after the query");
    long versionAfter = currentVersion(tableName);
    assertTrue(
        versionAfter >= versionBefore + 3,
        "each epoch must advance the dataset version; before="
            + versionBefore
            + " after="
            + versionAfter);
  }

  @Test
  public void testEmptyEpochIsNoOp() throws Exception {
    String tableName = uniqueTable("stream_empty");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");
    long versionBefore = currentVersion(tableName);

    String queryId = "qid-" + tableName;
    LanceStreamingWrite sink = directSink(tableName, queryId, 100);
    sink.commit(42L, new WriterCommitMessage[0]);

    // Empty epochs are skipped entirely — no Lance transaction is issued, so the dataset
    // version does not advance. Replays of empty epochs are also no-ops.
    assertEquals(0L, countRows(fullName));
    assertEquals(versionBefore, currentVersion(tableName));

    sink.commit(42L, new WriterCommitMessage[0]);
    assertEquals(versionBefore, currentVersion(tableName));
  }

  @Test
  public void testDedupeReplayWithNonEmptyEpoch() throws Exception {
    // Unlike testDedupeOnReplayedEpoch (which replays an EMPTY epoch and would pass even if dedupe
    // were broken, since empty epochs are no-ops regardless), this replays a NON-EMPTY epoch with
    // real fragments. If the (queryId, epochId) dedupe scan failed to fire, the rows would be
    // inserted twice and the version would advance — so this actually pins the idempotency
    // contract.
    String tableName = uniqueTable("stream_dedupe_real");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    String queryId = "qid-" + tableName;
    LanceStreamingWrite sink = directSink(tableName, queryId, 100);

    sink.commit(0L, writeEpoch(sink, 0L, new int[] {10, 20}));
    assertEquals(2L, countRows(fullName));
    long versionAfterFirst = currentVersion(tableName);

    // Replay epoch 0 with freshly written fragments (a real restart re-runs the writers and
    // produces new fragment files); the dedupe scan must still skip on the (queryId, 0) match.
    sink.commit(0L, writeEpoch(sink, 0L, new int[] {10, 20}));
    assertEquals(2L, countRows(fullName), "replayed non-empty epoch must not double-insert");
    assertEquals(
        versionAfterFirst,
        currentVersion(tableName),
        "replayed epoch must not issue a new transaction");
  }

  @Test
  public void testDedupeExpiresOnceEpochFallsOutOfLookbackWindow() throws Exception {
    // The dedupe scan is BOUNDED to the last `lookback` versions (the documented at-least-once
    // fallback). With lookback=1, a single unrelated commit after epoch 0 pushes epoch 0's commit
    // out of the window, so a later replay of epoch 0 is NO LONGER deduped and re-inserts. This
    // pins the bounded-window contract — a regression making the window unbounded would otherwise
    // ship green (every other dedupe test replays the immediately preceding epoch, always
    // in-window).
    String tableName = uniqueTable("stream_dedupe_expire");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    String queryId = "qid-" + tableName;
    LanceStreamingWrite sink = directSink(tableName, queryId, 1);

    sink.commit(0L, writeEpoch(sink, 0L, new int[] {10, 20}));
    assertEquals(2L, countRows(fullName));

    // One unrelated (different epochId) commit advances the version past epoch 0's commit, so
    // epoch 0 now sits outside the 1-version lookback window.
    sink.commit(1L, writeEpoch(sink, 1L, new int[] {30}));
    assertEquals(3L, countRows(fullName));

    // Replay epoch 0: its prior commit is outside the window, so the scan cannot find it and the
    // rows are re-inserted — the documented bounded at-least-once fallback.
    sink.commit(0L, writeEpoch(sink, 0L, new int[] {10, 20}));
    assertEquals(
        5L,
        countRows(fullName),
        "epoch 0 fell out of the lookback window, so its replay must re-insert (at-least-once)");
  }

  @Test
  public void testOverwriteEpochReplacesAndEmptyEpochTruncates() throws Exception {
    // Complete output mode drives overwrite=true: each epoch fully replaces the table, and an
    // empty epoch must truncate to empty (NOT be skipped, which would leave the prior epoch's rows
    // on disk).
    String tableName = uniqueTable("stream_overwrite");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    String queryId = "qid-" + tableName;
    LanceStreamingWrite sink = directSink(tableName, queryId, 100, true);

    sink.commit(0L, writeEpoch(sink, 0L, new int[] {1, 2, 3}));
    assertEquals(3L, countRows(fullName));

    // Epoch 1 overwrites: the table holds ONLY the new rows, not the union.
    sink.commit(1L, writeEpoch(sink, 1L, new int[] {4, 5}));
    assertEquals(2L, countRows(fullName), "overwrite epoch must replace, not append");

    // Epoch 2 is empty: Complete mode must truncate the table to empty.
    sink.commit(2L, new WriterCommitMessage[0]);
    assertEquals(0L, countRows(fullName), "empty overwrite epoch must truncate to empty");

    // The empty overwrite still stamped (queryId, 2), so replaying it is a no-op.
    long versionAfterEmpty = currentVersion(tableName);
    sink.commit(2L, new WriterCommitMessage[0]);
    assertEquals(0L, countRows(fullName));
    assertEquals(
        versionAfterEmpty, currentVersion(tableName), "replayed empty overwrite is a no-op");
  }

  @Test
  public void testWriteModeOverwriteOptionDoesNotTruncateAppendStream() throws Exception {
    // A stray write_mode=overwrite (e.g. a catalog default meant for the batch path) must NOT turn
    // an Append-mode streaming query into a per-epoch truncate. The output mode — Spark's
    // truncate()-driven `overwrite` flag — is the sole overwrite signal on the streaming path.
    String tableName = uniqueTable("stream_wm_overwrite");
    String fullName = qualified(tableName);
    spark.sql("CREATE TABLE " + fullName + " (id INT NOT NULL, name STRING)");

    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, tableName));
    LanceSparkReadOptions read = table.readOptions();
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(read.getDatasetUri())
            .storageOptions(read.getStorageOptions())
            .writeMode(WriteParams.WriteMode.OVERWRITE)
            .streamingQueryId("qid-" + tableName)
            .streamingDedupeLookbackVersions(100)
            .build();
    // overwrite=false → Append output mode, despite write_mode=overwrite in the options.
    LanceStreamingWrite sink =
        new LanceStreamingWrite(
            table.schema(),
            writeOptions,
            false,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false,
            null,
            Collections.emptyMap());

    sink.commit(0L, writeEpoch(sink, 0L, new int[] {1, 2, 3}));
    assertEquals(3L, countRows(fullName));
    sink.commit(1L, writeEpoch(sink, 1L, new int[] {4, 5}));
    assertEquals(
        5L, countRows(fullName), "append stream must accumulate despite write_mode=overwrite");
  }

  @Test
  public void testWhitespaceQueryIdRejected() throws Exception {
    String tableName = uniqueTable("stream_ws_qid");
    spark.sql("CREATE TABLE " + qualified(tableName) + " (id INT NOT NULL, name STRING)");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> directSink(tableName, "   ", 100));
    assertTrue(
        ex.getMessage().contains(LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID),
        "expected error to mention streamingQueryId, got: " + ex.getMessage());
  }

  // ---------- helpers ----------

  /**
   * Drives the streaming writer factory exactly as Spark's task layer does — write rows, {@code
   * commit()} to obtain the {@link WriterCommitMessage}, then {@code close()} — producing real
   * Lance fragments for the given epoch.
   */
  private WriterCommitMessage[] writeEpoch(LanceStreamingWrite sink, long epochId, int[] ids)
      throws IOException {
    StreamingDataWriterFactory factory = sink.createStreamingWriterFactory(null);
    DataWriter<InternalRow> writer = factory.createWriter(0, 0L, epochId);
    try {
      for (int id : ids) {
        writer.write(new GenericInternalRow(new Object[] {id, UTF8String.fromString("row-" + id)}));
      }
      return new WriterCommitMessage[] {writer.commit()};
    } finally {
      writer.close();
    }
  }

  private String uniqueTable(String base) {
    return base + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  private String qualified(String tableName) {
    return catalogName + ".default." + tableName;
  }

  private StructType rowSchema() {
    return new StructType(
        new StructField[] {
          new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("name", DataTypes.StringType, true, Metadata.empty())
        });
  }

  private void writeBatchParquet(Path dir, int[] ids) throws IOException {
    Files.createDirectories(dir);
    List<Row> rows = new ArrayList<>(ids.length);
    for (int id : ids) {
      rows.add(RowFactory.create(id, "row-" + id));
    }
    Path target = dir.resolve("batch-" + UUID.randomUUID() + ".parquet");
    spark.createDataFrame(rows, rowSchema()).coalesce(1).write().parquet(target.toString());
  }

  private void runStreamingBatch(Path srcDir, Path checkpoint, String targetTable, String queryId)
      throws Exception {
    StreamingQuery q =
        spark
            .readStream()
            .schema(rowSchema())
            // Spark's parquet writer creates per-batch sub-directories — let the streaming
            // file source descend into them.
            .option("recursiveFileLookup", "true")
            .parquet(srcDir.toString())
            .writeStream()
            .format("lance")
            .option(LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID, queryId)
            .option("checkpointLocation", checkpoint.toString())
            .trigger(Trigger.AvailableNow())
            .toTable(targetTable);
    q.processAllAvailable();
    q.stop();
  }

  private long countRows(String fullName) {
    return spark.sql("SELECT * FROM " + fullName).count();
  }

  private long currentVersion(String tableName) throws Exception {
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, tableName));
    LanceSparkReadOptions read = table.readOptions();
    try (Dataset ds = openDataset(read)) {
      return ds.version();
    }
  }

  private Dataset openDataset(LanceSparkReadOptions read) {
    if (read.hasNamespace()) {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .namespaceClient(read.getNamespace())
          .tableId(read.getTableId())
          .readOptions(read.toReadOptions())
          .build();
    }
    return Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(read.getDatasetUri())
        .readOptions(read.toReadOptions())
        .build();
  }

  private LanceStreamingWrite directSink(String tableName, String queryId, int lookback)
      throws Exception {
    return directSink(tableName, queryId, lookback, false);
  }

  private LanceStreamingWrite directSink(
      String tableName, String queryId, int lookback, boolean overwrite) throws Exception {
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, tableName));
    LanceSparkReadOptions read = table.readOptions();
    StructType schema = table.schema();

    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(read.getDatasetUri())
            .storageOptions(read.getStorageOptions())
            .streamingQueryId(queryId)
            .streamingDedupeLookbackVersions(lookback)
            .build();

    return new LanceStreamingWrite(
        schema,
        writeOptions,
        overwrite,
        Collections.emptyMap(),
        null,
        Collections.emptyMap(),
        null,
        false,
        null,
        Collections.emptyMap());
  }
}
