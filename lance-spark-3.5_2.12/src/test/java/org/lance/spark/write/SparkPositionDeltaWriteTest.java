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

import org.lance.BlobFile;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.fragment.RowIdMeta;
import org.lance.spark.LanceConstant;
import org.lance.spark.LancePositionDeltaOperation;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.function.LanceFragmentIdWithDefaultFunction;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.distributions.ClusteredDistribution;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkPositionDeltaWriteTest {
  @TempDir static Path tempDir;

  /** The delta commit must also accept blob v1 fields written directly into 2.2 files. */
  @Test
  public void commitPreservesBlobV1FieldsIn22Files(TestInfo testInfo) throws Exception {
    String uri =
        TestUtils.getDatasetUri(tempDir.toString(), testInfo.getTestMethod().get().getName());
    Field data =
        new Field(
            "data",
            new FieldType(
                true,
                ArrowType.LargeBinary.INSTANCE,
                null,
                Collections.singletonMap(
                    BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)),
            null);
    Schema schema = new Schema(Arrays.asList(ARROW_SCHEMA.getFields().get(0), data));
    byte[][] expected = {new byte[] {1, 2, 3}, new byte[] {4, 5, 6, 7}};
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.write()
          .allocator(allocator)
          .uri(uri)
          .schema(schema)
          .dataStorageVersion("2.1")
          .execute()
          .close();
      for (int commit = 0; commit < expected.length; commit++) {
        List<FragmentMetadata> fragments;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          ((IntVector) root.getVector("id")).setSafe(0, commit + 1);
          ((LargeVarBinaryVector) root.getVector("data")).setSafe(0, expected[commit]);
          root.setRowCount(1);
          fragments =
              Fragment.write()
                  .datasetUri(uri)
                  .allocator(allocator)
                  .data(root)
                  .mode(WriteParams.WriteMode.APPEND)
                  .dataStorageVersion("2.2")
                  .execute();
        }
        DeltaBatchWrite batch =
            new SparkPositionDeltaWrite(
                    LanceArrowUtils.fromArrowSchema(schema),
                    LanceSparkWriteOptions.from(uri).toBuilder().fileFormatVersion("2.2").build(),
                    null,
                    null,
                    null,
                    false,
                    null,
                    Collections.emptyMap())
                .toBatch();
        batch.commit(
            new WriterCommitMessage[] {
              new SparkPositionDeltaWrite.DeltaWriteTaskCommit(fragments, Collections.emptyMap())
            });
        try (Dataset dataset =
            Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(uri).build())
                .build()) {
          assertEquals(commit + 2, dataset.version());
          assertEquals(commit + 1, dataset.countRows());
          assertEquals(commit + 1, dataset.getFragments().size());
          List<Long> rowAddresses = new ArrayList<>();
          for (Fragment fragment : dataset.getFragments()) {
            rowAddresses.add(((long) fragment.getId()) << 32);
            fragment
                .metadata()
                .getFiles()
                .forEach(
                    file -> {
                      assertEquals(2, file.getFileMajorVersion());
                      assertEquals(2, file.getFileMinorVersion());
                    });
          }
          List<BlobFile> blobs = dataset.takeBlobs(rowAddresses, "data");
          try {
            assertEquals(commit + 1, blobs.size());
            for (int row = 0; row <= commit; row++) {
              assertArrayEquals(expected[row], blobs.get(row).read());
            }
          } finally {
            for (BlobFile blob : blobs) {
              blob.close();
            }
          }
        }
      }
    }
  }

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  @Test
  public void deleteParallelismUsesExecutorCores() {
    SparkConf conf = new SparkConf(false);
    assertEquals(2, SparkPositionDeltaWrite.deleteParallelism(100, conf));
    assertEquals(2, SparkPositionDeltaWrite.deleteParallelism(100, null));
    conf.set("spark.executor.cores", "4");
    assertEquals(4, SparkPositionDeltaWrite.deleteParallelism(100, conf));
    assertEquals(1, SparkPositionDeltaWrite.deleteParallelism(1, conf));
    conf.set("spark.executor.cores", "1");
    assertEquals(1, SparkPositionDeltaWrite.deleteParallelism(100, conf));
  }

  @Test
  public void datasetCloseWaitsForFragmentDeletion(TestInfo testInfo) throws Exception {
    String uri = createDataset(testInfo, false);
    CountDownLatch preparingDeletion = new CountDownLatch(1);
    CountDownLatch allowDeletion = new CountDownLatch(1);
    CountDownLatch closingDataset = new CountDownLatch(1);
    RoaringBitmap bitmap =
        new RoaringBitmap() {
          @Override
          public PeekableIntIterator getIntIterator() {
            preparingDeletion.countDown();
            try {
              if (!allowDeletion.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to resume deletion");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("Interrupted while preparing deletion", e);
            }
            return super.getIntIterator();
          }
        };
    bitmap.add(0);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      int fragmentId = dataset.getFragments().get(0).getId();
      try {
        Future<FragmentMetadata> deleting =
            pool.submit(
                () ->
                    SparkPositionDeltaWrite.deleteFragmentRows(dataset, fragmentId, bitmap)
                        .getValue());
        assertTrue(preparingDeletion.await(10, TimeUnit.SECONDS));
        Future<?> closing =
            pool.submit(
                () -> {
                  closingDataset.countDown();
                  dataset.close();
                });
        assertTrue(closingDataset.await(10, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> closing.get(200, TimeUnit.MILLISECONDS));
        allowDeletion.countDown();
        assertEquals(1, deleting.get(10, TimeUnit.SECONDS).getNumDeletions());
        closing.get(10, TimeUnit.SECONDS);
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SparkPositionDeltaWrite.deleteFragmentRows(
                    dataset, fragmentId, RoaringBitmap.bitmapOf(1)));
      } finally {
        allowDeletion.countDown();
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  public void positionDeltaWriteDoesNotUseCommitCoordinator(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build())
          .close();

      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);
      SparkPositionDeltaWrite write =
          new SparkPositionDeltaWrite(
              sparkSchema,
              LanceSparkWriteOptions.from(datasetUri),
              null,
              null,
              null,
              false,
              null,
              Collections.emptyMap());

      DeltaBatchWrite batchWrite = write.toBatch();
      Method useCommitCoordinator = batchWrite.getClass().getMethod("useCommitCoordinator");
      assertEquals(
          batchWrite.getClass(),
          useCommitCoordinator.getDeclaringClass(),
          "Position delta writes must explicitly override Spark's commit coordinator default");
      assertFalse(batchWrite.useCommitCoordinator());
    }
  }

  @Test
  public void newWriteBuilderPropagatesCacheBackendConfiguration() throws Exception {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri("file:///unused")
            .catalogName("configured-catalog")
            .indexCacheBackend("moka://?capacity=1048576")
            .metadataCacheBackend("moka://?capacity=524288")
            .build();
    StructType schema = new StructType();
    LancePositionDeltaOperation operation =
        new LancePositionDeltaOperation(
            RowLevelOperation.Command.DELETE,
            schema,
            readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            false,
            null,
            Collections.emptyMap());
    LogicalWriteInfo info =
        new LogicalWriteInfo() {
          @Override
          public String queryId() {
            return "probe";
          }

          @Override
          public StructType schema() {
            return schema;
          }

          @Override
          public CaseInsensitiveStringMap options() {
            return new CaseInsensitiveStringMap(Collections.emptyMap());
          }
        };

    DeltaWriteBuilder builder = operation.newWriteBuilder(info);
    LanceSparkWriteOptions writeOptions = extractWriteOptions(builder);
    assertEquals("configured-catalog", writeOptions.getCatalogName());
    assertEquals("moka://?capacity=1048576", writeOptions.getIndexCacheBackend());
    assertEquals("moka://?capacity=524288", writeOptions.getMetadataCacheBackend());
  }

  @Test
  public void deleteClustersOnBareFragmentId(TestInfo testInfo) throws Exception {
    String uri = createDataset(testInfo, false);

    // The commit rejects a fragment claimed by two tasks, so DELETE must cluster on the bare
    // _fragid that the scan reports for every row of its single fragment.
    ClusteredDistribution delete =
        assertInstanceOf(
            ClusteredDistribution.class,
            newPositionDeltaWrite(uri, RowLevelOperation.Command.DELETE).requiredDistribution());
    assertEquals(1, delete.clustering().length);
    NamedReference deleteKey = assertInstanceOf(NamedReference.class, delete.clustering()[0]);
    assertArrayEquals(new String[] {LanceConstant.FRAGMENT_ID}, deleteKey.fieldNames());

    // UPDATE and MERGE also write insert rows, whose _fragid is null, so they keep the function
    // that spreads those rows instead of sending every insert to a single task.
    ClusteredDistribution update =
        assertInstanceOf(
            ClusteredDistribution.class,
            newPositionDeltaWrite(uri, RowLevelOperation.Command.UPDATE).requiredDistribution());
    assertEquals(1, update.clustering().length);
    Transform updateKey = assertInstanceOf(Transform.class, update.clustering()[0]);
    assertEquals(LanceFragmentIdWithDefaultFunction.NAME, updateKey.name());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void deleteTasksReturnUncommittedFragmentMetadata(boolean stableRowIds, TestInfo testInfo)
      throws Exception {
    String uri = createDataset(testInfo, stableRowIds);
    List<FragmentMetadata> original;
    long version;
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      original =
          dataset.getFragments().stream().map(Fragment::metadata).collect(Collectors.toList());
      version = dataset.version();
    }
    RowIdMeta rowIds = original.get(0).getRowIdMeta();
    SparkPositionDeltaWrite write = newDeleteWrite(uri);
    DeltaBatchWrite batch = write.toBatch();
    DeltaWriterFactory factory =
        roundTrip(batch.createBatchWriterFactory(() -> 2), DeltaWriterFactory.class);
    SparkPositionDeltaWrite.DeleteTaskCommit partial;
    SparkPositionDeltaWrite.DeleteTaskCommit full;
    try (DeltaWriter<InternalRow> writer = factory.createWriter(0, 0)) {
      deleteRow(writer, original.get(0).getId(), 1);
      deleteRow(writer, original.get(0).getId(), 1);
      partial = roundTrip(writer.commit(), SparkPositionDeltaWrite.DeleteTaskCommit.class);
    }
    try (DeltaWriter<InternalRow> writer = factory.createWriter(1, 1)) {
      for (int i = 0; i < 5; i++) {
        deleteRow(writer, original.get(1).getId(), i);
      }
      full = roundTrip(writer.commit(), SparkPositionDeltaWrite.DeleteTaskCommit.class);
    }
    assertEquals(1, partial.updatedFragments().size());
    assertNotNull(partial.updatedFragments().get(0).getDeletionFile());
    assertEquals(1, partial.updatedFragments().get(0).getNumDeletions());
    assertEquals(rowIds, partial.updatedFragments().get(0).getRowIdMeta());
    assertTrue(partial.removedFragmentIds().isEmpty());
    assertTrue(full.updatedFragments().isEmpty());
    assertEquals(
        Collections.singletonList((long) original.get(1).getId()), full.removedFragmentIds());
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      assertEquals(version, dataset.version());
      assertEquals(10, dataset.countRows());
    }
    batch.commit(new WriterCommitMessage[] {partial, full});
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      assertEquals(version + 1, dataset.version());
      assertEquals(4, dataset.countRows());
      assertEquals(1, dataset.getFragments().size());
      assertEquals(stableRowIds, dataset.hasStableRowIds());
      assertEquals(rowIds, dataset.getFragments().get(0).metadata().getRowIdMeta());
    }
  }

  @Test
  public void oneDeleteTaskWritesEveryFragmentItOwns(TestInfo testInfo) throws Exception {
    String uri = createDataset(testInfo, false);
    List<FragmentMetadata> original;
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      original =
          dataset.getFragments().stream().map(Fragment::metadata).collect(Collectors.toList());
    }
    DeltaBatchWrite batch = newDeleteWrite(uri).toBatch();
    SparkPositionDeltaWrite.DeleteTaskCommit commit;
    try (DeltaWriter<InternalRow> writer =
        batch.createBatchWriterFactory(() -> 1).createWriter(0, 0)) {
      deleteRow(writer, original.get(0).getId(), 0);
      for (int i = 0; i < 5; i++) {
        deleteRow(writer, original.get(1).getId(), i);
      }
      commit = (SparkPositionDeltaWrite.DeleteTaskCommit) writer.commit();
    }
    assertEquals(1, commit.updatedFragments().size());
    assertEquals(original.get(0).getId(), commit.updatedFragments().get(0).getId());
    assertEquals(1, commit.updatedFragments().get(0).getNumDeletions());
    assertEquals(
        Collections.singletonList((long) original.get(1).getId()), commit.removedFragmentIds());
    batch.commit(new WriterCommitMessage[] {commit});
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      assertEquals(4, dataset.countRows());
      assertEquals(1, dataset.getFragments().size());
    }
  }

  @Test
  public void deleteRetriesDoNotPublishDiscardedAttempts(TestInfo testInfo) throws Exception {
    String uri = createDataset(testInfo, false);
    int fragmentId;
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      fragmentId = dataset.getFragments().get(0).getId();
    }
    DeltaBatchWrite batch = newDeleteWrite(uri).toBatch();
    DeltaWriterFactory factory = batch.createBatchWriterFactory(() -> 1);
    SparkPositionDeltaWrite.DeleteTaskCommit retry;
    try (DeltaWriter<InternalRow> discarded = factory.createWriter(0, 0)) {
      deleteRow(discarded, fragmentId, 0);
      discarded.commit();
      discarded.abort();
    }
    try (DeltaWriter<InternalRow> writer = factory.createWriter(0, 1)) {
      deleteRow(writer, fragmentId, 0);
      retry = (SparkPositionDeltaWrite.DeleteTaskCommit) writer.commit();
    }
    batch.commit(new WriterCommitMessage[] {retry});
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      assertEquals(9, dataset.countRows());
      assertEquals(1, dataset.getFragments().get(0).metadata().getNumDeletions());
    }
  }

  @Test
  public void deleteCommitRejectsMultipleOwnersForOneFragment(TestInfo testInfo) throws Exception {
    String uri = createDataset(testInfo, false);
    int fragmentId;
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      fragmentId = dataset.getFragments().get(0).getId();
    }
    DeltaBatchWrite batch = newDeleteWrite(uri).toBatch();
    DeltaWriterFactory factory = batch.createBatchWriterFactory(() -> 2);
    WriterCommitMessage first;
    WriterCommitMessage second;
    try (DeltaWriter<InternalRow> writer = factory.createWriter(0, 0)) {
      deleteRow(writer, fragmentId, 0);
      first = writer.commit();
    }
    try (DeltaWriter<InternalRow> writer = factory.createWriter(1, 1)) {
      deleteRow(writer, fragmentId, 1);
      second = writer.commit();
    }
    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> batch.commit(new WriterCommitMessage[] {first, second}));
    assertTrue(error.getMessage().contains("Multiple DELETE task commits"));
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      assertEquals(10, dataset.countRows());
    }
  }

  @Test
  public void staleDeleteTaskCannotOverwriteCommittedDeletions(TestInfo testInfo) throws Exception {
    String uri = createDataset(testInfo, false);
    int fragmentId;
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      fragmentId = dataset.getFragments().get(0).getId();
    }
    DeltaBatchWrite first = newDeleteWrite(uri).toBatch();
    DeltaBatchWrite stale = newDeleteWrite(uri).toBatch();
    WriterCommitMessage firstCommit;
    try (DeltaWriter<InternalRow> writer =
        first.createBatchWriterFactory(() -> 1).createWriter(0, 0)) {
      deleteRow(writer, fragmentId, 0);
      firstCommit = writer.commit();
    }
    first.commit(new WriterCommitMessage[] {firstCommit});
    WriterCommitMessage staleCommit;
    try (DeltaWriter<InternalRow> writer =
        stale.createBatchWriterFactory(() -> 1).createWriter(0, 1)) {
      deleteRow(writer, fragmentId, 1);
      staleCommit = writer.commit();
    }
    assertThrows(Exception.class, () -> stale.commit(new WriterCommitMessage[] {staleCommit}));
    try (Dataset dataset = Utils.openDatasetBuilder(LanceSparkWriteOptions.from(uri)).build()) {
      assertEquals(9, dataset.countRows());
      assertEquals(1, dataset.getFragments().get(0).metadata().getNumDeletions());
    }
  }

  private String createDataset(TestInfo testInfo, boolean stableRowIds) throws Exception {
    String name = testInfo.getTestMethod().get().getName() + "_" + stableRowIds;
    String uri = TestUtils.getDatasetUri(tempDir.toString(), name);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(
              allocator,
              uri,
              ARROW_SCHEMA,
              new WriteParams.Builder().withEnableStableRowIds(stableRowIds).build())
          .close();
    }
    for (int fragment = 0; fragment < 2; fragment++) {
      LanceBatchWrite append =
          new LanceBatchWrite(
              LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA),
              LanceSparkWriteOptions.builder()
                  .datasetUri(uri)
                  .enableStableRowIds(stableRowIds)
                  .build(),
              false,
              null,
              null,
              null,
              null,
              false,
              null);
      WriterCommitMessage message;
      try (DataWriter<InternalRow> writer =
          append.createBatchWriterFactory(() -> 1).createWriter(0, fragment)) {
        for (int i = 0; i < 5; i++) {
          int id = fragment * 5 + i;
          writer.write(new GenericInternalRow(new Object[] {id, id * 10}));
        }
        message = writer.commit();
      }
      append.commit(new WriterCommitMessage[] {message});
    }
    return uri;
  }

  private SparkPositionDeltaWrite newDeleteWrite(String uri) {
    return newPositionDeltaWrite(uri, RowLevelOperation.Command.DELETE);
  }

  private SparkPositionDeltaWrite newPositionDeltaWrite(
      String uri, RowLevelOperation.Command command) {
    StructType schema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);
    LancePositionDeltaOperation operation =
        new LancePositionDeltaOperation(
            command,
            schema,
            LanceSparkReadOptions.builder().datasetUri(uri).build(),
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            false,
            null,
            Collections.emptyMap());
    String queryId = command.name() + "-test";
    LogicalWriteInfo info =
        new LogicalWriteInfo() {
          @Override
          public String queryId() {
            return queryId;
          }

          @Override
          public StructType schema() {
            return schema;
          }

          @Override
          public CaseInsensitiveStringMap options() {
            return new CaseInsensitiveStringMap(Collections.emptyMap());
          }
        };
    return (SparkPositionDeltaWrite) operation.newWriteBuilder(info).build();
  }

  private static void deleteRow(DeltaWriter<InternalRow> writer, int fragmentId, int rowIndex)
      throws Exception {
    long rowAddr = ((long) fragmentId << 32) | rowIndex;
    writer.delete(
        new GenericInternalRow(new Object[] {fragmentId}),
        new GenericInternalRow(new Object[] {rowAddr, rowAddr}));
  }

  private static <T> T roundTrip(Object value, Class<T> type) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return type.cast(in.readObject());
    }
  }

  private static LanceSparkWriteOptions extractWriteOptions(DeltaWriteBuilder builder)
      throws Exception {
    // Fully qualified to avoid clashing with the Arrow Field import used above.
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("writeOptions");
    field.setAccessible(true);
    return (LanceSparkWriteOptions) field.get(builder);
  }
}
