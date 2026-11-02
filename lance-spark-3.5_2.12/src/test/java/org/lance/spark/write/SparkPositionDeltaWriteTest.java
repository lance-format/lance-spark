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
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.fragment.RowIdMeta;
import org.lance.spark.LancePositionDeltaOperation;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkPositionDeltaWriteTest {
  @TempDir static Path tempDir;

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

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
    assertTrue(write.distributionStrictlyRequired());
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
    StructType schema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);
    LancePositionDeltaOperation operation =
        new LancePositionDeltaOperation(
            RowLevelOperation.Command.DELETE,
            schema,
            LanceSparkReadOptions.builder().datasetUri(uri).build(),
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
            return "delete-test";
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
