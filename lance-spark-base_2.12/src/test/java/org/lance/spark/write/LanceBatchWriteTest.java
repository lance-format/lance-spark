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
import org.lance.WriteParams;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;

import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceBatchWriteTest {
  @TempDir static Path tempDir;

  @Test
  public void testLanceDataWriter(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // Create lance dataset
      Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(field));
      Dataset.create(allocator, datasetUri, schema, new WriteParams.Builder().build()).close();

      // Append data to lance dataset
      LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
      LanceBatchWrite lanceBatchWrite =
          new LanceBatchWrite(
              sparkSchema,
              writeOptions,
              false,
              null, // initialStorageOptions
              null, // namespaceImpl
              null, // namespaceProperties
              null, // tableId
              false, // managedVersioning
              null); // stagedCommit
      DataWriterFactory factor = lanceBatchWrite.createBatchWriterFactory(() -> 1);

      int rows = 132;
      WriterCommitMessage message;
      try (DataWriter<InternalRow> writer = factor.createWriter(0, 0)) {
        for (int i = 0; i < rows; i++) {
          InternalRow row = new GenericInternalRow(new Object[] {i});
          writer.write(row);
        }
        message = writer.commit();
      }
      lanceBatchWrite.commit(new WriterCommitMessage[] {message});

      // Validate lance dataset data
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        try (Scanner scanner = dataset.newScan()) {
          try (ArrowReader reader = scanner.scanBatches()) {
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            int totalRowsRead = 0;
            while (reader.loadNextBatch()) {
              int batchRows = readerRoot.getRowCount();
              for (int i = 0; i < batchRows; i++) {
                int value = (int) readerRoot.getVector("column1").getObject(i);
                assertEquals(totalRowsRead + i, value);
              }
              totalRowsRead += batchRows;
            }
            assertEquals(rows, totalRowsRead);
          }
        }
      }
    }
  }

  /**
   * Two overwrite writers both pin the same table version at construction. After the first driver
   * commit advances the version, the second commit must fail (OCC), not apply a stale overwrite.
   */
  @Test
  public void testConcurrentWriteConflict(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(field));
      Dataset.create(allocator, datasetUri, schema, new WriteParams.Builder().build()).close();

      LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

      LanceBatchWrite writerA =
          new LanceBatchWrite(sparkSchema, writeOptions, true, null, null, null, null, false, null);
      LanceBatchWrite writerB =
          new LanceBatchWrite(sparkSchema, writeOptions, true, null, null, null, null, false, null);

      WriterCommitMessage messageA = writeRows(writerA, sparkSchema, 10, 0);
      WriterCommitMessage messageB = writeRows(writerB, sparkSchema, 10, 100);

      writerA.commit(new WriterCommitMessage[] {messageA});
      assertThrows(Exception.class, () -> writerB.commit(new WriterCommitMessage[] {messageB}));
    }
  }

  /**
   * A staged create (e.g. {@code stageCreateAtPath}) builds {@code StagedCommit} with only the
   * catalog's static storage options. Confirms {@link LanceBatchWrite#commit} merges {@code
   * initialStorageOptions} in before the eventual {@link StagedCommit#commit()}.
   */
  @Test
  public void testCommitMergesInitialStorageOptionsIntoStagedCommit(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);

    // StagedCommit starts with no storage options, as for a path-based staged create.
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema, datasetUri, StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));

    // initialStorageOptions represents namespace-vended credentials, passed to LanceBatchWrite
    // independently of how stagedCommit was constructed.
    Map<String, String> initialStorageOptions = new HashMap<>();
    initialStorageOptions.put("access_key_id", "AKIA-from-namespace");

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema,
            writeOptions,
            false,
            initialStorageOptions,
            null, // namespaceImpl
            null, // namespaceProperties
            null, // tableId
            false, // managedVersioning
            stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("AKIA-from-namespace", stagedCommit.getStorageOptions().get("access_key_id"));

    // The merged options must not break the subsequent commit.
    stagedCommit.commit();
    try (Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertEquals(0, dataset.countRows());
    }
  }

  /**
   * On key conflict, {@code initialStorageOptions} wins over write-time options; a write-time-only
   * key (no namespace-vended counterpart) must still be included in the merge.
   */
  @Test
  public void testCommitStagedMergePrefersInitialStorageOptionsOnConflict(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    Map<String, String> writeTimeOptions = new HashMap<>();
    writeTimeOptions.put("access_key_id", "stale-write-time-key");
    writeTimeOptions.put("region", "us-west-2");
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(datasetUri).toBuilder()
            .storageOptions(writeTimeOptions)
            .build();

    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema, datasetUri, StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema,
            writeOptions,
            false,
            Collections.singletonMap("access_key_id", "fresh-namespace-key"),
            null,
            null,
            null,
            false,
            stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    // initialStorageOptions wins on the conflicting key...
    assertEquals("fresh-namespace-key", stagedCommit.getStorageOptions().get("access_key_id"));
    // ...but a write-time-only key (absent from initialStorageOptions) must still come through.
    assertEquals("us-west-2", stagedCommit.getStorageOptions().get("region"));
  }

  /**
   * When a write-time option sets {@code file_format_version} (e.g. via {@code
   * DataFrameWriterV2.option("file_format_version", "2.2")}), the staged commit must use that
   * version even if none was set at stage time.
   */
  @Test
  public void testCommitStagedPropagatesWriteTimeFileFormatVersion(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    // Stage-time: no file format version set (simulates stageCreate without table property)
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema, datasetUri, StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
    assertNull(stagedCommit.getFileFormatVersion());

    // Write-time: user set file_format_version via DataFrameWriterV2.option(...)
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(datasetUri).toBuilder().fileFormatVersion("2.1").build();

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema, writeOptions, false, null, null, null, null, false, stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("2.1", stagedCommit.getFileFormatVersion());
  }

  /**
   * Write-time {@code file_format_version} takes precedence over the stage-time value. This covers
   * the case where the catalog resolved a default at stage time but the user explicitly overrides
   * it at write time.
   */
  @Test
  public void testCommitStagedWriteTimeFileFormatVersionTakesPrecedence(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    // Stage-time: catalog resolved file format version "2.0"
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, "2.0"));
    assertEquals("2.0", stagedCommit.getFileFormatVersion());

    // Write-time: user explicitly overrides to "2.2"
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(datasetUri).toBuilder().fileFormatVersion("2.2").build();

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema, writeOptions, false, null, null, null, null, false, stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("2.2", stagedCommit.getFileFormatVersion());
  }

  /**
   * When write options have no {@code file_format_version}, the stage-time value must be preserved.
   * Null write-time must not overwrite a stage-time resolution.
   */
  @Test
  public void testCommitStagedPreservesStageTimeVersionWhenWriteTimeIsNull(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    // Stage-time: version set from table properties
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, "2.1"));

    // Write-time: no file_format_version option
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema, writeOptions, false, null, null, null, null, false, stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("2.1", stagedCommit.getFileFormatVersion());
  }

  private static WriterCommitMessage writeRows(
      LanceBatchWrite batchWrite, StructType sparkSchema, int numRows, int startValue)
      throws Exception {
    DataWriterFactory factory = batchWrite.createBatchWriterFactory(() -> 1);
    try (DataWriter<InternalRow> writer = factory.createWriter(0, 0)) {
      for (int i = 0; i < numRows; i++) {
        InternalRow row = new GenericInternalRow(new Object[] {startValue + i});
        writer.write(row);
      }
      return writer.commit();
    }
  }
}
