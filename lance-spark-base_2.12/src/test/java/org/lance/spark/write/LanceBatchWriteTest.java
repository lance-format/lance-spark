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
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.operation.Overwrite;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
   * Verifies that CommitBuilder.useStableRowIds(true) sets the manifest flag (readable via
   * Dataset.hasStableRowIds()), without requiring a separate config-map entry.
   */
  @Test
  public void testStableRowIdsViaCommitBuilder(final TestInfo testInfo) {
    final String datasetName = testInfo.getTestMethod().get().getName();
    final String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    final Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    final Schema schema = new Schema(Collections.singletonList(field));

    // Create table via CommitBuilder with useStableRowIds(true)
    final Overwrite createOp =
        Overwrite.builder().fragments(Collections.emptyList()).schema(schema).build();
    final CommitBuilder builder =
        new CommitBuilder(datasetUri, LanceRuntime.allocator()).writeParams(Collections.emptyMap());
    builder.useStableRowIds(true);
    try (Transaction txn = new Transaction.Builder().operation(createOp).build();
        Dataset committed = builder.execute(txn)) {
      // auto-close
    }

    // Verify manifest flag is set
    try (Dataset ds = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertTrue(
          ds.hasStableRowIds(),
          "hasStableRowIds() should be true after " + "CommitBuilder.useStableRowIds(true)");
    }
  }

  /**
   * Verifies that appending to a table with stable row IDs works even when the append does NOT
   * re-specify useStableRowIds. Lance-core auto-inherits the flag from the existing manifest.
   */
  @Test
  public void testAppendInheritsStableRowIds(final TestInfo testInfo) {
    final String datasetName = testInfo.getTestMethod().get().getName();
    final String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    final Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    final Schema schema = new Schema(Collections.singletonList(field));

    // Create table with stable row IDs
    final Overwrite createOp =
        Overwrite.builder().fragments(Collections.emptyList()).schema(schema).build();
    final CommitBuilder createBuilder =
        new CommitBuilder(datasetUri, LanceRuntime.allocator()).writeParams(Collections.emptyMap());
    createBuilder.useStableRowIds(true);
    try (Transaction txn = new Transaction.Builder().operation(createOp).build();
        Dataset committed = createBuilder.execute(txn)) {
      // auto-close
    }

    // Overwrite without re-specifying useStableRowIds.
    // Lance-core auto-inherits the flag from the existing manifest.
    try (Dataset ds = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      final Overwrite overwriteOp =
          Overwrite.builder().fragments(Collections.emptyList()).schema(schema).build();
      final CommitBuilder appendBuilder = new CommitBuilder(ds).writeParams(Collections.emptyMap());
      // Note: NOT calling appendBuilder.useStableRowIds(true)
      try (Transaction txn =
              new Transaction.Builder().readVersion(ds.version()).operation(overwriteOp).build();
          Dataset committed = appendBuilder.execute(txn)) {
        // auto-close
      }
    }

    // Verify flag is still set after append
    try (Dataset ds = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertTrue(
          ds.hasStableRowIds(),
          "hasStableRowIds() should remain true after " + "append without re-specifying the flag");
    }
  }

  /**
   * Verifies that creating a table WITHOUT stable row IDs results in hasStableRowIds() returning
   * false.
   */
  @Test
  public void testTableWithoutStableRowIds(final TestInfo testInfo) {
    final String datasetName = testInfo.getTestMethod().get().getName();
    final String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    final Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    final Schema schema = new Schema(Collections.singletonList(field));

    // Create table without stable row IDs
    final Overwrite createOp =
        Overwrite.builder().fragments(Collections.emptyList()).schema(schema).build();
    final CommitBuilder builder =
        new CommitBuilder(datasetUri, LanceRuntime.allocator()).writeParams(Collections.emptyMap());
    // NOT calling builder.useStableRowIds(true)
    try (Transaction txn = new Transaction.Builder().operation(createOp).build();
        Dataset committed = builder.execute(txn)) {
      // auto-close
    }

    try (Dataset ds = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertFalse(
          ds.hasStableRowIds(),
          "hasStableRowIds() should be false when " + "table is created without the flag");
    }
  }
}
