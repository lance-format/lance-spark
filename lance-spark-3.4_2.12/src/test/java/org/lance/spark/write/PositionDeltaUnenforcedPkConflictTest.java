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
import org.lance.operation.UpdateConfig;
import org.lance.operation.UpdateMap;
import org.lance.schema.LanceField;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Spark 3.4 has no MERGE INTO, but its position-delta write must still record inserted primary
 * keys, so two concurrent writes that insert the same new keys conflict instead of silently
 * duplicating. Mirrors the SQL-level MergeUnenforcedPkConcurrencyTest in the 3.5 module.
 */
public class PositionDeltaUnenforcedPkConflictTest {
  @TempDir static Path tempDir;

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.notNullable(new ArrowType.Utf8()), null),
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  /** Creates an empty dataset whose "id" column is declared as the unenforced primary key. */
  private String createPkDataset(BufferAllocator allocator, String datasetName) {
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build()).close();

    try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
      LanceField idField =
          dataset.getLanceSchema().fields().stream()
              .filter(f -> f.getName().equals("id"))
              .findFirst()
              .orElseThrow(IllegalStateException::new);
      Map<String, String> metadata = new HashMap<>();
      metadata.put("lance-schema:unenforced-primary-key", "true");
      metadata.put("lance-schema:unenforced-primary-key:position", "1");
      Map<Integer, UpdateMap> fieldMetadataUpdates = new HashMap<>();
      fieldMetadataUpdates.put(
          idField.getId(), UpdateMap.builder().updates(metadata).replace(false).build());
      UpdateConfig updateConfig =
          UpdateConfig.builder().fieldMetadataUpdates(fieldMetadataUpdates).build();
      Transaction txn =
          new Transaction.Builder().readVersion(dataset.version()).operation(updateConfig).build();
      try {
        new CommitBuilder(dataset).execute(txn).close();
      } finally {
        txn.close();
      }
    }
    return datasetUri;
  }

  /** Inserts one row per id through the position-delta writer and returns the task commit. */
  private WriterCommitMessage insertRows(SparkPositionDeltaWrite write, String... ids)
      throws Exception {
    DeltaWriter<InternalRow> writer =
        write.toBatch().createBatchWriterFactory(() -> 1).createWriter(0, 0);
    try {
      int value = 0;
      for (String id : ids) {
        writer.insert(new GenericInternalRow(new Object[] {UTF8String.fromString(id), value++}));
      }
      return writer.commit();
    } finally {
      writer.close();
    }
  }

  @Test
  public void concurrentInsertsOfSameNewKeysConflict(TestInfo testInfo) throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      String datasetUri = createPkDataset(allocator, testInfo.getTestMethod().get().getName());
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);

      // Both writes pin the same version before either commits.
      SparkPositionDeltaWrite writeA =
          new SparkPositionDeltaWrite(
              sparkSchema, LanceSparkWriteOptions.from(datasetUri), null, null, null, false, null);
      SparkPositionDeltaWrite writeB =
          new SparkPositionDeltaWrite(
              sparkSchema, LanceSparkWriteOptions.from(datasetUri), null, null, null, false, null);

      WriterCommitMessage msgA = insertRows(writeA, "k1", "k2");
      WriterCommitMessage msgB = insertRows(writeB, "k1", "k2");

      writeA.toBatch().commit(new WriterCommitMessage[] {msgA});
      assertThrows(
          Exception.class, () -> writeB.toBatch().commit(new WriterCommitMessage[] {msgB}));

      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        assertEquals(2, dataset.countRows());
      }
    }
  }

  @Test
  public void concurrentInsertsOfDisjointKeysBothSucceed(TestInfo testInfo) throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      String datasetUri = createPkDataset(allocator, testInfo.getTestMethod().get().getName());
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);

      SparkPositionDeltaWrite writeA =
          new SparkPositionDeltaWrite(
              sparkSchema, LanceSparkWriteOptions.from(datasetUri), null, null, null, false, null);
      SparkPositionDeltaWrite writeB =
          new SparkPositionDeltaWrite(
              sparkSchema, LanceSparkWriteOptions.from(datasetUri), null, null, null, false, null);

      WriterCommitMessage msgA = insertRows(writeA, "a1", "a2");
      WriterCommitMessage msgB = insertRows(writeB, "b1", "b2");

      writeA.toBatch().commit(new WriterCommitMessage[] {msgA});
      writeB.toBatch().commit(new WriterCommitMessage[] {msgB});

      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        assertEquals(4, dataset.countRows());
      }
    }
  }
}
