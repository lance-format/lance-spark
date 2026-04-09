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
import org.lance.spark.LanceConstant;
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
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that UpdateColumnsBackfillBatchWrite detects concurrent DELETEs via version pinning. The
 * writer pins the dataset version at construction time; a concurrent DELETE advances the version,
 * and the stale commit must fail.
 *
 * <p>This exercises the specific bug that motivated OCC: a concurrent DELETE between executor scan
 * and driver commit was silently lost when readVersion used the head version instead of the pinned
 * scan-time version.
 */
public class UpdateColumnsConflictTest {
  @TempDir static Path tempDir;

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  /**
   * An UpdateColumnsBackfillBatchWrite pins version at construction. A concurrent DELETE advances
   * the dataset version. The backfill commit must fail because the pinned readVersion is stale.
   */
  @Test
  public void testConcurrentDeleteDetectedByVersionPinning(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // Create dataset with (id, value) rows
      Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build())
          .close();

      LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
      StructType dataSchema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);

      // Append initial data: 5 rows
      LanceBatchWrite initialWrite =
          new LanceBatchWrite(dataSchema, writeOptions, false, null, null, null, null, false, null);
      DataWriterFactory initFactory = initialWrite.createBatchWriterFactory(() -> 1);
      WriterCommitMessage initialMsg;
      try (DataWriter<InternalRow> writer = initFactory.createWriter(0, 0)) {
        for (int i = 0; i < 5; i++) {
          writer.write(new GenericInternalRow(new Object[] {i, i * 10}));
        }
        initialMsg = writer.commit();
      }
      initialWrite.commit(new WriterCommitMessage[] {initialMsg});
      // Dataset now at V2

      // Create UpdateColumnsBackfillBatchWrite — pins version at V2
      List<String> updateColumns = Collections.singletonList("value");
      StructType updateSchema =
          new StructType()
              .add(LanceConstant.ROW_ADDRESS, DataTypes.LongType, false)
              .add(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType, false)
              .add("value", DataTypes.IntegerType, true);

      UpdateColumnsBackfillBatchWrite updateWrite =
          new UpdateColumnsBackfillBatchWrite(
              updateSchema, writeOptions, updateColumns, null, null, null, null);

      // Simulate executor work: write updated column values
      DataWriterFactory factory = updateWrite.createBatchWriterFactory(() -> 1);
      WriterCommitMessage updateMsg;
      try (DataWriter<InternalRow> writer = factory.createWriter(0, 0)) {
        for (int i = 0; i < 5; i++) {
          long rowAddr = i;
          writer.write(new GenericInternalRow(new Object[] {rowAddr, 0, i * 999}));
        }
        updateMsg = writer.commit();
      }

      // Concurrent DELETE advances dataset to V3
      try (Dataset ds = Dataset.open(datasetUri, allocator)) {
        ds.delete("id = 2");
      }

      // Backfill commit with pinned readVersion=V2 must fail — V3 exists
      assertThrows(
          Exception.class, () -> updateWrite.commit(new WriterCommitMessage[] {updateMsg}));
    }
  }
}
