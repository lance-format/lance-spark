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
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
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
 * Tests that SparkPositionDeltaWrite correctly detects OCC conflicts via version pinning. Two
 * writers pin the same dataset version; after the first commits (advancing the version), the second
 * must fail due to a stale readVersion.
 */
public class SparkPositionDeltaConflictTest {
  @TempDir static Path tempDir;

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  /** Writes rows to the dataset URI and returns their fragment metadata (uncommitted). */
  private List<FragmentMetadata> writeFragment(
      String datasetUri, BufferAllocator allocator, int[] ids, int[] values) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      IntVector valueVec = (IntVector) root.getVector("value");
      for (int i = 0; i < ids.length; i++) {
        idVec.setSafe(i, ids[i]);
        valueVec.setSafe(i, values[i]);
      }
      root.setRowCount(ids.length);
      return Fragment.write().datasetUri(datasetUri).allocator(allocator).data(root).execute();
    }
  }

  /**
   * Two position-delta writers both pin the same version. Writer A commits an Update that replaces
   * the original fragment. Writer B then attempts the same — must fail with an OCC conflict.
   */
  @Test
  public void testConcurrentPositionDeltaConflict(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // Create dataset with initial rows
      Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build())
          .close();

      // Append some data so we have a fragment to work with
      LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);

      LanceBatchWrite initialWrite =
          new LanceBatchWrite(
              sparkSchema, writeOptions, false, null, null, null, null, false, null);
      org.apache.spark.sql.connector.write.DataWriterFactory factory =
          initialWrite.createBatchWriterFactory(() -> 1);
      WriterCommitMessage initMsg;
      try (org.apache.spark.sql.connector.write.DataWriter<
              org.apache.spark.sql.catalyst.InternalRow>
          writer = factory.createWriter(0, 0)) {
        for (int i = 0; i < 5; i++) {
          writer.write(
              new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                  new Object[] {i, i * 10}));
        }
        initMsg = writer.commit();
      }
      initialWrite.commit(new WriterCommitMessage[] {initMsg});
      // Dataset now at V2 with one fragment

      long originalFragmentId;
      try (Dataset ds = Dataset.open(datasetUri, allocator)) {
        originalFragmentId = ds.getFragments().get(0).getId();
      }

      // Both writers pin at V2
      LanceSparkWriteOptions opts = LanceSparkWriteOptions.from(datasetUri);
      SparkPositionDeltaWrite writerA =
          new SparkPositionDeltaWrite(sparkSchema, opts, null, null, null, null);
      SparkPositionDeltaWrite writerB =
          new SparkPositionDeltaWrite(sparkSchema, opts, null, null, null, null);

      // Writer A: replace original fragment with new data
      List<FragmentMetadata> newFragsA =
          writeFragment(datasetUri, allocator, new int[] {1, 2}, new int[] {100, 200});
      WriterCommitMessage msgA =
          new SparkPositionDeltaWrite.DeltaWriteTaskCommit(
              Collections.singletonList(originalFragmentId), Collections.emptyList(), newFragsA);

      writerA.toBatch().commit(new WriterCommitMessage[] {msgA});

      // Writer B: same replacement attempt with stale version — must fail
      List<FragmentMetadata> newFragsB =
          writeFragment(datasetUri, allocator, new int[] {1, 2}, new int[] {999, 888});
      WriterCommitMessage msgB =
          new SparkPositionDeltaWrite.DeltaWriteTaskCommit(
              Collections.singletonList(originalFragmentId), Collections.emptyList(), newFragsB);

      assertThrows(
          Exception.class, () -> writerB.toBatch().commit(new WriterCommitMessage[] {msgB}));
    }
  }
}
