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
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.fragment.FragmentUpdateResult;
import org.lance.operation.Overwrite;
import org.lance.operation.Update;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * JNI-level demonstrations of optimistic concurrency for column updates when a concurrent DELETE
 * lands between executor work and driver commit.
 *
 * <p>These tests do not invoke Spark or UpdateColumnsBackfillBatchWrite. But is constructs the same
 * behavior with Transaction.Builder.readVersion and direct update.
 *
 * <p>testConcurrentDeleteLostByWrongReadVersion exercises the intentional incorrect pattern:
 * readVersion = head after the DELETE, so the concurrent DELETE is not reflected (no deletion
 * file).
 */
public class UpdateColumnsConflictTest {
  @TempDir Path tempDir;

  private static final Schema DATASET_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  /**
   * Creates a Lance dataset at the given URI with 3 rows: (id=1,value=10), (id=2,value=20),
   * (id=3,value=30). Returns the FragmentMetadata of the single fragment created.
   */
  private FragmentMetadata createInitialDataset(String datasetUri, BufferAllocator allocator)
      throws IOException {
    List<FragmentMetadata> fragments;
    try (VectorSchemaRoot root = VectorSchemaRoot.create(DATASET_SCHEMA, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      IntVector valueVec = (IntVector) root.getVector("value");
      idVec.setSafe(0, 1);
      valueVec.setSafe(0, 10);
      idVec.setSafe(1, 2);
      valueVec.setSafe(1, 20);
      idVec.setSafe(2, 3);
      valueVec.setSafe(2, 30);
      root.setRowCount(3);

      fragments = Fragment.write().datasetUri(datasetUri).allocator(allocator).data(root).execute();
    }

    Overwrite createOp = Overwrite.builder().fragments(fragments).schema(DATASET_SCHEMA).build();
    try (Transaction txn = new Transaction.Builder().operation(createOp).build();
        Dataset committed = new CommitBuilder(datasetUri, allocator).execute(txn)) {
      // Dataset created at V1 with one fragment.
    }

    return fragments.get(0);
  }

  /**
   * Simulates what UpdateColumnsWriter.updateFragment() does on an executor: opens the dataset,
   * builds an Arrow stream with updated value for row at the given rowIndex in the given fragment,
   * and calls fragment.updateColumns(). Returns the updated FragmentMetadata.
   *
   * <p>At scan time (V1, no deletions) the returned FragmentMetadata has no deletion file.
   */
  private FragmentMetadata simulateExecutorUpdateColumns(
      String datasetUri, BufferAllocator allocator, int fragmentId, int rowIndex, int newValue)
      throws IOException {
    // _rowaddr encoding: upper 32 bits = fragment id, lower 32 bits = row index within fragment.
    long rowAddr = ((long) fragmentId << 32) | (long) rowIndex;

    Schema updateSchema =
        new Schema(
            Arrays.asList(
                // _rowaddr is stored as UInt64 in Lance; use unsigned Int64 to match.
                new Field("_rowaddr", FieldType.notNullable(new ArrowType.Int(64, false)), null),
                new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    byte[] arrowData;
    try (VectorSchemaRoot updateRoot = VectorSchemaRoot.create(updateSchema, allocator)) {
      updateRoot.allocateNew();
      ((UInt8Vector) updateRoot.getVector("_rowaddr")).setSafe(0, rowAddr);
      ((IntVector) updateRoot.getVector("value")).setSafe(0, newValue);
      updateRoot.setRowCount(1);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(updateRoot, null, out)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      arrowData = out.toByteArray();
    }

    try (Dataset dataset = Dataset.open(datasetUri, allocator);
        ArrowStreamReader reader =
            new ArrowStreamReader(new ByteArrayInputStream(arrowData), allocator);
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, stream);
      Fragment frag = new Fragment(dataset, fragmentId);
      FragmentUpdateResult result = frag.updateColumns(stream, "_rowaddr", "_rowaddr");
      return result.getUpdatedFragment();
    }
  }

  /**
   * Simulates a driver commit with readVersion taken from a fresh open at commit time (current
   * head), not the scan-time version, which is incorrect behavior.
   */
  private void simulateDriverCommit(
      String datasetUri, BufferAllocator allocator, List<FragmentMetadata> updatedFragments)
      throws IOException {
    // Collect unmodified fragments (those not in the updated set), as the real commit() does.
    Set<Integer> updatedIds = Collections.singleton(updatedFragments.get(0).getId());
    try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
      dataset.getFragments().stream()
          .filter(f -> !updatedIds.contains(f.getId()))
          .map(Fragment::metadata)
          .forEach(updatedFragments::add);
    }

    // readVersion is dataset.version() at commit time (current head), not scan time.
    // Any concurrent writer that committed between executor scan and now is absorbed into
    // readVersion, so Rust OCC finds no intermediate transactions and always succeeds.
    try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
      long wrongReadVersion = dataset.version();
      Update update =
          Update.builder()
              .updatedFragments(updatedFragments)
              .updateMode(Optional.of(Update.UpdateMode.RewriteColumns))
              .build();
      try (Transaction txn =
              new Transaction.Builder().readVersion(wrongReadVersion).operation(update).build();
          Dataset committed = new CommitBuilder(dataset).execute(txn)) {
        // committed dataset auto-closed
      }
    }
  }

  /**
   * Demonstrates silent data inconsistency when readVersion is the post-DELETE head: OCC does not
   * reconcile the executor's stale fragment with the intermediate DELETE.
   *
   * <p>The concurrent writer is a DELETE. Executor metadata has no deletion file (correct for scan
   * time). Driver update commit still goes through and the committed fragment lacks the deletion
   * file from the DELETE.
   */
  @Test
  public void testConcurrentDeleteLostByWrongReadVersion() throws IOException {
    String datasetUri = tempDir.resolve("wrong_read_version_commit_test.lance").toString();

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // V1: dataset with 3 rows (id=1,value=10), (id=2,value=20), (id=3,value=30)
      FragmentMetadata initialFrag = createInitialDataset(datasetUri, allocator);
      int fragmentId = initialFrag.getId();

      // Executor scans at V1, computes new value=200 for row at index 1 (id=2).
      // The returned FragmentMetadata has no deletion file (none at scan time).
      FragmentMetadata updatedFrag =
          simulateExecutorUpdateColumns(datasetUri, allocator, fragmentId, 1, 200);

      assertNull(
          updatedFrag.getDeletionFile(),
          "No deletion existed at scan time (V1); executor fragment must carry no deletion file");

      // Concurrent DELETE creates V2 between executor scan and driver commit.
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        dataset.delete("id = 2");
      }

      // Driver commit with wrong readVersion = current head (V2)
      // OCC sees no intermediate work to rebase against
      // that readVersion, so the commit succeeds and the DELETE can be lost.
      List<FragmentMetadata> updatedFragments = new ArrayList<>();
      updatedFragments.add(updatedFrag);
      simulateDriverCommit(datasetUri, allocator, updatedFragments);

      // After wrong readVersion commit: concurrent DELETE is not preserved (no deletion file).
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        FragmentMetadata frag = dataset.getFragments().get(0).metadata();
        assertNull(frag.getDeletionFile(), "committed fragment should miss the deletion file.");
      }
    }
  }
}
