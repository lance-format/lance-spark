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

import org.lance.FragmentMetadata;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class LanceDataWriterTest {
  @TempDir static Path tempDir;

  @Test
  public void testLanceDataWriter(TestInfo testInfo) throws IOException {
    String datasetName = testInfo.getTestMethod().get().getName();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(field));
      LanceSparkWriteOptions writeOptions =
          LanceSparkWriteOptions.from(
              tempDir.resolve(datasetName + LanceSparkReadOptions.LANCE_FILE_SUFFIX).toString());
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
      LanceDataWriter.WriterFactory writerFactory =
          new LanceDataWriter.WriterFactory(
              sparkSchema,
              writeOptions,
              null, // initialStorageOptions
              null, // namespaceImpl
              null, // namespaceProperties
              null); // tableId
      LanceDataWriter dataWriter = (LanceDataWriter) writerFactory.createWriter(0, 0);

      int rows = 132;
      for (int i = 0; i < rows; i++) {
        InternalRow row = new GenericInternalRow(new Object[] {i});
        dataWriter.write(row);
      }

      LanceBatchWrite.TaskCommit commitMessage = (LanceBatchWrite.TaskCommit) dataWriter.commit();
      dataWriter.close();
      List<FragmentMetadata> fragments = commitMessage.getFragments();
      assertEquals(1, fragments.size());
      assertEquals(rows, fragments.get(0).getPhysicalRows());
    }
  }

  /**
   * Tests that abort() does not hang when called after partial writes. This reproduces the deadlock
   * reported in issue #114: when a Spark task fails mid-write, the driver calls abort() which must
   * complete promptly. Before the fix, abort() would hang because the fragment creation thread's
   * consumer side was blocked waiting for more data that would never come.
   */
  @Test
  public void testAbortDoesNotHang(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(field));
      LanceSparkWriteOptions writeOptions =
          LanceSparkWriteOptions.from(
              tempDir.resolve(datasetName + LanceSparkReadOptions.LANCE_FILE_SUFFIX).toString());
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
      LanceDataWriter.WriterFactory writerFactory =
          new LanceDataWriter.WriterFactory(
              sparkSchema,
              writeOptions,
              null, // initialStorageOptions
              null, // namespaceImpl
              null, // namespaceProperties
              null); // tableId
      LanceDataWriter dataWriter = (LanceDataWriter) writerFactory.createWriter(0, 0);

      // Write partial data (fewer rows than batch size) to create the deadlock scenario:
      // the consumer thread is waiting for a full batch while the producer stops writing.
      int partialRows = 5;
      for (int i = 0; i < partialRows; i++) {
        InternalRow row = new GenericInternalRow(new Object[] {i});
        dataWriter.write(row);
      }

      // abort() must complete within a reasonable time.
      // Before the fix, this would hang indefinitely because the fragment creation
      // thread was blocked in loadNextBatch() waiting for more data.
      Thread abortThread =
          new Thread(
              () -> {
                try {
                  dataWriter.abort();
                } catch (IOException e) {
                  // ExecutionException is expected since fragment creation may fail on abort
                }
              });
      abortThread.start();
      abortThread.join(TimeUnit.SECONDS.toMillis(30));

      assertFalse(
          abortThread.isAlive(),
          "abort() should complete within 30 seconds, but it is still hanging. "
              + "This indicates the deadlock from issue #114 is present.");
    }
  }
}
