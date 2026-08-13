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
import org.lance.WriteParams;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractBackfillWriterTest {
  @TempDir Path tempDir;

  @Test
  public void streamsFragmentInMaxBatchByteBoundedBatches() throws Exception {
    String datasetUri = tempDir.resolve("bounded-backfill.lance").toString();
    Schema datasetSchema =
        new Schema(
            Collections.singletonList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset ignored =
            Dataset.create(
                allocator, datasetUri, datasetSchema, new WriteParams.Builder().build())) {
      // The dataset only needs to exist so the backfill writer can open its pinned target.
    }

    StructType inputSchema =
        new StructType()
            .add(LanceConstant.ROW_ADDRESS, DataTypes.LongType, false)
            .add(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType, false)
            .add("value", DataTypes.BinaryType, true);
    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder()
            .datasetUri(datasetUri)
            .batchSize(100)
            .maxBatchBytes(1)
            .build();

    CountingBackfillWriter writer = new CountingBackfillWriter(options, inputSchema);
    try {
      writer.write(new GenericInternalRow(new Object[] {0L, 0, new byte[1024 * 1024]}));
      writer.write(new GenericInternalRow(new Object[] {1L, 0, new byte[1024 * 1024]}));
      writer.commit();

      assertEquals(2, writer.batchCount.get());
      assertEquals(1, writer.maxRowsPerBatch.get());
    } finally {
      writer.close();
    }
  }

  private static class CountingBackfillWriter extends AbstractBackfillWriter {
    final AtomicInteger batchCount = new AtomicInteger();
    final AtomicInteger maxRowsPerBatch = new AtomicInteger();

    CountingBackfillWriter(LanceSparkWriteOptions options, StructType schema) {
      super(
          options,
          schema,
          Collections.singletonList("value"),
          Collections.emptyMap(),
          null,
          Collections.emptyMap(),
          Collections.emptyList(),
          Collections.emptyMap());
    }

    @Override
    protected void processFragment(Fragment fragment, ArrowArrayStream stream) {
      try (ArrowReader reader = Data.importArrayStream(LanceRuntime.allocator(), stream)) {
        while (reader.loadNextBatch()) {
          batchCount.incrementAndGet();
          maxRowsPerBatch.accumulateAndGet(reader.getVectorSchemaRoot().getRowCount(), Math::max);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected WriterCommitMessage buildCommitMessage() {
      return new TestCommitMessage();
    }
  }

  private static class TestCommitMessage implements WriterCommitMessage {}
}
