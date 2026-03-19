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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SemaphoreArrowBatchWriteBufferTest {

  private Schema createIntSchema() {
    Field field =
        new Field(
            "column1",
            FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
            null);
    return new Schema(Collections.singletonList(field));
  }

  private StructType createIntSparkSchema() {
    return new StructType(
        new StructField[] {DataTypes.createStructField("column1", DataTypes.IntegerType, true)});
  }

  private void runWriterReader(
      SemaphoreArrowBatchWriteBuffer writeBuffer,
      int totalRows,
      AtomicInteger rowsWritten,
      AtomicInteger rowsRead)
      throws Exception {
    Thread writerThread =
        new Thread(
            () -> {
              for (int i = 0; i < totalRows; i++) {
                InternalRow row =
                    new GenericInternalRow(new Object[] {rowsWritten.incrementAndGet()});
                writeBuffer.write(row);
              }
              writeBuffer.setFinished();
            });

    Callable<Void> readerCallable =
        () -> {
          while (writeBuffer.loadNextBatch()) {
            VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
            int rowCount = root.getRowCount();
            int baseValue = rowsRead.get();
            rowsRead.addAndGet(rowCount);
            for (int i = 0; i < rowCount; i++) {
              int value = (int) root.getVector("column1").getObject(i);
              assertEquals(baseValue + i + 1, value);
            }
          }
          return null;
        };

    FutureTask<Void> readerTask = writeBuffer.createTrackedTask(readerCallable);

    Thread readerThread = new Thread(readerTask);
    writerThread.start();
    readerThread.start();
    writerThread.join();
    readerThread.join();
  }

  @Test
  public void test() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 125;
      final int batchSize = 34;
      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);

      runWriterReader(writeBuffer, totalRows, rowsWritten, rowsRead);

      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testEmptyWrite() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(allocator, schema, sparkSchema, 34);

      AtomicInteger batchCount = new AtomicInteger(0);

      Thread writerThread = new Thread(writeBuffer::setFinished);

      Callable<Void> readerCallable =
          () -> {
            while (writeBuffer.loadNextBatch()) {
              batchCount.incrementAndGet();
            }
            return null;
          };

      FutureTask<Void> readerTask = writeBuffer.createTrackedTask(readerCallable);

      Thread readerThread = new Thread(readerTask);
      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(0, batchCount.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testExactBatchBoundary() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int batchSize = 25;
      final int totalRows = batchSize * 4; // exactly 100 rows = 4 full batches
      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);

      runWriterReader(writeBuffer, totalRows, rowsWritten, rowsRead);

      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testSingleRowBatch() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 50;
      final int batchSize = 1;
      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);

      runWriterReader(writeBuffer, totalRows, rowsWritten, rowsRead);

      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testWriteErrorPropagation() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 125;
      final int batchSize = 34;
      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicLong expectedBytesRead = new AtomicLong(0);

      Callable<Integer> read =
          () -> {
            if (writeBuffer.loadNextBatch()) {
              VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
              int rowCount = root.getRowCount();
              rowsRead.addAndGet(rowCount);
              try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
                expectedBytesRead.addAndGet(recordBatch.computeBodyLength());
              }
              for (int i = 0; i < rowCount; i++) {
                int value = (int) root.getVector("column1").getObject(i);
                assertEquals(value, rowsRead.get() - rowCount + i + 1);
              }

              // Throw a mock exception after reading a batch
              throw new RuntimeException("Mock exception");
            }
            return rowsRead.get();
          };

      // Start background thread to read data
      FutureTask<Integer> readTask = writeBuffer.createTrackedTask(read);
      Thread readerThread = new Thread(readTask);
      readerThread.start();

      // Write data
      Assertions.assertThrows(
          RuntimeException.class,
          () -> {
            for (int i = 0; i < totalRows; i++) {
              InternalRow row = new GenericInternalRow(new Object[] {i + 1});
              writeBuffer.write(row);
              rowsWritten.incrementAndGet();
            }
            writeBuffer.setFinished();
          });

      Assertions.assertThrows(ExecutionException.class, readTask::get);

      assertEquals(batchSize, rowsWritten.get());
      assertEquals(batchSize, rowsRead.get());
      writeBuffer.close();
    }
  }
}
