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
import org.apache.spark.unsafe.types.UTF8String;
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

      try {
        assertEquals(totalRows, rowsWritten.get());
        assertEquals(totalRows, rowsRead.get());
      } finally {
        writeBuffer.close();
      }
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

      try {
        assertEquals(totalRows, rowsWritten.get());
        assertEquals(totalRows, rowsRead.get());
      } finally {
        writeBuffer.close();
      }
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

      try {
        assertEquals(totalRows, rowsWritten.get());
        assertEquals(totalRows, rowsRead.get());
      } finally {
        writeBuffer.close();
      }
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

  // ========== Byte-based flush tests ==========

  private Schema createStringSchema() {
    Field field =
        new Field(
            "data",
            FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()),
            null);
    return new Schema(Collections.singletonList(field));
  }

  private StructType createStringSparkSchema() {
    return new StructType(
        new StructField[] {DataTypes.createStructField("data", DataTypes.StringType, true)});
  }

  /** Generate a string of approximately the given size in bytes. */
  private UTF8String generateLargeString(int sizeBytes) {
    byte[] data = new byte[sizeBytes];
    java.util.Arrays.fill(data, (byte) 'A');
    return UTF8String.fromBytes(data);
  }

  @Test
  public void testByteBasedFlushWithSmallRows() throws Exception {
    // With small rows, the row count limit should be reached before byte limit.
    // This verifies that maxBatchBytes does not interfere with normal row-count flushing.
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 100;
      final int batchSize = 25;
      final long maxBatchBytes = 100 * 1024 * 1024; // 100MB - should never be reached
      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, maxBatchBytes);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);

      runWriterReader(writeBuffer, totalRows, rowsWritten, rowsRead);

      try {
        assertEquals(totalRows, rowsWritten.get());
        assertEquals(totalRows, rowsRead.get());
      } finally {
        writeBuffer.close();
      }
    }
  }

  @Test
  public void testByteBasedFlush() throws Exception {
    // Each row is ~100KB. With batchSize=1000 (would be 100MB), but maxBatchBytes=256KB,
    // we should see batches flush after ~2-3 rows instead of waiting for 1000.
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createStringSchema();
      StructType sparkSchema = createStringSparkSchema();

      final int totalRows = 20;
      final int batchSize = 1000; // High row limit - should never be reached
      final long maxBatchBytes = 256 * 1024; // 256KB - should trigger flush after ~2 rows
      final int rowSizeBytes = 100 * 1024; // ~100KB per row
      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, maxBatchBytes);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger batchCount = new AtomicInteger(0);
      AtomicInteger maxRowsInBatch = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < totalRows; i++) {
                    UTF8String largeValue = generateLargeString(rowSizeBytes);
                    InternalRow row = new GenericInternalRow(new Object[] {largeValue});
                    writeBuffer.write(row);
                    rowsWritten.incrementAndGet();
                  }
                } finally {
                  writeBuffer.setFinished();
                }
              });

      Callable<Void> readerCallable =
          () -> {
            while (writeBuffer.loadNextBatch()) {
              VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
              int rowCount = root.getRowCount();
              rowsRead.addAndGet(rowCount);
              batchCount.incrementAndGet();
              maxRowsInBatch.updateAndGet(prev -> Math.max(prev, rowCount));
            }
            return null;
          };

      FutureTask<Void> readerTask = writeBuffer.createTrackedTask(readerCallable);
      Thread readerThread = new Thread(readerTask);
      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      try {
        assertEquals(totalRows, rowsWritten.get());
        assertEquals(totalRows, rowsRead.get());
        // With 100KB rows and 256KB limit, each batch should have at most ~3 rows.
        // Without byte-based flush, we'd get 1 batch of 20 rows (since batchSize=1000).
        Assertions.assertTrue(
            batchCount.get() > 1,
            "Should have multiple batches due to byte-based flushing, but got " + batchCount.get());
        Assertions.assertTrue(
            maxRowsInBatch.get() < batchSize,
            "Max rows per batch ("
                + maxRowsInBatch.get()
                + ") should be less than batchSize ("
                + batchSize
                + ") due to byte-based flushing");
      } finally {
        writeBuffer.close();
      }
    }
  }

  @Test
  public void testByteBasedFlushSingleLargeRow() throws Exception {
    // A single row exceeds maxBatchBytes - should flush after each row.
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createStringSchema();
      StructType sparkSchema = createStringSparkSchema();

      final int totalRows = 5;
      final int batchSize = 1000;
      final long maxBatchBytes = 1024; // 1KB - each row will exceed this
      final int rowSizeBytes = 10 * 1024; // 10KB per row
      final SemaphoreArrowBatchWriteBuffer writeBuffer =
          new SemaphoreArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, maxBatchBytes);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger batchCount = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < totalRows; i++) {
                    UTF8String largeValue = generateLargeString(rowSizeBytes);
                    InternalRow row = new GenericInternalRow(new Object[] {largeValue});
                    writeBuffer.write(row);
                    rowsWritten.incrementAndGet();
                  }
                } finally {
                  writeBuffer.setFinished();
                }
              });

      Callable<Void> readerCallable =
          () -> {
            while (writeBuffer.loadNextBatch()) {
              VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
              rowsRead.addAndGet(root.getRowCount());
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

      try {
        assertEquals(totalRows, rowsWritten.get());
        assertEquals(totalRows, rowsRead.get());
        // Each row should produce its own batch since each exceeds the byte limit
        assertEquals(
            totalRows,
            batchCount.get(),
            "Each row should be its own batch when row size exceeds maxBatchBytes");
      } finally {
        writeBuffer.close();
      }
    }
  }
}
