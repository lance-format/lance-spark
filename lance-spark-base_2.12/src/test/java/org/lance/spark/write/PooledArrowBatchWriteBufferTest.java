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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class PooledArrowBatchWriteBufferTest {

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
      PooledArrowBatchWriteBuffer writeBuffer,
      int totalRows,
      AtomicInteger rowsWritten,
      AtomicInteger rowsRead,
      AtomicReference<Throwable> writerError,
      AtomicReference<Throwable> readerError)
      throws InterruptedException {
    Thread writerThread =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < totalRows; i++) {
                  InternalRow row =
                      new GenericInternalRow(new Object[] {rowsWritten.incrementAndGet()});
                  writeBuffer.write(row);
                }
                writeBuffer.setFinished();
              } catch (Throwable e) {
                writerError.set(e);
              }
            });

    Thread readerThread =
        new Thread(
            () -> {
              try {
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
              } catch (Throwable e) {
                readerError.set(e);
              }
            });

    writerThread.start();
    readerThread.start();
    writerThread.join();
    readerThread.join();
  }

  @Test
  public void testBasicWriteAndRead() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 125;
      final int batchSize = 34;
      final int poolSize = 4;
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, poolSize, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicReference<Throwable> writerError = new AtomicReference<>();
      AtomicReference<Throwable> readerError = new AtomicReference<>();

      runWriterReader(writeBuffer, totalRows, rowsWritten, rowsRead, writerError, readerError);

      assertNull(writerError.get(), "Writer thread should not have errors");
      assertNull(readerError.get(), "Reader thread should not have errors");
      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testPartialBatch() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 50;
      final int batchSize = 34;
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger batchCount = new AtomicInteger(0);

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

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    batchCount.incrementAndGet();
                    VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
                    rowsRead.addAndGet(root.getRowCount());
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      assertEquals(2, batchCount.get()); // 1 full + 1 partial
      writeBuffer.close();
    }
  }

  @Test
  public void testEmptyWrite() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(allocator, schema, sparkSchema, 100, 2, Long.MAX_VALUE);

      AtomicInteger batchCount = new AtomicInteger(0);

      Thread writerThread = new Thread(writeBuffer::setFinished);

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    batchCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(0, batchCount.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testLargeDataset() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 10000;
      final int batchSize = 512;
      final int poolSize = 8;
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, poolSize, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicLong bytesRead = new AtomicLong(0);

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

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
                    rowsRead.addAndGet(root.getRowCount());
                    try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
                      bytesRead.addAndGet(recordBatch.computeBodyLength());
                    }
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      assertTrue(bytesRead.get() > 0);
      writeBuffer.close();
    }
  }

  @Test
  public void testPoolSizeOne() throws Exception {
    // Pool size 1 = serial producer/consumer, equivalent to old semaphore behavior
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 100;
      final int batchSize = 10;
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 1, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicReference<Throwable> writerError = new AtomicReference<>();
      AtomicReference<Throwable> readerError = new AtomicReference<>();

      runWriterReader(writeBuffer, totalRows, rowsWritten, rowsRead, writerError, readerError);

      assertNull(writerError.get());
      assertNull(readerError.get());
      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testMultipleColumns() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field intField =
          new Field(
              "intCol",
              FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
              null);
      Field longField =
          new Field(
              "longCol",
              FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType()),
              null);
      Schema schema = new Schema(Arrays.asList(intField, longField));

      StructType sparkSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("intCol", DataTypes.IntegerType, true),
                DataTypes.createStructField("longCol", DataTypes.LongType, true)
              });

      final int totalRows = 200;
      final int batchSize = 50;
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                for (int i = 0; i < totalRows; i++) {
                  int val = rowsWritten.incrementAndGet();
                  InternalRow row = new GenericInternalRow(new Object[] {val, (long) val * 100});
                  writeBuffer.write(row);
                }
                writeBuffer.setFinished();
              });

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
                    int rowCount = root.getRowCount();
                    int baseValue = rowsRead.get();
                    rowsRead.addAndGet(rowCount);
                    for (int i = 0; i < rowCount; i++) {
                      int intVal = (int) root.getVector("intCol").getObject(i);
                      long longVal = (long) root.getVector("longCol").getObject(i);
                      assertEquals(baseValue + i + 1, intVal);
                      assertEquals((baseValue + i + 1) * 100L, longVal);
                    }
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                  fail("Reader thread failed: " + e.getMessage());
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testExactBatchBoundary() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int batchSize = 10;
      final int totalRows = 30; // Exactly 3 batches
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger batchCount = new AtomicInteger(0);
      AtomicReference<Throwable> writerError = new AtomicReference<>();
      AtomicReference<Throwable> readerError = new AtomicReference<>();

      Thread writerThread =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < totalRows; i++) {
                    InternalRow row =
                        new GenericInternalRow(new Object[] {rowsWritten.incrementAndGet()});
                    writeBuffer.write(row);
                  }
                  writeBuffer.setFinished();
                } catch (Throwable e) {
                  writerError.set(e);
                }
              });

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    batchCount.incrementAndGet();
                    VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
                    rowsRead.addAndGet(root.getRowCount());
                    assertEquals(batchSize, root.getRowCount());
                  }
                } catch (Throwable e) {
                  readerError.set(e);
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertNull(writerError.get());
      assertNull(readerError.get());
      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      assertEquals(3, batchCount.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testSingleRowBatch() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int batchSize = 1;
      final int totalRows = 5;
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicReference<Throwable> writerError = new AtomicReference<>();
      AtomicReference<Throwable> readerError = new AtomicReference<>();

      runWriterReader(writeBuffer, totalRows, rowsWritten, rowsRead, writerError, readerError);

      assertNull(writerError.get());
      assertNull(readerError.get());
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

      final int totalRows = 100;
      final int batchSize = 10;
      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, Long.MAX_VALUE);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      CountDownLatch readerConsumedBatch = new CountDownLatch(1);

      Callable<Integer> read =
          () -> {
            if (writeBuffer.loadNextBatch()) {
              VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
              rowsRead.addAndGet(root.getRowCount());
              readerConsumedBatch.countDown();
              throw new RuntimeException("Mock exception");
            }
            return rowsRead.get();
          };

      FutureTask<Integer> readTask = writeBuffer.createTrackedTask(read);
      Thread readerThread = new Thread(readTask);
      readerThread.start();

      assertThrows(
          RuntimeException.class,
          () -> {
            try {
              for (int i = 0; i < totalRows; i++) {
                InternalRow row = new GenericInternalRow(new Object[] {i + 1});
                writeBuffer.write(row);
                rowsWritten.incrementAndGet();

                if (rowsWritten.get() >= batchSize) {
                  readerConsumedBatch.await();
                  while (!readTask.isDone()) {
                    Thread.sleep(1);
                  }
                }
              }
            } finally {
              writeBuffer.setFinished();
            }
          });

      assertThrows(ExecutionException.class, readTask::get);

      assertEquals(batchSize, rowsWritten.get());
      assertEquals(batchSize, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testByteBasedFlushWithSmallRows() throws Exception {
    // Small rows should not trigger byte-based flush — only row count matters
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 100;
      final int batchSize = 50;
      // 256MB limit — small int rows will never reach this
      final long maxBatchBytes = 256L * 1024 * 1024;

      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, maxBatchBytes);

      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger batchCount = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                for (int i = 0; i < totalRows; i++) {
                  writeBuffer.write(new GenericInternalRow(new Object[] {i}));
                }
                writeBuffer.setFinished();
              });

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    batchCount.incrementAndGet();
                    rowsRead.addAndGet(writeBuffer.getVectorSchemaRoot().getRowCount());
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(totalRows, rowsRead.get());
      assertEquals(2, batchCount.get()); // 100 rows / 50 batch size = 2 batches
      writeBuffer.close();
    }
  }

  @Test
  public void testByteBasedFlushWithLargeStrings() throws Exception {
    // Large string rows should trigger byte-based flush before row count limit
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field stringField =
          new Field(
              "data",
              FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()),
              null);
      Schema schema = new Schema(Collections.singletonList(stringField));
      StructType sparkSchema =
          new StructType(
              new StructField[] {DataTypes.createStructField("data", DataTypes.StringType, true)});

      final int totalRows = 20;
      final int batchSize = 1000; // High row limit — byte limit should trigger first
      // ~100KB strings, 256KB byte limit → should flush every ~2-3 rows
      final long maxBatchBytes = 256L * 1024;

      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, maxBatchBytes);

      // Build a ~100KB string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 100 * 1024; i++) {
        sb.append('x');
      }
      String largeString = sb.toString();

      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger batchCount = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                for (int i = 0; i < totalRows; i++) {
                  writeBuffer.write(
                      new GenericInternalRow(
                          new Object[] {UTF8String.fromString(largeString + i)}));
                }
                writeBuffer.setFinished();
              });

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    batchCount.incrementAndGet();
                    VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
                    rowsRead.addAndGet(root.getRowCount());
                    // Each batch should have fewer than batchSize rows
                    assertTrue(
                        root.getRowCount() < batchSize,
                        "Byte-based flush should trigger before row count limit");
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(totalRows, rowsRead.get());
      // Should have more batches than if only row-count-based flushing
      assertTrue(batchCount.get() > 1, "Should have multiple batches from byte-based flush");
      writeBuffer.close();
    }
  }

  @Test
  public void testByteBasedFlushSingleLargeRow() throws Exception {
    // A single row exceeding the byte limit should flush as a batch of 1
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field stringField =
          new Field(
              "data",
              FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()),
              null);
      Schema schema = new Schema(Collections.singletonList(stringField));
      StructType sparkSchema =
          new StructType(
              new StructField[] {DataTypes.createStructField("data", DataTypes.StringType, true)});

      final int totalRows = 5;
      final int batchSize = 1000;
      // 1KB byte limit with ~10KB strings → 1 row per batch
      final long maxBatchBytes = 1024;

      final PooledArrowBatchWriteBuffer writeBuffer =
          new PooledArrowBatchWriteBuffer(
              allocator, schema, sparkSchema, batchSize, 4, maxBatchBytes);

      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 10 * 1024; i++) {
        sb.append('y');
      }
      String largeString = sb.toString();

      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger batchCount = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                for (int i = 0; i < totalRows; i++) {
                  writeBuffer.write(
                      new GenericInternalRow(new Object[] {UTF8String.fromString(largeString)}));
                }
                writeBuffer.setFinished();
              });

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    batchCount.incrementAndGet();
                    VectorSchemaRoot root = writeBuffer.getVectorSchemaRoot();
                    rowsRead.addAndGet(root.getRowCount());
                    assertEquals(1, root.getRowCount(), "Each batch should contain exactly 1 row");
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });

      writerThread.start();
      readerThread.start();
      writerThread.join();
      readerThread.join();

      assertEquals(totalRows, rowsRead.get());
      assertEquals(totalRows, batchCount.get()); // 1 row per batch
      writeBuffer.close();
    }
  }
}
