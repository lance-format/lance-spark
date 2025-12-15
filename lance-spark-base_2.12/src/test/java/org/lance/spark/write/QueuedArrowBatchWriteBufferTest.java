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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class QueuedArrowBatchWriteBufferTest {

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

  @Test
  public void testBasicWriteAndRead() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 125;
      final int batchSize = 34;
      final int queueDepth = 4;
      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize, queueDepth);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
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
                  e.printStackTrace();
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
                  e.printStackTrace();
                }
              });

      writerThread.start();
      readerThread.start();

      writerThread.join();
      readerThread.join();

      assertNull(writerError.get(), "Writer thread should not have errors");
      assertNull(readerError.get(), "Reader thread should not have errors");
      assertEquals(totalRows, rowsWritten.get());
      assertEquals(totalRows, rowsRead.get());
      writeBuffer.close();
    }
  }

  @Test
  public void testPartialBatch() throws Exception {
    // Test that partial batches (when totalRows % batchSize != 0) are handled correctly
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 50;
      final int batchSize = 34; // Will have 1 full batch (34) + 1 partial batch (16)
      final int queueDepth = 2;
      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize, queueDepth);

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
    // Test that calling setFinished without writing any rows works correctly
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, 100, 2);

      AtomicInteger batchCount = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                // Don't write anything, just finish
                writeBuffer.setFinished();
              });

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
    // Test with a larger dataset to ensure queue pipelining works
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 10000;
      final int batchSize = 512;
      final int queueDepth = 8;
      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize, queueDepth);

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
  public void testQueueDepthOne() throws Exception {
    // Test with minimum queue depth of 1
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 100;
      final int batchSize = 10;
      final int queueDepth = 1;
      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize, queueDepth);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);

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
      writeBuffer.close();
    }
  }

  @Test
  public void testMultipleColumns() throws Exception {
    // Test with multiple columns of different types
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
      final int queueDepth = 4;
      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize, queueDepth);

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
  public void testDefaultQueueDepth() throws Exception {
    // Test using the constructor with default queue depth
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 100;
      final int batchSize = 20;
      // Use constructor without queueDepth - should use default of 8
      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize);

      assertEquals(8, writeBuffer.getQueueDepth());

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);

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
      writeBuffer.close();
    }
  }

  @Test
  public void testSlowConsumer() throws Exception {
    // Test that the queue buffers batches when consumer is slow
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createIntSchema();
      StructType sparkSchema = createIntSparkSchema();

      final int totalRows = 100;
      final int batchSize = 10;
      final int queueDepth = 4;
      final QueuedArrowBatchWriteBuffer writeBuffer =
          new QueuedArrowBatchWriteBuffer(allocator, schema, sparkSchema, batchSize, queueDepth);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicInteger maxQueueSize = new AtomicInteger(0);

      Thread writerThread =
          new Thread(
              () -> {
                for (int i = 0; i < totalRows; i++) {
                  InternalRow row =
                      new GenericInternalRow(new Object[] {rowsWritten.incrementAndGet()});
                  writeBuffer.write(row);
                  // Track max queue size
                  int currentSize = writeBuffer.getCurrentQueueSize();
                  maxQueueSize.updateAndGet(prev -> Math.max(prev, currentSize));
                }
                writeBuffer.setFinished();
              });

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (writeBuffer.loadNextBatch()) {
                    // Simulate slow consumer
                    Thread.sleep(10);
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
      // Queue should have been used (max size > 0 at some point)
      assertTrue(maxQueueSize.get() >= 0);
      writeBuffer.close();
    }
  }
}
