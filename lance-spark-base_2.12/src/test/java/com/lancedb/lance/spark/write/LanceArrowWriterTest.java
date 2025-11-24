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
package com.lancedb.lance.spark.write;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceArrowWriterTest {
  @Test
  public void test() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field field =
          new Field(
              "column1",
              FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
              null);
      Schema schema = new Schema(Collections.singletonList(field));

      StructType sparkSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("column1", DataTypes.IntegerType, true)
              });

      final int totalRows = 125;
      final int batchSize = 34;
      final LanceArrowWriter arrowWriter =
          new LanceArrowWriter(allocator, schema, sparkSchema, batchSize);

      AtomicInteger rowsWritten = new AtomicInteger(0);
      AtomicInteger rowsRead = new AtomicInteger(0);
      AtomicLong expectedBytesRead = new AtomicLong(0);

      Thread writerThread =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < totalRows; i++) {
                    InternalRow row =
                        new GenericInternalRow(new Object[] {rowsWritten.incrementAndGet()});
                    arrowWriter.write(row);
                  }
                  arrowWriter.setFinished();
                } catch (Exception e) {
                  e.printStackTrace();
                  throw e;
                }
              });

      Thread readerThread =
          new Thread(
              () -> {
                try {
                  while (arrowWriter.loadNextBatch()) {
                    VectorSchemaRoot root = arrowWriter.getVectorSchemaRoot();
                    int rowCount = root.getRowCount();
                    rowsRead.addAndGet(rowCount);
                    try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
                      expectedBytesRead.addAndGet(recordBatch.computeBodyLength());
                    }
                    for (int i = 0; i < rowCount; i++) {
                      int value = (int) root.getVector("column1").getObject(i);
                      assertEquals(value, rowsRead.get() - rowCount + i + 1);
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
      arrowWriter.close();
    }
  }

  @Test
  public void propagatesStructNullsToChildren() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field childField =
          new Field(
              "child", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
      Field structField =
          new Field(
              "struct_col",
              FieldType.nullable(ArrowType.Struct.INSTANCE),
              Collections.singletonList(childField));
      Schema arrowSchema = new Schema(Collections.singletonList(structField));

      StructType childType =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("child", DataTypes.IntegerType, true)
              });
      StructType sparkSchema =
          new StructType(
              new StructField[] {DataTypes.createStructField("struct_col", childType, true)});

      try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        com.lancedb.lance.spark.arrow.LanceArrowWriter structWriter =
            com.lancedb.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(root, sparkSchema);

        InternalRow[] rows =
            new InternalRow[] {
              new GenericInternalRow(new Object[] {new GenericInternalRow(new Object[] {1})}),
              new GenericInternalRow(new Object[] {null}),
              new GenericInternalRow(new Object[] {new GenericInternalRow(new Object[] {3})})
            };

        for (InternalRow row : rows) {
          structWriter.write(row);
        }
        structWriter.finish();

        StructVector structVector = (StructVector) root.getVector("struct_col");
        IntVector childVector = (IntVector) structVector.getChild("child");

        assertEquals(rows.length, structVector.getValueCount());
        assertEquals(rows.length, childVector.getValueCount());
        assertTrue(structVector.isNull(1));
        assertEquals(1, childVector.get(0));
        assertEquals(3, childVector.get(2));
      }
    }
  }
}
