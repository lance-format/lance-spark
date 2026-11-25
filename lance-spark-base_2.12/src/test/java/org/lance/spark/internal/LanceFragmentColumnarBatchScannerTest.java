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
package org.lance.spark.internal;

import org.lance.spark.TestUtils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceFragmentColumnarBatchScannerTest {

  @Test
  public void scanner() throws IOException {
    List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;
    int rowIndex = 0;
    int fragmentId = 0;
    while (fragmentId <= 1) {
      try (LanceFragmentColumnarBatchScanner scanner =
          LanceFragmentColumnarBatchScanner.create(
              fragmentId, TestUtils.TestTable1Config.inputPartition)) {
        while (scanner.loadNextBatch()) {
          try (ColumnarBatch batch = scanner.getCurrentBatch()) {
            Iterator<InternalRow> rows = batch.rowIterator();
            while (rows.hasNext()) {
              InternalRow row = rows.next();
              assertNotNull(row);
              for (int colIndex = 0; colIndex < row.numFields(); colIndex++) {
                long actualValue = row.getLong(colIndex);
                long expectedValue = expectedValues.get(rowIndex).get(colIndex);
                assertEquals(
                    expectedValue,
                    actualValue,
                    "Mismatch at row " + rowIndex + " column " + colIndex);
              }
              rowIndex++;
            }
          }
        }
      }
      fragmentId++;
    }
    assertEquals(4, rowIndex);
  }

  private static final class RefusingListener implements AllocationListener {
    private final int refused;
    private int allocations;

    RefusingListener(int refused) {
      this.refused = refused;
    }

    @Override
    public void onPreAllocation(long size) {
      if (++allocations >= refused) {
        throw new IllegalStateException("refused allocation " + allocations);
      }
    }
  }

  @Test
  public void refusedImportReleasesTheExportedStream() throws Exception {
    byte[] ipc = oneBatchStream();
    // Refuses the stream struct, the importer's struct, the schema or the first batch's array. The
    // producer exports a Java stream, so its allocator holds whatever the import leaks.
    for (int refused = 1; refused <= 4; refused++) {
      try (BufferAllocator producer = new RootAllocator();
          BufferAllocator consumer =
              new RootAllocator(new RefusingListener(refused), Long.MAX_VALUE)) {
        boolean[] exported = {false};
        boolean[] released = {false};
        // Releasing the exported stream closes the reader it exports.
        ArrowReader source =
            new ArrowStreamReader(new ByteArrayInputStream(ipc), producer) {
              @Override
              protected void closeReadSource() throws IOException {
                released[0] = true;
                super.closeReadSource();
              }
            };
        RuntimeException failure =
            assertThrows(
                RuntimeException.class,
                () -> {
                  try (ArrowReader reader =
                      LanceFragmentColumnarBatchScanner.importArrowStream(
                          consumer,
                          address -> {
                            Data.exportArrayStream(
                                producer, source, ArrowArrayStream.wrap(address));
                            exported[0] = true;
                          })) {
                    while (reader.loadNextBatch()) {
                      // Read until the refusal.
                    }
                  }
                });
        assertTrue(
            causes(failure, "refused allocation " + refused),
            "allocation " + refused + " failed otherwise: " + failure);
        assertEquals(0, consumer.getAllocatedMemory(), "allocation " + refused);
        if (exported[0]) {
          assertTrue(released[0], "refusing allocation " + refused + " did not release the stream");
        } else {
          source.close();
        }
        assertEquals(
            0,
            producer.getAllocatedMemory(),
            "refusing allocation " + refused + " leaked the stream");
      }
    }
  }

  private static boolean causes(Throwable failure, String message) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(message)) {
        return true;
      }
    }
    return false;
  }

  private static byte[] oneBatchStream() throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector ids = new IntVector("id", allocator)) {
      ids.allocateNew(1024);
      for (int i = 0; i < 1024; i++) {
        ids.set(i, i);
      }
      ids.setValueCount(1024);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(ids));
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return out.toByteArray();
    }
  }
}
