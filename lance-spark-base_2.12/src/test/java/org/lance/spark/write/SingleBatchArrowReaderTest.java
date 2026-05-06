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

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SingleBatchArrowReaderTest {

  private VectorSchemaRoot createIntRoot(BufferAllocator allocator, int rowCount) {
    Field field =
        new Field(
            "column",
            FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
            null);
    Schema schema = new Schema(Collections.singletonList(field));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    IntVector vector = (IntVector) root.getVector("column");
    vector.allocateNew(rowCount);
    for (int i = 0; i < rowCount; i++) {
      vector.set(i, i + 1);
    }
    vector.setValueCount(rowCount);
    root.setRowCount(rowCount);
    return root;
  }

  @Test
  public void testLoadNextBatch() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot source = createIntRoot(allocator, 3);
        SingleBatchArrowReader reader = new SingleBatchArrowReader(allocator, source)) {
      assertTrue(reader.loadNextBatch());

      VectorSchemaRoot out = reader.getVectorSchemaRoot();
      assertEquals(source.getSchema(), out.getSchema());
      assertEquals(3, out.getRowCount());
      for (int i = 0; i < 3; i++) {
        assertEquals(i + 1, out.getVector("column").getObject(i));
      }

      assertFalse(reader.loadNextBatch());
    }
  }

  @Test
  public void testExportToArrowArrayStream() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot source = createIntRoot(allocator, 5);
        SingleBatchArrowReader reader = new SingleBatchArrowReader(allocator, source);
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {

      Data.exportArrayStream(allocator, reader, stream);

      try (ArrowReader exportedReader = Data.importArrayStream(allocator, stream)) {
        assertTrue(exportedReader.loadNextBatch());
        VectorSchemaRoot exportedRoot = exportedReader.getVectorSchemaRoot();

        assertEquals(5, exportedRoot.getRowCount());

        IntVector exportedVector = (IntVector) exportedRoot.getVector("column");
        for (int i = 0; i < 5; i++) {
          assertEquals(i + 1, exportedVector.getObject(i));
        }

        assertFalse(exportedReader.loadNextBatch());
      }

      assertFalse(reader.loadNextBatch());
    }
  }
}
