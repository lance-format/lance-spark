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
package org.lance.spark.search;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceSearchRowPartitionReaderTest {

  @Test
  void anEmptyBatchDoesNotEndTheStream() throws IOException {
    // A fragment-restricted prefiltered search can hand back a batch whose filter excluded every
    // row. Treating that as end of stream silently drops everything after it.
    PartitionReader<ColumnarBatch> batches =
        batchReader(Arrays.asList(Collections.emptyList(), Arrays.asList(1, 2)));

    assertEquals(Arrays.asList(1, 2), drain(batches));
  }

  @Test
  void emptyBatchesInEveryPositionAreSkipped() throws IOException {
    PartitionReader<ColumnarBatch> batches =
        batchReader(
            Arrays.asList(
                Collections.emptyList(),
                Collections.singletonList(1),
                Collections.emptyList(),
                Arrays.asList(2, 3),
                Collections.emptyList()));

    assertEquals(Arrays.asList(1, 2, 3), drain(batches));
  }

  @Test
  void onlyEmptyBatchesReturnNoRows() throws IOException {
    PartitionReader<ColumnarBatch> batches =
        batchReader(Arrays.asList(Collections.emptyList(), Collections.emptyList()));

    assertEquals(Collections.emptyList(), drain(batches));
  }

  private static List<Integer> drain(PartitionReader<ColumnarBatch> batches) throws IOException {
    LanceSearchRowPartitionReader reader = new LanceSearchRowPartitionReader(batches);
    List<Integer> ids = new ArrayList<>();
    while (reader.next()) {
      ids.add(reader.get().getInt(0));
    }
    assertFalse(reader.next(), "a drained reader stays drained");
    reader.close();
    return ids;
  }

  /** Serves one {@link ColumnarBatch} per list of ids, including empty ones. */
  private static PartitionReader<ColumnarBatch> batchReader(List<List<Integer>> batches) {
    return new PartitionReader<ColumnarBatch>() {
      private int index = -1;

      @Override
      public boolean next() {
        index++;
        return index < batches.size();
      }

      @Override
      public ColumnarBatch get() {
        List<Integer> ids = batches.get(index);
        OnHeapColumnVector vector = new OnHeapColumnVector(ids.size(), DataTypes.IntegerType);
        for (int i = 0; i < ids.size(); i++) {
          vector.putInt(i, ids.get(i));
        }
        ColumnarBatch batch = new ColumnarBatch(new ColumnVector[] {vector});
        batch.setNumRows(ids.size());
        return batch;
      }

      @Override
      public void close() {}
    };
  }

  @Test
  void getReturnsTheRowMostRecentlyAdvancedTo() throws IOException {
    PartitionReader<ColumnarBatch> batches =
        batchReader(Arrays.asList(Arrays.asList(7, 8), Collections.singletonList(9)));
    LanceSearchRowPartitionReader reader = new LanceSearchRowPartitionReader(batches);

    assertTrue(reader.next());
    assertEquals(7, reader.get().getInt(0));
    assertTrue(reader.next());
    assertEquals(8, reader.get().getInt(0));
    assertTrue(reader.next());
    assertEquals(9, reader.get().getInt(0));
    assertFalse(reader.next());
    reader.close();
  }
}
