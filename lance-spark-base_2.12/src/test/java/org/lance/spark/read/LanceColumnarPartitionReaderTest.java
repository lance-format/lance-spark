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
package org.lance.spark.read;

import org.lance.ipc.ColumnOrdering;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LanceColumnarPartitionReaderTest {
  @Test
  public void test() throws Exception {
    LanceSplit split = new LanceSplit(Arrays.asList(0, 1));
    LanceInputPartition partition =
        new LanceInputPartition(
            TestUtils.TestTable1Config.schema,
            0 /* partitionId */,
            split,
            TestUtils.TestTable1Config.readOptions,
            Optional.empty() /* whereCondition */,
            Optional.empty() /* limit */,
            Optional.empty() /* offset */,
            Optional.empty() /* topNSortOrders */,
            Optional.empty() /* pushedAggregation */,
            "test" /* scanId */,
            null /* initialStorageOptions */,
            null /* namespaceImpl */,
            null /* namespaceProperties */);
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
      List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;
      int rowIndex = 0;

      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        assertNotNull(batch);

        for (int i = 0; i < batch.numRows(); i++) {
          for (int j = 0; j < batch.numCols(); j++) {
            long actualValue = batch.column(j).getLong(i);
            long expectedValue = expectedValues.get(rowIndex).get(j);
            assertEquals(
                expectedValue, actualValue, "Mismatch at row " + rowIndex + " column " + j);
          }
          rowIndex++;
        }
        batch.close();
      }

      assertEquals(expectedValues.size(), rowIndex);
    }
  }

  @Test
  public void testOffsetAndLimit() throws Exception {
    LanceSplit split = new LanceSplit(Collections.singletonList(0));
    LanceInputPartition partition =
        new LanceInputPartition(
            TestUtils.TestTable1Config.schema,
            0 /* partitionId */,
            split,
            TestUtils.TestTable1Config.readOptions,
            Optional.empty() /* whereCondition */,
            Optional.of(1) /* limit */,
            Optional.of(1) /* offset */,
            Optional.empty() /* topNSortOrders */,
            Optional.empty() /* pushedAggregation */,
            "testOffsetAndLimit" /* scanId */,
            null /* initialStorageOptions */,
            null /* namespaceImpl */,
            null /* namespaceProperties */);
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
      List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;
      int rowIndex = 1;

      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        assertNotNull(batch);
        assertEquals(1, batch.numRows());
        for (int i = 0; i < batch.numRows(); i++) {
          for (int j = 0; j < batch.numCols(); j++) {
            long actualValue = batch.column(j).getLong(i);
            long expectedValue = expectedValues.get(rowIndex).get(j);
            assertEquals(
                expectedValue, actualValue, "Mismatch at row " + rowIndex + " column " + j);
          }
          rowIndex++;
        }
        batch.close();
      }
    }
  }

  @Test
  public void testTopN() throws Exception {
    LanceSplit split = new LanceSplit(Collections.singletonList(1));
    ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
    builder.setNullFirst(true);
    builder.setAscending(false);
    builder.setColumnName("b");
    LanceInputPartition partition =
        new LanceInputPartition(
            TestUtils.TestTable1Config.schema,
            0 /* partitionId */,
            split,
            TestUtils.TestTable1Config.readOptions,
            Optional.empty() /* whereCondition */,
            Optional.of(1) /* limit */,
            Optional.empty() /* offset */,
            Optional.of(Collections.singletonList(builder.build())) /* topNSortOrders */,
            Optional.empty() /* pushedAggregation */,
            "testTopN" /* scanId */,
            null /* initialStorageOptions */,
            null /* namespaceImpl */,
            null /* namespaceProperties */);
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
      List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;

      // Only get the 4th row
      int rowIndex = 3;
      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        assertNotNull(batch);
        assertEquals(1, batch.numRows());
        for (int i = 0; i < batch.numRows(); i++) {
          for (int j = 0; j < batch.numCols(); j++) {
            long actualValue = batch.column(j).getLong(i);
            long expectedValue = expectedValues.get(rowIndex).get(j);
            assertEquals(
                expectedValue, actualValue, "Mismatch at row " + rowIndex + " column " + j);
          }
        }
        batch.close();
      }
    }
  }
}
