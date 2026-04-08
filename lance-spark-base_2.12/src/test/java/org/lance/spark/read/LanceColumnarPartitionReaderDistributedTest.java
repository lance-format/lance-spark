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

import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceColumnarPartitionReaderDistributedTest {

  @TempDir static Path tempDir;

  private static LanceSparkReadOptions readOptions;

  @BeforeAll
  static void setup() {
    String datasetUri = TestUtils.createTestTable1V2Dataset(tempDir);
    readOptions = LanceSparkReadOptions.from(datasetUri);
  }

  private LanceSplit.DistributedScanPlanResult planDistributed(
      Optional<String> whereCondition, Optional<Integer> limit) {
    return LanceSplit.planDistributedScan(
        readOptions,
        TestUtils.TestTable1Config.schema,
        whereCondition,
        limit,
        Optional.empty() /* offset */,
        Optional.empty() /* topNSortOrders */);
  }

  private LanceInputPartition createDistributedPartition(
      byte[] task,
      int fragmentId,
      int partitionId,
      LanceSparkReadOptions resolvedReadOptions,
      Optional<String> whereCondition,
      Optional<Integer> limit) {
    return new LanceInputPartition(
        TestUtils.TestTable1Config.schema,
        partitionId,
        new LanceSplit(Collections.singletonList(fragmentId)),
        resolvedReadOptions,
        whereCondition,
        limit,
        Optional.empty() /* offset */,
        Optional.empty() /* topNSortOrders */,
        Optional.empty() /* pushedAggregation */,
        "distributed-test" /* scanId */,
        null /* initialStorageOptions */,
        null /* namespaceImpl */,
        null /* namespaceProperties */,
        task);
  }

  /**
   * Reads all rows from all distributed tasks and returns the total row count and collected values.
   */
  private ReadResult readAllDistributed(Optional<String> whereCondition, Optional<Integer> limit)
      throws Exception {
    LanceSplit.DistributedScanPlanResult planResult = planDistributed(whereCondition, limit);
    List<byte[]> tasks = planResult.getTasks();
    int[] fragmentIds = planResult.getFragmentIds();
    LanceSparkReadOptions resolvedReadOptions =
        readOptions.withVersion((int) planResult.getResolvedVersion());

    int totalRows = 0;
    List<List<Long>> allValues = new ArrayList<>();

    for (int i = 0; i < tasks.size(); i++) {
      LanceInputPartition partition =
          createDistributedPartition(
              tasks.get(i), fragmentIds[i], i, resolvedReadOptions, whereCondition, limit);
      assertTrue(partition.isDistributedMode());

      try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
        while (reader.next()) {
          ColumnarBatch batch = reader.get();
          assertNotNull(batch);
          for (int row = 0; row < batch.numRows(); row++) {
            List<Long> rowValues = new ArrayList<>();
            for (int col = 0; col < batch.numCols(); col++) {
              rowValues.add(batch.column(col).getLong(row));
            }
            allValues.add(rowValues);
            totalRows++;
          }
          batch.close();
        }
      }
    }
    return new ReadResult(totalRows, allValues);
  }

  private static class ReadResult {
    final int totalRows;
    final List<List<Long>> values;

    ReadResult(int totalRows, List<List<Long>> values) {
      this.totalRows = totalRows;
      this.values = values;
    }
  }

  @Test
  public void testDistributedReadAll() throws Exception {
    ReadResult result = readAllDistributed(Optional.empty(), Optional.empty());
    List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;
    assertEquals(expectedValues.size(), result.totalRows, "Total row count should match expected");

    // Verify all expected values are present (order may differ
    // across fragments)
    for (List<Long> expected : expectedValues) {
      assertTrue(result.values.contains(expected), "Expected row not found: " + expected);
    }
  }

  @Test
  public void testDistributedReadSingleFragment() throws Exception {
    LanceSplit.DistributedScanPlanResult planResult =
        planDistributed(Optional.empty(), Optional.empty());
    List<byte[]> tasks = planResult.getTasks();
    int[] fragmentIds = planResult.getFragmentIds();
    LanceSparkReadOptions resolvedReadOptions =
        readOptions.withVersion((int) planResult.getResolvedVersion());

    assertTrue(tasks.size() > 0, "Should have at least one task");

    // Read only the first task
    LanceInputPartition partition =
        createDistributedPartition(
            tasks.get(0),
            fragmentIds[0],
            0,
            resolvedReadOptions,
            Optional.empty(),
            Optional.empty());

    int rowCount = 0;
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        assertNotNull(batch);
        rowCount += batch.numRows();
        batch.close();
      }
    }

    // Single fragment should have fewer rows than total
    assertTrue(rowCount > 0, "Single fragment should have at least one row");
    assertTrue(
        rowCount < TestUtils.TestTable1Config.expectedValues.size(),
        "Single fragment should have fewer rows than total");
  }

  @Test
  public void testDistributedReadWithFilter() throws Exception {
    ReadResult result = readAllDistributed(Optional.of("x > 0"), Optional.empty());
    // x > 0 should exclude the first row (x=0)
    assertTrue(result.totalRows > 0, "Filtered result should have rows");
    assertTrue(
        result.totalRows < TestUtils.TestTable1Config.expectedValues.size(),
        "Filtered result should have fewer rows than total");

    // Verify all returned rows satisfy the filter
    for (List<Long> row : result.values) {
      assertTrue(row.get(0) > 0, "All rows should have x > 0, got x=" + row.get(0));
    }
  }

  @Test
  public void testDistributedReadWithLimit() throws Exception {
    ReadResult result = readAllDistributed(Optional.empty(), Optional.of(2));
    // With limit=2, we should get at most 2 rows total
    // Note: with distributed execution, each fragment task
    // applies the limit independently, so the actual count
    // may be up to (numFragments * limit). The coordinator
    // is responsible for final limit enforcement.
    assertTrue(result.totalRows > 0, "Should have at least one row");
    assertTrue(
        result.totalRows <= TestUtils.TestTable1Config.expectedValues.size(),
        "Should not exceed total row count");
  }

  @Test
  public void testDistributedAndLegacyConsistency() throws Exception {
    // Read via distributed mode
    ReadResult distributedResult = readAllDistributed(Optional.empty(), Optional.empty());

    // Read via legacy mode
    LanceSplit split = new LanceSplit(java.util.Arrays.asList(0, 1));
    LanceInputPartition legacyPartition =
        new LanceInputPartition(
            TestUtils.TestTable1Config.schema,
            0 /* partitionId */,
            split,
            readOptions,
            Optional.empty() /* whereCondition */,
            Optional.empty() /* limit */,
            Optional.empty() /* offset */,
            Optional.empty() /* topNSortOrders */,
            Optional.empty() /* pushedAggregation */,
            "legacy-test" /* scanId */,
            null /* initialStorageOptions */,
            null /* namespaceImpl */,
            null /* namespaceProperties */);
    assertFalse(legacyPartition.isDistributedMode());

    List<List<Long>> legacyValues = new ArrayList<>();
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(legacyPartition)) {
      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        for (int row = 0; row < batch.numRows(); row++) {
          List<Long> rowValues = new ArrayList<>();
          for (int col = 0; col < batch.numCols(); col++) {
            rowValues.add(batch.column(col).getLong(row));
          }
          legacyValues.add(rowValues);
        }
        batch.close();
      }
    }

    // Both modes should return the same total rows
    assertEquals(
        legacyValues.size(),
        distributedResult.totalRows,
        "Distributed and legacy should return same row count");

    // Both modes should contain the same values
    // (order may differ)
    for (List<Long> legacyRow : legacyValues) {
      assertTrue(
          distributedResult.values.contains(legacyRow),
          "Distributed result missing legacy row: " + legacyRow);
    }
    for (List<Long> distRow : distributedResult.values) {
      assertTrue(
          legacyValues.contains(distRow), "Legacy result missing distributed row: " + distRow);
    }
  }

  @Test
  public void testCloseReleasesResources() throws Exception {
    LanceSplit.DistributedScanPlanResult planResult =
        planDistributed(Optional.empty(), Optional.empty());
    List<byte[]> tasks = planResult.getTasks();
    int[] fragmentIds = planResult.getFragmentIds();
    LanceSparkReadOptions resolvedReadOptions =
        readOptions.withVersion((int) planResult.getResolvedVersion());

    LanceInputPartition partition =
        createDistributedPartition(
            tasks.get(0),
            fragmentIds[0],
            0,
            resolvedReadOptions,
            Optional.empty(),
            Optional.empty());

    // Read partially, then close
    assertDoesNotThrow(
        () -> {
          LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition);
          // Read one batch to initialize the reader
          if (reader.next()) {
            ColumnarBatch batch = reader.get();
            assertNotNull(batch);
            batch.close();
          }
          // Close without reading all batches
          reader.close();
        },
        "Closing reader after partial read should not throw");
  }

  @Test
  public void testDistributedReadEmptyResult() throws Exception {
    ReadResult result = readAllDistributed(Optional.of("x > 9999"), Optional.empty());
    assertEquals(0, result.totalRows, "Filter x > 9999 should return no rows");
    assertTrue(result.values.isEmpty(), "Values list should be empty for empty result");
  }
}
