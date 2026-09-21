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
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
            null /* namespaceProperties */,
            null /* partitionKeyRow */);
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
            null /* namespaceProperties */,
            null /* partitionKeyRow */);
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
            null /* namespaceProperties */,
            null /* partitionKeyRow */);
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

  @Test
  public void testCloseAfterMidIterationCreateFailure() throws Exception {
    // Fragment 0 exists; fragment 999999 does not. Iterating 0 → 999999 must fail on the second
    // fragment; close() must then be idempotent — not re-close the already-closed fragment 0
    // scanner and raise `ArrowArrayStream is already closed`.
    LanceSplit split = new LanceSplit(Arrays.asList(0, 999999));
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
            "testCloseAfterMidIterationCreateFailure" /* scanId */,
            null /* initialStorageOptions */,
            null /* namespaceImpl */,
            null /* namespaceProperties */,
            null /* partitionKeyRow */);
    LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition);
    try {
      // Drain fragment 0 first so next() advances into the create(fragment=999999) branch, which
      // must throw.
      assertThrows(
          RuntimeException.class,
          () -> {
            while (reader.next()) {
              ColumnarBatch batch = reader.get();
              assertNotNull(batch);
              batch.close();
            }
          });
      assertDoesNotThrow(reader::close, "close() must be idempotent after mid-iteration failure");
    } finally {
      // Defensive close in case an AssertionError above (from assertNotNull / assertDoesNotThrow)
      // bypassed the inline close() call — AssertionError is not caught by assertThrows.
      reader.close();
    }
  }

  @Test
  public void testCloseIsIdempotent() throws Exception {
    // Second close() must be a no-op — verifies the null-first idempotence guard.
    LanceSplit split = new LanceSplit(Collections.singletonList(0));
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
            "testCloseIsIdempotent" /* scanId */,
            null /* initialStorageOptions */,
            null /* namespaceImpl */,
            null /* namespaceProperties */,
            null /* partitionKeyRow */);
    LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition);
    try {
      while (reader.next()) {
        reader.get().close();
      }
      assertDoesNotThrow(reader::close, "first close() after full iteration must succeed");
      assertDoesNotThrow(reader::close, "second close() must be a no-op");
    } finally {
      reader.close();
    }
  }

  @Test
  public void testPartitionReaderSkipsNamespaceWhenExecutorCredentialRefreshDisabled()
      throws Exception {
    RecordingNamespace.reset();
    LanceInputPartition partition =
        namespacePartition(
            TestUtils.TestTable1Config.datasetUri,
            Collections.singletonList(0),
            "testPartitionReaderSkipsNamespaceWhenExecutorCredentialRefreshDisabled",
            false);
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
      while (reader.next()) {
        reader.get().close();
      }
    }

    assertNull(
        partition.getReadOptions().getNamespace(),
        "executor_credential_refresh=false must skip namespace rebuild on the executor");
    assertEquals(
        0,
        RecordingNamespace.INITIALIZE_CALLS.get(),
        "executor_credential_refresh=false must not load or initialize the namespace impl");
  }

  @Test
  public void testPartitionReaderReusesAndClosesExecutorNamespace() throws Exception {
    RecordingNamespace.reset();
    LanceInputPartition partition =
        namespacePartition(
            TestUtils.TestTable1Config.datasetUri,
            Arrays.asList(0, 1),
            "testPartitionReaderReusesAndClosesExecutorNamespace",
            true);

    LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition);
    int rowsRead = 0;
    try {
      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        assertNotNull(batch);
        rowsRead += batch.numRows();
        batch.close();
      }
    } finally {
      reader.close();
    }
    reader.close();

    assertEquals(TestUtils.TestTable1Config.expectedValues.size(), rowsRead);
    assertEquals(
        1,
        RecordingNamespace.INITIALIZE_CALLS.get(),
        "all fragments in one Spark task must reuse one namespace client");
    assertEquals(
        1,
        RecordingNamespace.CLOSE_CALLS.get(),
        "the task-owned namespace client must be closed exactly once");
  }

  @Test
  public void testPartitionReaderClosesExecutorNamespaceWhenFragmentOpenFails() throws Exception {
    RecordingNamespace.reset();
    LanceInputPartition partition =
        namespacePartition(
            TestUtils.TestTable1Config.datasetUri,
            Collections.singletonList(999999),
            "testPartitionReaderClosesExecutorNamespaceWhenFragmentOpenFails",
            true);
    LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition);

    RuntimeException failure = assertThrows(RuntimeException.class, reader::next);

    assertNotNull(failure.getCause());
    assertEquals(1, RecordingNamespace.INITIALIZE_CALLS.get());
    assertEquals(
        1,
        RecordingNamespace.CLOSE_CALLS.get(),
        "namespace initialization must be rolled back when fragment open fails");
    reader.close();
    assertEquals(
        1,
        RecordingNamespace.CLOSE_CALLS.get(),
        "close after failed initialization cleanup must be idempotent");
  }

  private LanceInputPartition namespacePartition(
      String location, List<Integer> fragments, String scanId, boolean executorCredentialRefresh) {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(location)
            .tableId(Collections.singletonList(TestUtils.TestTable1Config.datasetName))
            .executorCredentialRefresh(executorCredentialRefresh)
            .build();
    return new LanceInputPartition(
        TestUtils.TestTable1Config.schema,
        0,
        new LanceSplit(fragments),
        readOptions,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        scanId,
        Collections.emptyMap(),
        RecordingNamespace.class.getName(),
        Collections.singletonMap("location", location),
        null);
  }

  /**
   * Public, top-level-by-FQCN, no-arg {@link LanceNamespace} so that {@link
   * LanceNamespace#connect(String, Map, BufferAllocator)} can resolve it via {@code Class.forName}
   * during executor-side namespace initialization.
   */
  public static class RecordingNamespace implements LanceNamespace, AutoCloseable {
    static final AtomicInteger INITIALIZE_CALLS = new AtomicInteger();
    static final AtomicInteger CLOSE_CALLS = new AtomicInteger();

    private String location;

    public RecordingNamespace() {}

    static void reset() {
      INITIALIZE_CALLS.set(0);
      CLOSE_CALLS.set(0);
    }

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
      INITIALIZE_CALLS.incrementAndGet();
      location = properties.get("location");
    }

    @Override
    public String namespaceId() {
      return "recording";
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      return new DescribeTableResponse().location(location);
    }

    @Override
    public void close() {
      CLOSE_CALLS.incrementAndGet();
    }
  }
}
