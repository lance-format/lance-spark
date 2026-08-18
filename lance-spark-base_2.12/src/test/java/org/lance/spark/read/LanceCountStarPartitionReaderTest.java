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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceCountStarPartitionReaderTest {

  @Test
  public void testFilteredCountsReuseExecutorNamespaceMetadata() throws Exception {
    RecordingNamespace.initializeCalls.set(0);
    RecordingNamespace.describeCalls.set(0);
    RecordingNamespace.location = TestUtils.TestTable1Config.datasetUri;
    String scanId = "count-star-namespace-" + UUID.randomUUID();

    readCount(createNamespacePartition(scanId, 0));
    readCount(createNamespacePartition(scanId, 1));

    assertEquals(1, RecordingNamespace.initializeCalls.get());
    assertEquals(1, RecordingNamespace.describeCalls.get());
  }

  @Test
  public void testCloseReleasesArrowMemory() throws Exception {
    // Build a partition with a CountStar aggregation and a filter condition
    // so it exercises LanceCountStarPartitionReader (not the LocalScan path)
    Aggregation countStarAgg =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    LanceInputPartition partition =
        new LanceInputPartition(
            TestUtils.TestTable1Config.schema,
            0,
            new LanceSplit(Arrays.asList(0, 1)),
            TestUtils.TestTable1Config.readOptions,
            Optional.of("x > 0"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(countStarAgg),
            "testCountStarMemoryLeak",
            null,
            null,
            null,
            null);

    LanceCountStarPartitionReader reader = new LanceCountStarPartitionReader(partition);

    // Exercise the reader lifecycle: next() -> get() -> close()
    assertTrue(reader.next(), "First next() should return true");

    ColumnarBatch batch = reader.get();
    long countResult = batch.column(0).getLong(0);
    assertEquals(3L, countResult, "Filtered count (x > 0) should return 3 rows");

    // After get(), Arrow buffers for the result batch are allocated.
    // Record the allocator memory before calling close().
    long memBeforeClose = LanceRuntime.allocator().getAllocatedMemory();
    assertTrue(memBeforeClose > 0, "Arrow buffers should be allocated after get()");

    // close() should release the Arrow buffers held by the result batch.
    // Before the fix, currentBatch was never assigned in get(), so close() was
    // a no-op and these buffers leaked.
    reader.close();

    long memAfterClose = LanceRuntime.allocator().getAllocatedMemory();
    assertTrue(
        memAfterClose < memBeforeClose,
        "close() should release Arrow memory allocated by get(). "
            + "Memory before close: "
            + memBeforeClose
            + ", after close: "
            + memAfterClose);
  }

  private static void readCount(LanceInputPartition partition) throws Exception {
    try (LanceCountStarPartitionReader reader = new LanceCountStarPartitionReader(partition)) {
      assertTrue(reader.next());
      reader.get();
    }
  }

  private static LanceInputPartition createNamespacePartition(String scanId, int fragmentId) {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(RecordingNamespace.location)
            .tableId(Collections.singletonList("test_dataset1"))
            .version(6L)
            .build();
    Aggregation countStarAgg =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    return new LanceInputPartition(
        TestUtils.TestTable1Config.schema,
        0,
        new LanceSplit(Collections.singletonList(fragmentId)),
        readOptions,
        Optional.of("x > 0"),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(countStarAgg),
        scanId,
        Collections.emptyMap(),
        RecordingNamespace.class.getName(),
        Collections.emptyMap(),
        null);
  }

  public static class RecordingNamespace implements LanceNamespace {
    static final AtomicInteger initializeCalls = new AtomicInteger();
    static final AtomicInteger describeCalls = new AtomicInteger();
    static volatile String location;

    public RecordingNamespace() {}

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
      initializeCalls.incrementAndGet();
    }

    @Override
    public String namespaceId() {
      return "count-star-recording";
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      describeCalls.incrementAndGet();
      return new DescribeTableResponse().location(location);
    }
  }
}
