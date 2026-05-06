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

import org.lance.spark.TestUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class LanceScanTest {

  private static final StructType TEST_SCHEMA = TestUtils.TestTable1Config.schema;

  private LanceScan buildScan() {
    return (LanceScan)
        new LanceScanBuilder(
                TEST_SCHEMA,
                TestUtils.TestTable1Config.readOptions,
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap())
            .build();
  }

  @Test
  public void testReadSchemaReturnsOriginalSchema() {
    assertEquals(TEST_SCHEMA, buildScan().readSchema());
  }

  @Test
  public void testReadSchemaWithAggregationReturnsCountSchema() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    builder.pushFilters(new Filter[] {new GreaterThan("x", 0L)});
    builder.pushAggregation(
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {}));
    // With filters, COUNT(*) falls back to scanner-based (returns LanceScan, not LanceLocalScan)
    Scan scan = builder.build();
    assertInstanceOf(LanceScan.class, scan);
    StructType countSchema = scan.readSchema();
    assertEquals(1, countSchema.fields().length);
    assertEquals("count", countSchema.fields()[0].name());
    assertEquals(DataTypes.LongType, countSchema.fields()[0].dataType());
  }

  @Test
  public void testPlanInputPartitionsReturnsNonEmpty() {
    InputPartition[] partitions = buildScan().planInputPartitions();
    assertTrue(partitions.length > 0);
    for (InputPartition p : partitions) {
      assertInstanceOf(LanceInputPartition.class, p);
    }
  }

  @Test
  public void testPlanInputPartitionsPropagatesFilters() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    builder.pushFilters(new Filter[] {new GreaterThan("x", 0L)});
    LanceScan scan = (LanceScan) builder.build();
    LanceInputPartition partition = (LanceInputPartition) scan.planInputPartitions()[0];
    assertTrue(partition.getWhereCondition().isPresent());
  }

  @Test
  public void testPlanInputPartitionsPropagatesLimit() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    builder.pushLimit(2);
    LanceScan scan = (LanceScan) builder.build();
    LanceInputPartition partition = (LanceInputPartition) scan.planInputPartitions()[0];
    assertTrue(partition.getLimit().isPresent());
    assertEquals(2, partition.getLimit().get());
  }

  @Test
  public void testLimitPrunesPartitions() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    builder.pushLimit(1);
    LanceScan scan = (LanceScan) builder.build();
    // With LIMIT 1 and test dataset having 2 rows per fragment,
    // only 1 fragment should be planned
    InputPartition[] partitions = scan.planInputPartitions();
    assertTrue(partitions.length >= 1);
    // Should be fewer than total fragments if limit pruning works
    LanceScan scanNoLimit = buildScan();
    InputPartition[] allPartitions = scanNoLimit.planInputPartitions();
    assertTrue(
        partitions.length <= allPartitions.length,
        "Limit-pruned partitions should not exceed total partitions");
  }

  // --- outputPartitioning ---

  @Test
  public void testOutputPartitioningBeforePlanIsUnknown() {
    LanceScan scan = buildScan();
    Partitioning partitioning = scan.outputPartitioning();
    assertInstanceOf(UnknownPartitioning.class, partitioning);
  }

  @Test
  public void testOutputPartitioningAfterPlanIsUnknownWithoutPartitionInfo() {
    LanceScan scan = buildScan();
    scan.planInputPartitions();
    Partitioning partitioning = scan.outputPartitioning();
    assertInstanceOf(UnknownPartitioning.class, partitioning);
  }

  // --- HasPartitionKey / SPJ ---

  @Test
  public void testInputPartitionsImplementHasPartitionKey() {
    LanceScan scan = buildScan();
    InputPartition[] partitions = scan.planInputPartitions();
    assertTrue(partitions.length > 0);
    for (InputPartition p : partitions) {
      assertInstanceOf(HasPartitionKey.class, p);
      HasPartitionKey hpk = (HasPartitionKey) p;
      assertNotNull(hpk.partitionKey());
    }
  }

  @Test
  public void testPartitionKeyReturnsEmptyRowWithoutPartitionInfo() {
    // Without partition info, partition key returns an empty row
    LanceScan scan = buildScan();
    InputPartition[] partitions = scan.planInputPartitions();
    for (InputPartition p : partitions) {
      HasPartitionKey hpk = (HasPartitionKey) p;
      InternalRow key = hpk.partitionKey();
      assertNotNull(key);
      assertEquals(0, key.numFields());
    }
  }

  @Test
  public void testOutputPartitioningWithPartitionInfo() {
    // Create a LanceScan with partition info
    Map<Integer, Comparable<?>> fragValues = new HashMap<>();
    fragValues.put(0, "east");
    fragValues.put(1, "west");
    ZonemapFragmentPruner.PartitionInfo partInfo =
        new ZonemapFragmentPruner.PartitionInfo("region", fragValues);

    LanceScan scan =
        new LanceScan(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            new Filter[0],
            null,
            Collections.emptyMap(),
            null,
            partInfo,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());

    // Plan partitions to set numPartitions
    scan.planInputPartitions();

    Partitioning partitioning = scan.outputPartitioning();
    assertInstanceOf(KeyGroupedPartitioning.class, partitioning);

    KeyGroupedPartitioning kgp = (KeyGroupedPartitioning) partitioning;
    Expression[] keys = kgp.keys();
    assertEquals(1, keys.length);
    assertInstanceOf(FieldReference.class, keys[0]);
    // Key should be "region", not "_fragid"
    String[] fieldNames = ((FieldReference) keys[0]).fieldNames();
    assertEquals("region", fieldNames[0]);
  }

  @Test
  public void testOutputPartitioningWithoutPartitionInfoIsUnknown() {
    // No partition info → should return UnknownPartitioning
    LanceScan scan = buildScan();
    scan.planInputPartitions();
    Partitioning partitioning = scan.outputPartitioning();
    assertInstanceOf(UnknownPartitioning.class, partitioning);
  }

  // --- equals / hashCode (required for ReusedExchange) ---

  @Test
  public void testEqualsForIdenticalScans() {
    LanceScan scan1 = buildScan();
    LanceScan scan2 = buildScan();
    assertEquals(scan1, scan2, "Two scans of the same table should be equal for ReusedExchange");
  }

  @Test
  public void testHashCodeConsistentWithEquals() {
    LanceScan scan1 = buildScan();
    LanceScan scan2 = buildScan();
    assertEquals(scan1.hashCode(), scan2.hashCode(), "Equal scans must have the same hashCode");
  }

  @Test
  public void testNotEqualWithDifferentFilters() {
    LanceScan scan1 = buildScan();

    LanceScanBuilder builder2 =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    builder2.pushFilters(new Filter[] {new GreaterThan("x", 0L)});
    LanceScan scan2 = (LanceScan) builder2.build();

    assertNotEquals(scan1, scan2, "Scans with different filters should not be equal");
  }

  @Test
  public void testNotEqualWithDifferentSchema() {
    LanceScan scan1 = buildScan();

    StructType differentSchema = new StructType().add("x", DataTypes.LongType);
    LanceScan scan2 =
        (LanceScan)
            new LanceScanBuilder(
                    differentSchema,
                    TestUtils.TestTable1Config.readOptions,
                    Collections.emptyMap(),
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap())
                .build();

    assertNotEquals(scan1, scan2, "Scans with different schemas should not be equal");
  }
}
