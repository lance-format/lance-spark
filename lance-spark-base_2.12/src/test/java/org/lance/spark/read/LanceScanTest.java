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

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

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
            Collections.emptyMap());
    builder.pushLimit(2);
    LanceScan scan = (LanceScan) builder.build();
    LanceInputPartition partition = (LanceInputPartition) scan.planInputPartitions()[0];
    assertTrue(partition.getLimit().isPresent());
    assertEquals(2, partition.getLimit().get());
  }
}
