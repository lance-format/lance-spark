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

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Sum;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class LanceScanBuilderTest {

  private static final StructType TEST_SCHEMA = TestUtils.TestTable1Config.schema;

  private LanceScanBuilder createBuilder() {
    return new LanceScanBuilder(
        TEST_SCHEMA,
        TestUtils.TestTable1Config.readOptions,
        Collections.emptyMap(),
        null,
        Collections.emptyMap());
  }

  // --- pruneColumns ---

  @Test
  public void testPruneColumnsUpdatesSchema() {
    LanceScanBuilder builder = createBuilder();
    StructType requiredSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("x", DataTypes.LongType, true),
            });
    builder.pruneColumns(requiredSchema);
    Scan scan = builder.build();
    assertEquals(requiredSchema, scan.readSchema());
  }

  @Test
  public void testPruneColumnsToEmptySchema() {
    LanceScanBuilder builder = createBuilder();
    StructType emptySchema = new StructType();
    builder.pruneColumns(emptySchema);
    Scan scan = builder.build();
    assertEquals(emptySchema, scan.readSchema());
  }

  // --- pushPredicates ---

  @Test
  public void testPushPredicatesAllSupported() {
    LanceScanBuilder builder = createBuilder();
    Predicate[] predicates = {
      TestPredicates.gt("x", 1L), TestPredicates.lt("y", 10L), TestPredicates.isNotNull("b"),
    };
    Predicate[] postScan = builder.pushPredicates(predicates);
    assertEquals(0, postScan.length);
    assertEquals(3, builder.pushedPredicates().length);
  }

  @Test
  public void testPushPredicatesMixedSupportedAndUnsupported() {
    LanceScanBuilder builder = createBuilder();
    // CONTAINS is not supported for push-down
    Predicate[] predicates = {TestPredicates.gt("x", 1L), TestPredicates.contains("b", "test")};
    Predicate[] postScan = builder.pushPredicates(predicates);
    assertEquals(1, postScan.length);
    assertEquals("CONTAINS", postScan[0].name());
    assertEquals(1, builder.pushedPredicates().length);
    assertEquals(">", builder.pushedPredicates()[0].name());
  }

  @Test
  public void testPushPredicatesEmptyArray() {
    LanceScanBuilder builder = createBuilder();
    Predicate[] result = builder.pushPredicates(new Predicate[0]);
    assertEquals(0, result.length);
    assertEquals(0, builder.pushedPredicates().length);
  }

  @Test
  public void testPushPredicatesDisabledByConfig() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(
            Collections.singletonMap(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS, "false"),
            TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(TEST_SCHEMA, options, Collections.emptyMap(), null, null);
    Predicate[] predicates = {TestPredicates.gt("x", 1L)};
    Predicate[] result = builder.pushPredicates(predicates);
    assertEquals(1, result.length);
    assertEquals(0, builder.pushedPredicates().length);
  }

  @Test
  public void testPushPredicatesWithNestedArrayOfStruct() {
    // Filters on non-Array<Struct> columns should be pushed down normally.
    StructType nestedSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField(
                  "items",
                  new ArrayType(
                      new StructType(
                          new StructField[] {
                            DataTypes.createStructField("name", DataTypes.StringType, true)
                          }),
                      true),
                  true),
            });
    LanceScanBuilder builder =
        new LanceScanBuilder(
            nestedSchema,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    Predicate[] predicates = {TestPredicates.gt("id", 1L)};
    Predicate[] result = builder.pushPredicates(predicates);
    assertEquals(0, result.length);
    assertEquals(1, builder.pushedPredicates().length);
  }

  // --- pushLimit ---

  @Test
  public void testPushLimitAlwaysSucceeds() {
    LanceScanBuilder builder = createBuilder();
    assertTrue(builder.pushLimit(100));
  }

  // --- pushOffset ---

  @Test
  public void testPushOffsetRejectsMultiFragmentDataset() {
    // TestTable1 has 2 fragments, so offset cannot be pushed
    LanceScanBuilder builder = createBuilder();
    assertFalse(builder.pushOffset(10));
  }

  @Test
  public void testIsPartiallyPushedAlwaysTrue() {
    LanceScanBuilder builder = createBuilder();
    assertTrue(builder.isPartiallyPushed());
  }

  // --- pushTopN ---

  @Test
  public void testPushTopNEnabledByDefault() {
    LanceScanBuilder builder = createBuilder();
    SortOrder order = new TestSortOrder("x", SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    assertTrue(builder.pushTopN(new SortOrder[] {order}, 10));
  }

  @Test
  public void testPushTopNDisabledByConfig() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(
            Collections.singletonMap(LanceSparkReadOptions.CONFIG_TOP_N_PUSH_DOWN, "false"),
            TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(TEST_SCHEMA, options, Collections.emptyMap(), null, null);
    SortOrder order = new TestSortOrder("x", SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    assertFalse(builder.pushTopN(new SortOrder[] {order}, 10));
  }

  @Test
  public void testPushTopNRejectsNonFieldReferenceExpression() {
    LanceScanBuilder builder = createBuilder();
    // A SortOrder whose expression is not a FieldReference should be rejected
    SortOrder nonFieldOrder =
        new SortOrder() {
          @Override
          public Expression expression() {
            return new Expression() {
              @Override
              public Expression[] children() {
                return new Expression[0];
              }

              @Override
              public String toString() {
                return "custom_expression";
              }
            };
          }

          @Override
          public SortDirection direction() {
            return SortDirection.ASCENDING;
          }

          @Override
          public NullOrdering nullOrdering() {
            return NullOrdering.NULLS_FIRST;
          }
        };
    assertFalse(builder.pushTopN(new SortOrder[] {nonFieldOrder}, 10));
  }

  // --- pushAggregation ---

  @Test
  public void testPushAggregationCountStarFromMetadata() {
    LanceScanBuilder builder = createBuilder();
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    assertTrue(builder.pushAggregation(countStar));
  }

  @Test
  public void testPushAggregationCountStarWithFiltersFallsBackToScanner() {
    LanceScanBuilder builder = createBuilder();
    builder.pushPredicates(new Predicate[] {TestPredicates.gt("x", 0L)});
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    // With pushed predicates, metadata count cannot be used; falls back to scanner-based count
    assertTrue(builder.pushAggregation(countStar));
  }

  @Test
  public void testPushAggregationRejectsGroupBy() {
    LanceScanBuilder builder = createBuilder();
    Aggregation groupedAgg =
        new Aggregation(
            new AggregateFunc[] {new CountStar()}, new Expression[] {FieldReference.apply("x")});
    assertFalse(builder.pushAggregation(groupedAgg));
  }

  @Test
  public void testPushAggregationRejectsNonCountStar() {
    LanceScanBuilder builder = createBuilder();
    Aggregation sumAgg =
        new Aggregation(
            new AggregateFunc[] {new Sum(FieldReference.apply("x"), false)}, new Expression[] {});
    assertFalse(builder.pushAggregation(sumAgg));
  }

  // --- build ---

  @Test
  public void testBuildReturnsLanceScan() {
    LanceScanBuilder builder = createBuilder();
    Scan scan = builder.build();
    assertNotNull(scan);
    assertInstanceOf(LanceScan.class, scan);
    assertEquals(TEST_SCHEMA, scan.readSchema());
  }

  @Test
  public void testBuildWithCountStarReturnsLocalScan() {
    LanceScanBuilder builder = createBuilder();
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    builder.pushAggregation(countStar);
    Scan scan = builder.build();
    // Metadata-based COUNT(*) without filters returns LanceLocalScan
    assertNotNull(scan);
    assertInstanceOf(LanceLocalScan.class, scan);
  }

  /** Minimal SortOrder implementation for testing pushTopN. */
  private static class TestSortOrder implements SortOrder {
    private final String columnName;
    private final SortDirection direction;
    private final NullOrdering nullOrdering;

    TestSortOrder(String columnName, SortDirection direction, NullOrdering nullOrdering) {
      this.columnName = columnName;
      this.direction = direction;
      this.nullOrdering = nullOrdering;
    }

    @Override
    public Expression expression() {
      return FieldReference.apply(columnName);
    }

    @Override
    public SortDirection direction() {
      return direction;
    }

    @Override
    public NullOrdering nullOrdering() {
      return nullOrdering;
    }
  }
}
