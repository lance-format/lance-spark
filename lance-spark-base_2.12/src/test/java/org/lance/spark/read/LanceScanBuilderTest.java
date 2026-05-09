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

import org.lance.index.scalar.ZoneStats;
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
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class LanceScanBuilderTest {

  private static final StructType TEST_SCHEMA = TestUtils.TestTable1Config.schema;

  private LanceScanBuilder createBuilder() {
    return new LanceScanBuilder(
        TEST_SCHEMA,
        TestUtils.TestTable1Config.readOptions,
        Collections.emptyMap(),
        null,
        Collections.emptyMap(),
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

  // --- pushFilters ---

  @Test
  public void testPushFiltersAllSupported() {
    LanceScanBuilder builder = createBuilder();
    Filter[] filters =
        new Filter[] {
          new GreaterThan("x", 1L), new LessThan("y", 10L), new IsNotNull("b"),
        };
    Filter[] postScanFilters = builder.pushFilters(filters);
    assertEquals(0, postScanFilters.length);
    assertEquals(3, builder.pushedFilters().length);
  }

  @Test
  public void testPushFiltersMixedSupportedAndUnsupported() {
    LanceScanBuilder builder = createBuilder();
    // StringContains is not supported for push-down
    Filter[] filters =
        new Filter[] {
          new GreaterThan("x", 1L), new StringContains("b", "test"),
        };
    Filter[] postScanFilters = builder.pushFilters(filters);
    assertEquals(1, postScanFilters.length);
    assertInstanceOf(StringContains.class, postScanFilters[0]);
    assertEquals(1, builder.pushedFilters().length);
    assertInstanceOf(GreaterThan.class, builder.pushedFilters()[0]);
  }

  @Test
  public void testPushFiltersEmptyArray() {
    LanceScanBuilder builder = createBuilder();
    Filter[] result = builder.pushFilters(new Filter[0]);
    assertEquals(0, result.length);
    assertEquals(0, builder.pushedFilters().length);
  }

  @Test
  public void testPushFiltersDisabledByConfig() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(
            Collections.singletonMap(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS, "false"),
            TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(TEST_SCHEMA, options, Collections.emptyMap(), null, null, null);
    Filter[] filters = new Filter[] {new GreaterThan("x", 1L)};
    Filter[] result = builder.pushFilters(filters);
    assertEquals(1, result.length);
    assertEquals(0, builder.pushedFilters().length);
  }

  @Test
  public void testPushFiltersWithNestedArrayOfStruct() {
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
            Collections.emptyMap(),
            Collections.emptyMap());
    Filter[] filters = new Filter[] {new GreaterThan("id", 1L)};
    Filter[] result = builder.pushFilters(filters);
    assertEquals(0, result.length);
    assertEquals(1, builder.pushedFilters().length);
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
        new LanceScanBuilder(TEST_SCHEMA, options, Collections.emptyMap(), null, null, null);
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
    builder.pushFilters(new Filter[] {new GreaterThan("x", 0L)});
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    // With pushed filters, metadata count cannot be used; falls back to scanner-based count
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

  // --- lance.partition.columns parsing guards ---

  private LanceScanBuilder builderWithPartitionColumns(String value) {
    return new LanceScanBuilder(
        TEST_SCHEMA,
        TestUtils.TestTable1Config.readOptions,
        Collections.emptyMap(),
        null,
        Collections.emptyMap(),
        Collections.singletonMap("lance.partition.columns", value));
  }

  @Test
  public void testPartitionColumnsUnknownColumnFallsBackCleanly() {
    // Unknown column must not throw IllegalArgumentException; the builder logs a WARN and
    // falls back to a scan that reports UnknownPartitioning.
    Scan scan = builderWithPartitionColumns("nonexistent_column").build();
    assertInstanceOf(LanceScan.class, scan);
    LanceScan ls = (LanceScan) scan;
    ls.planInputPartitions();
    assertInstanceOf(
        org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning.class,
        ls.outputPartitioning());
  }

  @Test
  public void testPartitionColumnsNestedPathFallsBackCleanly() {
    // Nested field paths are not supported; builder rejects the property, scan reports Unknown.
    Scan scan = builderWithPartitionColumns("outer.inner").build();
    assertInstanceOf(LanceScan.class, scan);
    LanceScan ls = (LanceScan) scan;
    ls.planInputPartitions();
    assertInstanceOf(
        org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning.class,
        ls.outputPartitioning());
  }

  @Test
  public void testPartitionColumnsWhitespaceOnlyIsAbsent() {
    // Whitespace-only property must be treated exactly like an absent property: no WARN about
    // empty tokenization for the common "users didn't set it" path.
    Scan scan = builderWithPartitionColumns("   ").build();
    assertInstanceOf(LanceScan.class, scan);
  }

  @Test
  public void testPartitionColumnsEmptyStringIsAbsent() {
    Scan scan = builderWithPartitionColumns("").build();
    assertInstanceOf(LanceScan.class, scan);
  }

  @Test
  public void testPartitionColumnsDelimitersOnlyIsAbsent() {
    // Pure-delimiter input (",", ", , ,") must be treated as absent — no WARN about empty
    // tokenization, since the effective user intent is "no partition columns declared".
    Scan scan = builderWithPartitionColumns(",").build();
    assertInstanceOf(LanceScan.class, scan);
    scan = builderWithPartitionColumns(", , ,").build();
    assertInstanceOf(LanceScan.class, scan);
  }

  @Test
  public void testPartitionColumnsUnsupportedTypeFallsBackCleanly() {
    // A column whose Spark type is outside the whitelist (Float/Double/Decimal/complex) must
    // trigger reject-all: the scan still builds, but reports UnknownPartitioning.
    StructType schemaWithDouble =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("x", DataTypes.LongType, true),
              DataTypes.createStructField("y", DataTypes.LongType, true),
              DataTypes.createStructField("b", DataTypes.LongType, true),
              DataTypes.createStructField("c", DataTypes.LongType, true),
              DataTypes.createStructField("score", DataTypes.DoubleType, true),
            });
    LanceScanBuilder builder =
        new LanceScanBuilder(
            schemaWithDouble,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.singletonMap("lance.partition.columns", "score"));
    Scan scan = builder.build();
    assertInstanceOf(LanceScan.class, scan);
    LanceScan ls = (LanceScan) scan;
    ls.planInputPartitions();
    assertInstanceOf(
        org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning.class,
        ls.outputPartitioning());
  }

  // --- detectPartitioning: identical per-column fragment coverage ---

  @Test
  public void testDetectPartitioningRejectsMismatchedCoverage() {
    // Column "x" covers fragments {0, 1}; column "y" covers only {0}. Strict-subset coverage
    // must reject detection entirely — otherwise fragment 1 would produce a phantom null tuple
    // element for column "y" (same class of bug the per-column intersection used to allow).
    // Column names chosen from TEST_SCHEMA (x, y, b, c) so fullSchema.fieldIndex resolves.
    LanceScanBuilder builder = createBuilder();
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x", Arrays.asList(new ZoneStats(0, 0, 10, 1L, 1L, 0), new ZoneStats(1, 0, 10, 2L, 2L, 0)));
    stats.put("y", Collections.singletonList(new ZoneStats(0, 0, 10, 100L, 100L, 0)));

    ZonemapFragmentPruner.PartitionInfo info =
        builder.detectPartitioning(Arrays.asList("x", "y"), stats);
    assertNull(info, "Detection must reject when per-column fragment coverage differs");
  }

  @Test
  public void testDetectPartitioningAcceptsIdenticalCoverage() {
    LanceScanBuilder builder = createBuilder();
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x", Arrays.asList(new ZoneStats(0, 0, 10, 1L, 1L, 0), new ZoneStats(1, 0, 10, 2L, 2L, 0)));
    stats.put(
        "y",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 100L, 100L, 0), new ZoneStats(1, 0, 10, 200L, 200L, 0)));

    ZonemapFragmentPruner.PartitionInfo info =
        builder.detectPartitioning(Arrays.asList("x", "y"), stats);
    assertNotNull(info);
    assertEquals(Arrays.asList("x", "y"), info.getColumnNames());
    // Types resolved from TEST_SCHEMA (both x and y are LongType).
    assertEquals(Arrays.asList(DataTypes.LongType, DataTypes.LongType), info.getColumnTypes());
    assertEquals(2, info.size());
    // Tuples are assembled in declaration order, fragment by fragment.
    assertArrayEquals(new Object[] {1L, 100L}, info.getFragmentPartitionKeys().get(0));
    assertArrayEquals(new Object[] {2L, 200L}, info.getFragmentPartitionKeys().get(1));
  }

  // --- parsePartitionColumns: direct assertions on the token list ---

  @Test
  public void testParsePartitionColumnsDedupesTrimsAndPreservesOrder() {
    // "y, x , x, b" exercises all three behaviors together: whitespace trimming happens before
    // dedup (so " x " collapses with "x"), duplicates after the first are dropped with a WARN,
    // and the surviving tokens keep source-string order (not alphabetic).
    LanceScanBuilder builder = createBuilder();
    List<String> result = builder.parsePartitionColumns("y, x , x, b");
    assertEquals(Arrays.asList("y", "x", "b"), result);
  }
}
