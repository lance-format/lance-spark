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

import org.lance.ipc.FullTextQuery;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.search.LanceSearchInputPartition;
import org.lance.spark.search.LanceSearchScan;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.catalyst.expressions.MetadataAttribute;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Sum;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
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
  public void testScanOptionsDoNotRestoreStaleMixedCaseReference() {
    for (String key : Arrays.asList("TaG", "BrAnCh", "VeRsIoN")) {
      LanceSparkReadOptions options =
          LanceSparkReadOptions.from(
                  Collections.singletonMap(key, "999"), TestUtils.TestTable1Config.datasetUri)
              .withRef(LanceRef.ofMain(1));
      LanceDataset table =
          new LanceDataset(
              options,
              TEST_SCHEMA,
              Collections.emptyMap(),
              null,
              Collections.emptyMap(),
              false,
              null);
      LanceScan scan =
          (LanceScan)
              table
                  .newScanBuilder(
                      new CaseInsensitiveStringMap(Collections.singletonMap("batch_size", "128")))
                  .build();
      LanceInputPartition partition = (LanceInputPartition) scan.planInputPartitions()[0];
      assertEquals(LanceRef.ofMain(1), partition.getReadOptions().getRef());
      assertEquals(128, partition.getReadOptions().getBatchSize());
    }
  }

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

  private LanceScanBuilder createSnapshotBuilder(StructType schema) {
    return new LanceScanBuilder(
        schema,
        TestUtils.TestTable1Config.readOptions.withRef(LanceRef.ofMain(1)),
        Collections.emptyMap(),
        null,
        Collections.emptyMap());
  }

  @Test
  public void testPruneSnapshotColumnsRejectsMissingFields() {
    LanceScanBuilder builder =
        createSnapshotBuilder(
            new StructType().add("payload", new StructType().add("value", DataTypes.IntegerType)));
    for (StructType required :
        Arrays.asList(
            new StructType().add("extra", DataTypes.IntegerType),
            new StructType()
                .add("payload", new StructType().add("extra", DataTypes.IntegerType)))) {
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> builder.pruneColumns(required));
      assertTrue(error.getMessage().contains("extra"));
      assertTrue(error.getMessage().contains("snapshot"));
    }
  }

  @Test
  public void testPruneSnapshotColumnsRejectsChangedTypes() {
    StructType oldStruct = new StructType().add("value", DataTypes.IntegerType);
    StructType newStruct = new StructType().add("value", DataTypes.LongType);
    DataType[] oldTypes = {
      DataTypes.IntegerType,
      oldStruct,
      DataTypes.createArrayType(oldStruct),
      DataTypes.createMapType(DataTypes.StringType, oldStruct),
      DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)
    };
    DataType[] newTypes = {
      DataTypes.LongType,
      newStruct,
      DataTypes.createArrayType(newStruct),
      DataTypes.createMapType(DataTypes.StringType, newStruct),
      DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType)
    };
    for (int i = 0; i < oldTypes.length; i++) {
      LanceScanBuilder builder =
          createSnapshotBuilder(new StructType().add("payload", oldTypes[i]));
      StructType required = new StructType().add("payload", newTypes[i]);
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> builder.pruneColumns(required));
      assertTrue(error.getMessage().contains("payload"));
      assertTrue(error.getMessage().contains("snapshot"));
    }
  }

  @Test
  public void testPruneSnapshotColumnsAllowsCompatibleProjections() {
    StructType oldStruct =
        new StructType().add("value", DataTypes.IntegerType).add("label", DataTypes.StringType);
    StructType projected = new StructType().add("label", DataTypes.StringType, false);
    DataType[] oldTypes = {
      oldStruct,
      DataTypes.createArrayType(oldStruct),
      DataTypes.createMapType(DataTypes.StringType, oldStruct)
    };
    DataType[] projectedTypes = {
      projected,
      DataTypes.createArrayType(projected, false),
      DataTypes.createMapType(DataTypes.StringType, projected, false)
    };
    for (int i = 0; i < oldTypes.length; i++) {
      LanceScanBuilder builder =
          createSnapshotBuilder(
              new StructType().add("id", DataTypes.IntegerType).add("payload", oldTypes[i]));
      StructType required = new StructType().add("payload", projectedTypes[i], false);
      assertDoesNotThrow(() -> builder.pruneColumns(required));
      assertDoesNotThrow(() -> builder.pruneColumns(new StructType()));
    }
  }

  @Test
  public void testPruneSnapshotColumnsAllowsVirtualMetadata() {
    StructField[] fields =
        Arrays.stream(LanceDataset.METADATA_COLUMNS)
            .map(
                column ->
                    DataTypes.createStructField(
                        column.name(),
                        column.dataType(),
                        column.isNullable(),
                        MetadataAttribute.apply(
                                column.name(), column.dataType(), column.isNullable())
                            .metadata()))
            .toArray(StructField[]::new);
    LanceScanBuilder builder = createSnapshotBuilder(TEST_SCHEMA);
    assertDoesNotThrow(() -> builder.pruneColumns(new StructType(fields)));
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
  public void testScoreFilterWithoutFtsStaysResidual() {
    StructType schema =
        new StructType()
            .add(
                LanceConstant.SCORE,
                DataTypes.FloatType,
                true,
                MetadataAttribute.apply(LanceConstant.SCORE, DataTypes.FloatType, true).metadata());
    LanceScanBuilder builder =
        new LanceScanBuilder(
            schema,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    Predicate[] predicates = {TestPredicates.gt(LanceConstant.SCORE, 0.0f)};
    assertArrayEquals(predicates, builder.pushPredicates(predicates));
    assertEquals(0, builder.pushedPredicates().length);
    assertFalse(builder.pushLimit(1));
    assertFalse(builder.pushOffset(1));
    assertFalse(
        builder.pushTopN(
            new SortOrder[] {
              new TestSortOrder(
                  LanceConstant.SCORE, SortDirection.DESCENDING, NullOrdering.NULLS_LAST)
            },
            1));
    assertFalse(
        builder.pushAggregation(
            new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[0])));
    assertTrue(
        assertThrows(IllegalArgumentException.class, builder::build)
            .getMessage()
            .contains("full-text search predicate"));
  }

  @Test
  public void testStoredScoreFilterWithoutFtsIsPushed() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            new StructType().add(LanceConstant.SCORE, DataTypes.FloatType),
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    Predicate[] predicates = {TestPredicates.gt(LanceConstant.SCORE, 0.0f)};
    assertEquals(0, builder.pushPredicates(predicates).length);
    assertArrayEquals(predicates, builder.pushedPredicates());
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
  public void testBlobV2FilterStaysResidualWhileOtherColumnsPushDown() {
    Metadata blobV2Metadata =
        new MetadataBuilder()
            .putString(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2)
            .build();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              new StructField("payload", DataTypes.BinaryType, true, blobV2Metadata)
            });
    LanceScanBuilder builder =
        new LanceScanBuilder(
            schema,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());

    assertFalse(
        builder.pushAggregation(
            new Aggregation(
                new AggregateFunc[] {new Count(FieldReference.apply("payload"), false)},
                new Expression[0])));
    Predicate onId = TestPredicates.gt("id", 1L);
    Predicate onBlob = TestPredicates.gt("payload", 1L);
    Predicate[] postScan = builder.pushPredicates(new Predicate[] {onId, onBlob});

    // Blob v2 filters stay in Spark. Normal-column filters still push to Lance.
    assertArrayEquals(new Predicate[] {onBlob}, postScan);
    assertArrayEquals(new Predicate[] {onId}, builder.pushedPredicates());
    assertFalse(builder.pushLimit(10));
  }

  @Test
  public void testResidualPredicateBlocksLimitPushdownWithoutBlobColumn() {
    LanceScanBuilder builder = createBuilder();
    // Unsupported CONTAINS stays in Spark even on a table with no blob columns.
    Predicate[] postScan =
        builder.pushPredicates(new Predicate[] {TestPredicates.contains("b", "x")});
    assertEquals(1, postScan.length);
    assertFalse(builder.pushLimit(10));
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
  public void testPushLimitWhenNoResidualPredicates() {
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
  public void testPushTopNForScoreOnlySupportsNamespaceDescendingOrder() {
    int k = 10;
    SortOrder scoreDescending =
        new TestSortOrder(LanceConstant.SCORE, SortDirection.DESCENDING, NullOrdering.NULLS_LAST);
    LanceScanBuilder namespaceBuilder = createFtsScoreBuilder("dir");
    assertTrue(namespaceBuilder.pushTopN(new SortOrder[] {scoreDescending}, k));
    assertEquals(k, getNamespaceFtsTopK(namespaceBuilder));

    SortOrder scoreAscending =
        new TestSortOrder(LanceConstant.SCORE, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    LanceScanBuilder ascendingBuilder = createFtsScoreBuilder("dir");
    assertFalse(ascendingBuilder.pushTopN(new SortOrder[] {scoreAscending}, k));
    assertEquals(Integer.MAX_VALUE, getNamespaceFtsTopK(ascendingBuilder));

    SortOrder otherColumnAscending =
        new TestSortOrder("x", SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    LanceScanBuilder otherColumnBuilder = createFtsScoreBuilder("dir");
    assertFalse(otherColumnBuilder.pushTopN(new SortOrder[] {otherColumnAscending}, k));
    assertEquals(Integer.MAX_VALUE, getNamespaceFtsTopK(otherColumnBuilder));
    LanceScanBuilder multiColumnBuilder = createFtsScoreBuilder("dir");
    assertFalse(
        multiColumnBuilder.pushTopN(new SortOrder[] {scoreDescending, otherColumnAscending}, k));
    assertEquals(Integer.MAX_VALUE, getNamespaceFtsTopK(multiColumnBuilder));

    LanceScanBuilder localBuilder = createFtsScoreBuilder(null);
    assertFalse(localBuilder.pushTopN(new SortOrder[] {scoreDescending}, k));
  }

  private LanceScanBuilder createFtsScoreBuilder(String namespaceImpl) {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .tableId(Collections.singletonList("default"))
            .fullTextQuery(FullTextQuery.match("hello", "body"))
            .build();
    StructType scoreSchema =
        new StructType()
            .add(
                LanceConstant.SCORE,
                DataTypes.FloatType,
                true,
                MetadataAttribute.apply(LanceConstant.SCORE, DataTypes.FloatType, true).metadata());
    return new LanceScanBuilder(
        scoreSchema, options, Collections.emptyMap(), namespaceImpl, Collections.emptyMap());
  }

  private int getNamespaceFtsTopK(LanceScanBuilder builder) {
    LanceSearchScan scan = (LanceSearchScan) builder.build();
    LanceSearchInputPartition partition = (LanceSearchInputPartition) scan.planInputPartitions()[0];
    return partition.getQuery().toQueryTableRequest().getK();
  }

  @Test
  public void testFtsRejectsStoredScore() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .fullTextQuery(FullTextQuery.match("hello", "body"))
            .build();
    UnsupportedOperationException failure =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                new LanceScanBuilder(
                    new StructType().add(LanceConstant.SCORE, DataTypes.FloatType),
                    options,
                    Collections.emptyMap(),
                    "dir",
                    Collections.emptyMap()));
    assertTrue(failure.getMessage().contains("stored '_score' column"));
    assertTrue(failure.getMessage().contains("Rename the stored column"));
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
  public void testExactLookupCountFilterDropsRedundantNotNull() {
    Predicate[] equality =
        new Predicate[] {TestPredicates.isNotNull("category"), TestPredicates.eq("category", 5)};
    Optional<String> equalityFilter = LanceScanBuilder.compileExactLookupCountFilter(equality);
    assertEquals(Optional.of("(category == 5)"), equalityFilter);

    Predicate[] inList =
        new Predicate[] {TestPredicates.isNotNull("category"), TestPredicates.in("category", 1, 2)};
    Optional<String> inFilter = LanceScanBuilder.compileExactLookupCountFilter(inList);
    assertEquals(Optional.of("(category IN (1,2))"), inFilter);
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
  public void testPushAggregationCountColumnUsesCountOnlyReader() throws Exception {
    LanceScanBuilder builder = createBuilder();
    Predicate filter = TestPredicates.gt("x", 0L);
    builder.pushPredicates(new Predicate[] {filter});
    assertTrue(
        builder.pushAggregation(
            new Aggregation(
                new AggregateFunc[] {new Count(FieldReference.apply("b"), false)},
                new Expression[0])));
    assertArrayEquals(new Predicate[] {filter}, builder.pushedPredicates());
    LanceScan scan = (LanceScan) builder.build();
    assertEquals(new StructType().add("count", DataTypes.LongType), scan.readSchema());
    LanceInputPartition partition = (LanceInputPartition) scan.planInputPartitions()[0];
    assertEquals(Optional.of("(x > 0) AND (`b` IS NOT NULL)"), partition.getWhereCondition());
    try (PartitionReader<?> reader = scan.createReaderFactory().createColumnarReader(partition)) {
      assertInstanceOf(LanceCountStarPartitionReader.class, reader);
    }
  }

  @Test
  public void testPushAggregationRejectsUnsupportedColumnCountsWithoutChangingFilters() {
    AggregateFunc[][] unsupported = {
      {new Count(FieldReference.apply("b"), true)},
      {new Count(TestPredicates.literalOf(1), false)},
      {new Count(FieldReference.apply("nested.value"), false)},
      {new Count(FieldReference.apply("missing"), false)},
      {new Count(FieldReference.apply(LanceConstant.SCORE), false)},
      {new Count(FieldReference.apply("b"), false), new CountStar()}
    };
    for (AggregateFunc[] funcs : unsupported) {
      LanceScanBuilder builder = createBuilder();
      Predicate[] filters = {TestPredicates.gt("x", 0L)};
      builder.pushPredicates(filters);
      assertFalse(builder.pushAggregation(new Aggregation(funcs, new Expression[0])));
      assertArrayEquals(filters, builder.pushedPredicates());
    }
  }

  @Test
  public void testPushAggregationRejectsCountColumnWithResidualFilters() {
    LanceScanBuilder builder = createBuilder();
    builder.pushPredicates(new Predicate[] {TestPredicates.contains("b", "test")});
    assertFalse(
        builder.pushAggregation(
            new Aggregation(
                new AggregateFunc[] {new Count(FieldReference.apply("b"), false)},
                new Expression[0])));
    assertEquals(0, builder.pushedPredicates().length);
  }

  @Test
  public void testPushAggregationRejectsCountColumnWhenFiltersDisabled() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            LanceSparkReadOptions.builder()
                .datasetUri(TestUtils.TestTable1Config.datasetUri)
                .pushDownFilters(false)
                .build(),
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    assertFalse(
        builder.pushAggregation(
            new Aggregation(
                new AggregateFunc[] {new Count(FieldReference.apply("b"), false)},
                new Expression[0])));
    assertEquals(0, builder.pushedPredicates().length);
  }

  @Test
  public void testPushAggregationRejectsCountColumnWithBacktickName() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            new StructType().add("a`b", DataTypes.StringType),
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    assertFalse(
        builder.pushAggregation(
            new Aggregation(
                new AggregateFunc[] {new Count(FieldReference.apply("`a``b`"), false)},
                new Expression[0])));
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

  /**
   * Asserts the contract that splits planned during {@link LanceScanBuilder#build()} match what
   * {@link LanceScan#planInputPartitions()} returns. This guarantees the second {@code
   * Dataset.open()} that previously happened during {@code planInputPartitions()} is gone — the
   * scan now consumes pre-computed splits instead of re-enumerating fragments.
   */
  @Test
  public void testBuildPrecomputesSplitsForPlanInputPartitions() {
    int expectedSplits =
        LanceSplit.planScan(TestUtils.TestTable1Config.readOptions).getSplits().size();
    LanceScanBuilder builder = createBuilder();
    LanceScan scan = (LanceScan) builder.build();
    assertEquals(expectedSplits, scan.planInputPartitions().length);
  }

  /**
   * Asserts that {@link LanceScanBuilder#build()} pins the resolved dataset version onto the read
   * options shipped to workers, so all tasks of one query observe a consistent snapshot even under
   * concurrent writes.
   */
  @Test
  public void testBuildPinsResolvedVersionOnReadOptions() {
    LanceScanBuilder builder = createBuilder();
    LanceScan scan = (LanceScan) builder.build();
    org.apache.spark.sql.connector.read.InputPartition[] partitions = scan.planInputPartitions();
    assertTrue(partitions.length > 0);
    LanceInputPartition first = (LanceInputPartition) partitions[0];
    LanceRef pinned = first.getReadOptions().getRef();
    assertNotNull(pinned, "build() must pin the resolved version onto readOptions");
    assertTrue(pinned.getVersionNumber().get() > 0);
  }

  @Test
  public void testTagFullTextQueryDoesNotUseNamespaceScan() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .ref(LanceRef.ofTag("stable"))
            .fullTextQuery(FullTextQuery.match("hello", "b"))
            .build();
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA, options, Collections.emptyMap(), "dir", Collections.emptyMap());

    assertFalse(builder.shouldNamespaceFtsScan());
  }

  @Test
  public void testBranchFullTextQueryDoesNotUseNamespaceScan() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .ref(LanceRef.ofBranch("audit"))
            .fullTextQuery(FullTextQuery.match("hello", "b"))
            .build();
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA, options, Collections.emptyMap(), "dir", Collections.emptyMap());

    assertFalse(builder.shouldNamespaceFtsScan());
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

  /**
   * A full-text query restricts rows but is carried in the read options rather than as a pushed
   * predicate, so the metadata-based COUNT(*) shortcut must not fire: answering from {@code
   * ManifestSummary.getTotalRows()} would ignore the FTS query and return the whole table's count.
   */
  @Test
  public void testBuildWithCountStarAndFullTextQueryReturnsLanceScan() {
    LanceSparkReadOptions ftsOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.readOptions.getDatasetUri())
            .fullTextQuery(FullTextQuery.match("hello", "name"))
            .build();
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA, ftsOptions, Collections.emptyMap(), null, Collections.emptyMap());
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    assertTrue(builder.pushAggregation(countStar), "COUNT(*) must still be pushed down");
    Scan scan = builder.build();
    assertInstanceOf(
        LanceScan.class,
        scan,
        "COUNT(*) with an active full-text query must be answered by a scan, not LanceLocalScan");
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
