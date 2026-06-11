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
import org.lance.spark.read.nativeplan.LanceNativeScanFallbackReason;
import org.lance.spark.read.nativeplan.LanceNativeScanFallbackReasonCode;
import org.lance.spark.read.nativeplan.LanceNativeScanPlan;
import org.lance.spark.read.nativeplan.LanceNativeScanSplit;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

public class LanceScanTest {

  private static final StructType TEST_SCHEMA = TestUtils.TestTable1Config.schema;
  private static final int NATIVE_BATCH_SIZE = 1234;
  private static final String CATALOG_NAME = "native_catalog";
  private static final String NAMESPACE_IMPL = "dir";
  private static final String NAMESPACE_PROPERTY_KEY = "root";
  private static final String NAMESPACE_PROPERTY_VALUE = "/tmp/native";
  private static final String STORAGE_OPTION_KEY = "native.option";
  private static final String STORAGE_OPTION_VALUE = "driver";
  private static final String TABLE_NAMESPACE = "default";
  private static final String TABLE_NAME = "table";

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

  private LanceScan buildScan(
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    return (LanceScan)
        new LanceScanBuilder(
                TEST_SCHEMA, readOptions, initialStorageOptions, namespaceImpl, namespaceProperties)
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
    builder.pushPredicates(new Predicate[] {TestPredicates.gt("x", 0L)});
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
    builder.pushPredicates(new Predicate[] {TestPredicates.gt("x", 0L)});
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

  @Test
  public void testNativeScanPlanForEligibleOrdinaryRead() throws Exception {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .batchSize(NATIVE_BATCH_SIZE)
            .tableId(Arrays.asList(TABLE_NAMESPACE, TABLE_NAME))
            .catalogName(CATALOG_NAME)
            .build();
    LanceScan scan =
        buildScan(
            readOptions,
            Collections.singletonMap(STORAGE_OPTION_KEY, STORAGE_OPTION_VALUE),
            NAMESPACE_IMPL,
            Collections.singletonMap(NAMESPACE_PROPERTY_KEY, NAMESPACE_PROPERTY_VALUE));

    java.util.Optional<LanceNativeScanPlan> maybePlan = scan.nativeScanPlan();
    assertTrue(maybePlan.isPresent());
    assertFalse(scan.nativeScanFallbackReason().isPresent());
    LanceNativeScanPlan plan = maybePlan.get();

    assertEquals(LanceNativeScanPlan.DESCRIPTOR_VERSION, plan.getDescriptorVersion());
    assertNotNull(plan.getScanId());
    assertEquals(TestUtils.TestTable1Config.datasetUri, plan.getDatasetUri());
    assertTrue(plan.getResolvedVersion() > 0);
    assertEquals(TEST_SCHEMA.json(), plan.getSparkReadSchemaJson());
    assertEquals(TEST_SCHEMA.json(), plan.getProjectedReadSchemaJson());
    assertFalse(plan.hasPushedFilterSql());
    assertFalse(plan.hasLimit());
    assertFalse(plan.hasOffset());
    assertEquals(NATIVE_BATCH_SIZE, plan.getBatchSize());
    assertEquals(STORAGE_OPTION_VALUE, plan.getStorageOptions().get(STORAGE_OPTION_KEY));
    assertEquals(NAMESPACE_IMPL, plan.getNamespaceImpl());
    assertEquals(
        NAMESPACE_PROPERTY_VALUE, plan.getNamespaceProperties().get(NAMESPACE_PROPERTY_KEY));
    assertEquals(Arrays.asList(TABLE_NAMESPACE, TABLE_NAME), plan.getTableId());
    assertEquals(CATALOG_NAME, plan.getCatalogName());
    assertFalse(plan.getSplits().isEmpty());
    for (LanceNativeScanSplit split : plan.getSplits()) {
      assertFalse(split.getFragmentIds().isEmpty());
    }
    assertThrows(
        UnsupportedOperationException.class,
        () -> plan.getStorageOptions().put(STORAGE_OPTION_KEY, STORAGE_OPTION_VALUE));
    assertThrows(UnsupportedOperationException.class, () -> plan.getTableId().add(TABLE_NAME));
    assertThrows(
        UnsupportedOperationException.class,
        () -> plan.getSplits().add(new LanceNativeScanSplit(0, Collections.singletonList(0))));

    LanceNativeScanPlan roundTripped = roundTrip(plan);
    assertEquals(plan.getDatasetUri(), roundTripped.getDatasetUri());
    assertEquals(plan.getSplits().size(), roundTripped.getSplits().size());
  }

  @Test
  public void testNativeScanPlanIncludesProjectedSchemaAndPushedOptions() {
    StructType projectedSchema = new StructType().add("x", DataTypes.LongType);
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    builder.pruneColumns(projectedSchema);
    builder.pushPredicates(new Predicate[] {TestPredicates.gt("x", 0L)});
    builder.pushLimit(2);

    LanceScan scan = (LanceScan) builder.build();
    java.util.Optional<LanceNativeScanPlan> maybePlan = scan.nativeScanPlan();
    assertTrue(maybePlan.isPresent());
    LanceNativeScanPlan plan = maybePlan.get();

    assertEquals(TEST_SCHEMA.json(), plan.getSparkReadSchemaJson());
    assertEquals(projectedSchema.json(), plan.getProjectedReadSchemaJson());
    assertTrue(plan.hasPushedFilterSql());
    assertNotNull(plan.getPushedFilterSql());
    assertEquals(Integer.valueOf(2), plan.getLimit());
    assertFalse(plan.hasOffset());
  }

  @Test
  public void testNativeScanFallbackReasons() {
    FallbackCase[] cases =
        new FallbackCase[] {
          new FallbackCase(
              "aggregation",
              this::buildPushedAggregationScan,
              LanceNativeScanFallbackReasonCode.PUSHED_AGGREGATION),
          new FallbackCase("topN", this::buildTopNScan, LanceNativeScanFallbackReasonCode.TOP_N),
          new FallbackCase(
              "missing version",
              this::buildMissingResolvedVersionScan,
              LanceNativeScanFallbackReasonCode.MISSING_RESOLVED_VERSION),
          new FallbackCase(
              "missing splits",
              this::buildMissingSplitStateScan,
              LanceNativeScanFallbackReasonCode.MISSING_SPLIT_STATE),
          new FallbackCase(
              "unsafe split",
              this::buildUnsafeSplitStateScan,
              LanceNativeScanFallbackReasonCode.UNSAFE_V1_STATE)
        };

    for (FallbackCase testCase : cases) {
      LanceScan scan = testCase.scanSupplier.get();
      java.util.Optional<LanceNativeScanFallbackReason> reason = scan.nativeScanFallbackReason();
      assertTrue(reason.isPresent(), testCase.name);
      assertEquals(testCase.expectedCode, reason.get().getCode(), testCase.name);
      assertFalse(scan.nativeScanPlan().isPresent(), testCase.name);
    }
  }

  @Test
  public void testLimitPrunesPartitions() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
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
    Map<Integer, Object> fragKeys = new HashMap<>();
    fragKeys.put(0, "east");
    fragKeys.put(1, "west");
    Expression partitionExpression = Expressions.column("region");

    LanceSplit.ScanPlanResult plan = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    LanceScan scan =
        new LanceScan(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            new Predicate[0],
            null,
            Collections.emptyMap(),
            null,
            plan.getSplits(),
            plan.getFragmentRowCounts(),
            partitionExpression,
            fragKeys,
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

  @Test
  public void testOutputPartitioningWithBucketInfo() {
    Map<Integer, Object> fragKeys = new HashMap<>();
    fragKeys.put(0, 0);
    fragKeys.put(1, 1);
    fragKeys.put(2, 2);
    Expression partitionExpression = Expressions.bucket(4, "region");

    LanceSplit.ScanPlanResult bucketPlan =
        LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    LanceScan scan =
        new LanceScan(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            org.lance.spark.utils.Optional.empty(),
            new Predicate[0],
            null,
            Collections.emptyMap(),
            null,
            bucketPlan.getSplits(),
            bucketPlan.getFragmentRowCounts(),
            partitionExpression,
            fragKeys,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());

    Partitioning partitioning = scan.outputPartitioning();
    assertInstanceOf(KeyGroupedPartitioning.class, partitioning);
    KeyGroupedPartitioning kgp = (KeyGroupedPartitioning) partitioning;
    assertEquals(3, kgp.numPartitions());
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
            Collections.emptyMap());
    builder2.pushPredicates(new Predicate[] {TestPredicates.gt("x", 0L)});
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
                    Collections.emptyMap())
                .build();

    assertNotEquals(scan1, scan2, "Scans with different schemas should not be equal");
  }

  private LanceScan buildPushedAggregationScan() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    builder.pushPredicates(new Predicate[] {TestPredicates.gt("x", 0L)});
    builder.pushAggregation(
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {}));
    return (LanceScan) builder.build();
  }

  private LanceScan buildTopNScan() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    assertTrue(
        builder.pushTopN(
            new SortOrder[] {
              Expressions.sort(
                  Expressions.column("x"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
            },
            10));
    return (LanceScan) builder.build();
  }

  private LanceScan buildMissingResolvedVersionScan() {
    LanceSplit.ScanPlanResult plan = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    return directScan(
        TestUtils.TestTable1Config.readOptions, plan.getSplits(), plan.getFragmentRowCounts());
  }

  private LanceScan buildMissingSplitStateScan() {
    LanceSparkReadOptions resolvedReadOptions = resolvedReadOptions();
    return directScan(resolvedReadOptions, null, Collections.emptyMap());
  }

  private LanceScan buildUnsafeSplitStateScan() {
    LanceSparkReadOptions resolvedReadOptions = resolvedReadOptions();
    return directScan(
        resolvedReadOptions,
        Collections.singletonList(new LanceSplit(Collections.singletonList(-1))),
        Collections.emptyMap());
  }

  private LanceScan directScan(
      LanceSparkReadOptions readOptions,
      List<LanceSplit> splits,
      Map<Integer, Long> fragmentRowCounts) {
    return new LanceScan(
        TEST_SCHEMA,
        readOptions,
        org.lance.spark.utils.Optional.empty(),
        org.lance.spark.utils.Optional.empty(),
        org.lance.spark.utils.Optional.empty(),
        org.lance.spark.utils.Optional.empty(),
        org.lance.spark.utils.Optional.empty(),
        new Predicate[0],
        null,
        Collections.emptyMap(),
        null,
        splits,
        fragmentRowCounts,
        null,
        null,
        Collections.emptyMap(),
        null,
        Collections.emptyMap());
  }

  private LanceSparkReadOptions resolvedReadOptions() {
    LanceSplit.ScanPlanResult plan = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    return TestUtils.TestTable1Config.readOptions.withVersion(plan.getResolvedVersion());
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T value) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  private static class FallbackCase {
    private final String name;
    private final Supplier<LanceScan> scanSupplier;
    private final LanceNativeScanFallbackReasonCode expectedCode;

    private FallbackCase(
        String name,
        Supplier<LanceScan> scanSupplier,
        LanceNativeScanFallbackReasonCode expectedCode) {
      this.name = name;
      this.scanSupplier = scanSupplier;
      this.expectedCode = expectedCode;
    }
  }
}
