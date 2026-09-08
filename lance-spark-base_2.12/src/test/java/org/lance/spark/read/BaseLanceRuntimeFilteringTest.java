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
import org.lance.spark.utils.BucketHashUtil;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.InputPartition;
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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Runtime filtering contract tests, instantiated only by the supported Spark 4.1+ modules. */
public abstract class BaseLanceRuntimeFilteringTest {
  private static final StructType SCHEMA =
      new StructType()
          .add("x", DataTypes.IntegerType)
          .add("y", DataTypes.StringType)
          .add("region", DataTypes.IntegerType)
          .add("_rowaddr", DataTypes.LongType);

  @Test
  public void testFilterAttributesAreReturnedDefensively() {
    LanceScan scan = newScan(new String[] {"x", "_rowaddr"}, options(), null, null, null, null);
    NamedReference[] first = scan.filterAttributes();
    assertArrayEquals(new String[] {"x", "_rowaddr"}, names(first));
    first[0] = Expressions.column("y");
    assertArrayEquals(new String[] {"x", "_rowaddr"}, names(scan.filterAttributes()));
  }

  @Test
  public void testPushDownFiltersDisablesRuntimeFiltering() {
    LanceSparkReadOptions disabled =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .pushDownFilters(false)
            .build();
    LanceScan scan = newScan(new String[] {"x"}, disabled, null, null, null, null);
    assertEquals(0, scan.filterAttributes().length);
    scan.filter(new Predicate[] {TestPredicates.in("x", 50)});
    assertEquals(0, scan.pushedPredicates().length);
    assertEquals(3, scan.planInputPartitions().length);
  }

  @Test
  public void testPushedAggregationDoesNotAdvertiseColumnsOutsideReadSchema() {
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TestUtils.TestTable1Config.schema,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    builder.pushPredicates(new Predicate[] {TestPredicates.gt("x", 0L)});
    builder.pushAggregation(
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[0]));
    LanceScan scan = (LanceScan) builder.build();

    assertEquals(1, scan.readSchema().size());
    assertEquals("count", scan.readSchema().fieldNames()[0]);
    assertEquals(0, scan.filterAttributes().length);
  }

  @Test
  public void testAcceptsCanonicalInAndDeduplicatesKeysAndCalls() {
    LanceScan scan = newScan(new String[] {"x"}, options(), null, null, null, null);
    Predicate predicate = TestPredicates.in("x", 50, 50, 150);
    scan.filter(new Predicate[] {predicate});
    scan.filter(new Predicate[] {predicate});

    assertEquals(1, scan.pushedPredicates().length);
    assertEquals(3, scan.pushedPredicates()[0].children().length);
    assertEquals(2, scan.planInputPartitions().length);
  }

  @Test
  public void testRepeatedCallsAreAnded() {
    LanceScan scan = newScan(new String[] {"x"}, options(), null, null, null, null);
    scan.filter(new Predicate[] {TestPredicates.in("x", 50, 150)});
    scan.filter(new Predicate[] {TestPredicates.in("x", 150, 250)});

    InputPartition[] partitions = scan.planInputPartitions();
    assertEquals(1, partitions.length);
    assertEquals(1, fragmentId(partitions[0]));
    String where = ((LanceInputPartition) partitions[0]).getWhereCondition().get();
    assertTrue(where.contains("AND"));
  }

  @Test
  public void testConcurrentDuplicateCallsAreThreadSafe() throws Exception {
    LanceScan scan = newScan(new String[] {"x"}, options(), null, null, null, null);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    for (int i = 0; i < 20; i++) {
      executor.submit(() -> scan.filter(new Predicate[] {TestPredicates.in("x", 150)}));
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

    assertEquals(1, scan.pushedPredicates().length);
    assertEquals(1, scan.planInputPartitions().length);
  }

  @Test
  public void testEmptyAndNullOnlyRuntimeKeysProduceEmptyScan() {
    LanceScan empty = newScan(new String[] {"x"}, options(), null, null, null, null);
    empty.filter(new Predicate[] {TestPredicates.in("x")});
    assertEquals(0, empty.planInputPartitions().length);

    LanceScan nullOnly = newScan(new String[] {"x"}, options(), null, null, null, null);
    nullOnly.filter(new Predicate[] {TestPredicates.in("x", (Object) null)});
    assertEquals(0, nullOnly.planInputPartitions().length);
  }

  @Test
  public void testNullKeysAreDroppedFromEqualityRuntimeFilter() {
    LanceScan scan = newScan(new String[] {"x"}, options(), null, null, null, null);
    scan.filter(new Predicate[] {TestPredicates.in("x", null, 150)});
    assertEquals(2, scan.pushedPredicates()[0].children().length);
    assertEquals(1, scan.planInputPartitions().length);
  }

  @Test
  public void testUnsupportedShapesAndColumnsAreObservableNoOps() {
    LanceScan scan = newScan(new String[] {"x"}, options(), null, null, null, null);
    scan.filter(
        new Predicate[] {
          TestPredicates.eq("x", 50),
          TestPredicates.in("y", "a"),
          new Predicate("IN", new Expression[] {Expressions.column("x"), Expressions.column("y")})
        });

    assertEquals(0, scan.pushedPredicates().length);
    assertEquals(3, scan.planInputPartitions().length);
    assertEquals("3", metadata(scan).get("rejectedRuntimeFilterCount"));
  }

  @Test
  public void testKeyAndSerializedSizeGuardrails() {
    LanceSparkReadOptions oneKey =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .runtimeFilterMaxKeys(1)
            .build();
    LanceScan keyGuard = newScan(new String[] {"x"}, oneKey, null, null, null, null);
    keyGuard.filter(new Predicate[] {TestPredicates.in("x", 50, 150)});
    assertEquals(0, keyGuard.pushedPredicates().length);

    LanceSparkReadOptions tinyBytes =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .runtimeFilterMaxBytes(1)
            .build();
    LanceScan byteGuard = newScan(new String[] {"x"}, tinyBytes, null, null, null, null);
    byteGuard.filter(new Predicate[] {TestPredicates.in("x", 50)});
    assertEquals(0, byteGuard.pushedPredicates().length);
  }

  @Test
  public void testStaticAndRuntimeFiltersComposeOnWorkers() {
    Predicate staticPredicate = TestPredicates.gte("x", 100);
    LanceScan scan =
        newScan(
            new String[] {"x"},
            options(),
            new Predicate[] {staticPredicate},
            Set.of(1, 2),
            null,
            null);
    scan.filter(new Predicate[] {TestPredicates.in("x", 150, 250)});

    InputPartition[] partitions = scan.planInputPartitions();
    assertEquals(2, partitions.length);
    for (InputPartition partition : partitions) {
      String where = ((LanceInputPartition) partition).getWhereCondition().get();
      assertTrue(where.contains("x >= 100"));
      assertTrue(where.contains("x IN (150,250)"));
      assertTrue(where.contains("AND"));
    }
  }

  @Test
  public void testPartialZonemapCoverageRetainsUncoveredFragments() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0), new ZoneStats(1, 0, 100, 100L, 199L, 0)));
    Map<String, Set<Integer>> uncovered = Collections.singletonMap("x", Set.of(2));
    LanceScan scan = newScan(new String[] {"x"}, options(), null, null, stats, uncovered);
    scan.filter(new Predicate[] {TestPredicates.in("x", 50)});

    assertArrayEquals(new int[] {0, 2}, fragmentIds(scan.planInputPartitions()));
  }

  @Test
  public void testExactRowAddressPruningUsesRuntimePredicate() {
    LanceScan scan = newScan(new String[] {"_rowaddr"}, options(), null, null, null, null);
    scan.filter(new Predicate[] {TestPredicates.in("_rowaddr", 0L, 2L << 32)});
    assertArrayEquals(new int[] {0, 2}, fragmentIds(scan.planInputPartitions()));
  }

  @Test
  public void testExactIdentityShardingPruningPreservesPartitionKeys() {
    Map<Integer, Object> keys = new HashMap<>();
    keys.put(0, 10L);
    keys.put(1, 20L);
    keys.put(2, 30L);
    LanceScan scan =
        newScan(
            new String[] {"region"},
            options(),
            null,
            null,
            null,
            null,
            Expressions.column("region"),
            "region",
            keys);
    scan.filter(new Predicate[] {TestPredicates.in("region", 20)});

    InputPartition[] partitions = scan.planInputPartitions();
    assertEquals(1, partitions.length);
    assertEquals(1, fragmentId(partitions[0]));
    InternalRow partitionKey = ((LanceInputPartition) partitions[0]).partitionKey();
    assertEquals(20, partitionKey.getInt(0));
  }

  @Test
  public void testExactBucketShardingPruning() {
    Map<Integer, Object> keys = new HashMap<>();
    keys.put(0, BucketHashUtil.computeBucketIdFromValue(10L, 4));
    keys.put(1, BucketHashUtil.computeBucketIdFromValue(20L, 4));
    keys.put(2, BucketHashUtil.computeBucketIdFromValue(30L, 4));
    LanceScan scan =
        newScan(
            new String[] {"region"},
            options(),
            null,
            null,
            null,
            null,
            Expressions.bucket(4, "region"),
            "region",
            keys);
    scan.filter(new Predicate[] {TestPredicates.in("region", 20)});

    Set<Integer> expectedBucket =
        Collections.singleton(BucketHashUtil.computeBucketIdFromValue(20L, 4));
    for (InputPartition partition : scan.planInputPartitions()) {
      assertTrue(expectedBucket.contains(keys.get(fragmentId(partition))));
    }
  }

  @Test
  public void testRuntimeFilterDisablesLimitSplitPruning() {
    LanceScan scan =
        newScan(
            new String[] {"x"},
            options(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Optional.of(1));
    scan.filter(new Predicate[] {TestPredicates.in("x", 50, 150)});
    assertEquals(2, scan.planInputPartitions().length);
  }

  @Test
  public void testRuntimeMutationDoesNotAffectEqualityOrHashCode() {
    LanceScan left = newScan(new String[] {"x"}, options(), null, null, null, null);
    LanceScan right = newScan(new String[] {"x"}, options(), null, null, null, null);
    int hash = left.hashCode();

    left.filter(new Predicate[] {TestPredicates.in("x", 50)});

    assertEquals(right, left);
    assertEquals(hash, left.hashCode());
    assertEquals(right.hashCode(), left.hashCode());
  }

  @Test
  public void testRuntimeStateIsSerializable() throws Exception {
    LanceScan scan = newScan(new String[] {"x"}, options(), null, null, null, null);
    scan.filter(new Predicate[] {TestPredicates.in("x", 150)});

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(scan);
    }
    LanceScan restored;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (LanceScan) in.readObject();
    }

    assertEquals(1, restored.pushedPredicates().length);
    assertEquals(1, restored.planInputPartitions().length);
  }

  private static LanceSparkReadOptions options() {
    return LanceSparkReadOptions.builder()
        .datasetUri(TestUtils.TestTable1Config.datasetUri)
        .build();
  }

  private static LanceScan newScan(
      String[] runtimeColumns,
      LanceSparkReadOptions readOptions,
      Predicate[] staticPredicates,
      Set<Integer> staticSurvivors,
      Map<String, List<ZoneStats>> stats,
      Map<String, Set<Integer>> uncovered) {
    return newScan(
        runtimeColumns,
        readOptions,
        staticPredicates,
        staticSurvivors,
        stats,
        uncovered,
        null,
        null,
        null);
  }

  private static LanceScan newScan(
      String[] runtimeColumns,
      LanceSparkReadOptions readOptions,
      Predicate[] staticPredicates,
      Set<Integer> staticSurvivors,
      Map<String, List<ZoneStats>> stats,
      Map<String, Set<Integer>> uncovered,
      Expression shardingExpression,
      String shardingColumn,
      Map<Integer, Object> shardingKeys) {
    return newScan(
        runtimeColumns,
        readOptions,
        staticPredicates,
        staticSurvivors,
        stats,
        uncovered,
        shardingExpression,
        shardingColumn,
        shardingKeys,
        Optional.empty());
  }

  private static LanceScan newScan(
      String[] runtimeColumns,
      LanceSparkReadOptions readOptions,
      Predicate[] staticPredicates,
      Set<Integer> staticSurvivors,
      Map<String, List<ZoneStats>> stats,
      Map<String, Set<Integer>> uncovered,
      Expression shardingExpression,
      String shardingColumn,
      Map<Integer, Object> shardingKeys,
      Optional<Integer> limit) {
    Predicate[] staticFilters = staticPredicates != null ? staticPredicates : new Predicate[0];
    Map<String, List<ZoneStats>> zonemapStats = stats != null ? stats : defaultStats();
    Map<String, Set<Integer>> coverage = uncovered != null ? uncovered : Collections.emptyMap();
    List<LanceSplit> splits =
        Arrays.asList(
            new LanceSplit(Collections.singletonList(0)),
            new LanceSplit(Collections.singletonList(1)),
            new LanceSplit(Collections.singletonList(2)));
    Map<Integer, Long> rowCounts = Map.of(0, 100L, 1, 100L, 2, 100L);
    NamedReference[] attributes =
        Arrays.stream(runtimeColumns).map(FieldReference::column).toArray(NamedReference[]::new);
    return new LanceScan(
        SCHEMA,
        readOptions,
        FilterPushDown.compileFiltersToSqlWhereClause(staticFilters),
        limit,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        staticFilters,
        null,
        staticSurvivors,
        zonemapStats,
        coverage,
        attributes,
        splits,
        rowCounts,
        shardingExpression,
        shardingColumn,
        shardingKeys,
        Collections.emptyMap(),
        null,
        Collections.emptyMap());
  }

  private static Map<String, List<ZoneStats>> defaultStats() {
    Map<String, List<ZoneStats>> stats = new HashMap<>();
    stats.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 100, 0L, 99L, 0),
            new ZoneStats(1, 0, 100, 100L, 199L, 0),
            new ZoneStats(2, 0, 100, 200L, 299L, 0)));
    return stats;
  }

  private static String[] names(NamedReference[] references) {
    return Arrays.stream(references)
        .map(reference -> String.join(".", reference.fieldNames()))
        .toArray(String[]::new);
  }

  private static int fragmentId(InputPartition partition) {
    return ((LanceInputPartition) partition).getLanceSplit().getFragments().get(0);
  }

  private static int[] fragmentIds(InputPartition[] partitions) {
    return Arrays.stream(partitions).mapToInt(BaseLanceRuntimeFilteringTest::fragmentId).toArray();
  }

  private static Map<String, String> metadata(LanceScan scan) {
    return scala.collection.JavaConverters.mapAsJavaMap(scan.getMetaData());
  }
}
