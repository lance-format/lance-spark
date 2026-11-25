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
package org.lance.spark.sharding;

import org.lance.index.scalar.ZoneStats;
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class SparkLanceShardingUtilsTest {

  @Test
  public void testBucketTransform() {
    Transform[] transforms = new Transform[] {Expressions.bucket(32, "col1")};
    ShardingSpec spec = SparkLanceShardingUtils.fromSparkTransforms(transforms);
    assertNotNull(spec);
    assertEquals(1, spec.fields().size());

    ShardingField field = spec.fields().get(0);
    assertEquals(Optional.of("bucket"), field.transform());
    assertEquals("col1", field.parameters().get("column"));
    assertEquals("32", field.parameters().get("num_buckets"));
    assertTrue(field.sourceIds().isEmpty());
  }

  @Test
  public void testIdentityTransform() {
    Transform[] transforms = new Transform[] {Expressions.identity("region")};
    ShardingSpec spec = SparkLanceShardingUtils.fromSparkTransforms(transforms);
    assertNotNull(spec);
    assertEquals(1, spec.fields().size());

    ShardingField field = spec.fields().get(0);
    assertEquals(Optional.of("identity"), field.transform());
    assertEquals("region", field.parameters().get("column"));
    assertTrue(field.sourceIds().isEmpty());
  }

  @Test
  public void testNullTransforms() {
    assertTrue(SparkLanceShardingUtils.isEmpty(SparkLanceShardingUtils.fromSparkTransforms(null)));
  }

  @Test
  public void testEmptyTransforms() {
    assertTrue(
        SparkLanceShardingUtils.isEmpty(
            SparkLanceShardingUtils.fromSparkTransforms(new Transform[0])));
  }

  @Test
  public void testMultiColumnBucketThrows() {
    Transform[] transforms = new Transform[] {Expressions.bucket(8, "col1", "col2")};
    assertThrows(
        UnsupportedOperationException.class,
        () -> SparkLanceShardingUtils.fromSparkTransforms(transforms));
  }

  @Test
  public void testUnsupportedTransformThrows() {
    Transform[] transforms = new Transform[] {Expressions.years("ts")};
    assertThrows(
        UnsupportedOperationException.class,
        () -> SparkLanceShardingUtils.fromSparkTransforms(transforms));
  }

  @Test
  public void testIdentitySparkExpressionUsesUnresolvedColumnParameter() {
    Transform[] transforms = new Transform[] {Expressions.identity("region")};
    ShardingField field = SparkLanceShardingUtils.fromSparkTransforms(transforms).fields().get(0);

    assertInstanceOf(FieldReference.class, SparkLanceShardingUtils.toSparkExpression(field, null));
    FieldReference expression =
        (FieldReference) SparkLanceShardingUtils.toSparkExpression(field, null);
    assertArrayEquals(new String[] {"region"}, expression.fieldNames());
  }

  @Test
  public void testNestedShardingColumnKeepsFieldPath() {
    String[] path = new String[] {"parent", "child"};
    ShardingField identity =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("parent.child")})
            .fields()
            .get(0);
    assertArrayEquals(
        path,
        ((NamedReference) SparkLanceShardingUtils.toSparkExpression(identity, null)).fieldNames());
    assertArrayEquals(path, SparkLanceShardingUtils.toClusteringRef(identity, null).fieldNames());
    assertArrayEquals(
        path,
        ((NamedReference) SparkLanceShardingUtils.toSortOrder(identity, null).expression())
            .fieldNames());

    ShardingField bucket =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.bucket(4, "parent.child")})
            .fields()
            .get(0);
    Transform transform = (Transform) SparkLanceShardingUtils.toSparkExpression(bucket, null);
    assertArrayEquals(path, transform.references()[0].fieldNames());
  }

  @Test
  public void testSourceIdBackedFieldRequiresLanceSchema() {
    ShardingField field =
        new ShardingField(
            "identity(region)",
            Collections.singletonList(1),
            "identity",
            "identity(region)",
            "utf8",
            Collections.singletonMap("column", "region"));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> SparkLanceShardingUtils.toSparkExpression(field, null));
    assertTrue(error.getMessage().contains("requires Lance schema"));
  }

  @Test
  public void testDetectFragmentKeysRequiresFullLiveCoverage() {
    ShardingField field =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("region")})
            .fields()
            .get(0);
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 10, "east", "east", 0), new ZoneStats(1, 0, 10, "west", "west", 0));

    Optional<Map<Integer, Object>> keys =
        SparkLanceShardingUtils.detectFragmentKeys(field, null, zones, Set.of(0, 1));

    assertTrue(keys.isPresent());
    assertEquals(Map.of(0, "east", 1, "west"), keys.get());
  }

  @Test
  public void testDetectFragmentKeysRejectsPartialLiveCoverage() {
    ShardingField field =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("region")})
            .fields()
            .get(0);
    List<ZoneStats> zones = Collections.singletonList(new ZoneStats(0, 0, 10, "east", "east", 0));

    Optional<Map<Integer, Object>> keys =
        SparkLanceShardingUtils.detectFragmentKeys(field, null, zones, Set.of(0, 1));

    assertFalse(keys.isPresent());
  }

  @Test
  public void testDetectFragmentKeysIgnoresRetiredFragmentStats() {
    ShardingField field =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("region")})
            .fields()
            .get(0);
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 10, "east", "east", 0),
            new ZoneStats(9, 0, 10, "retired", "retired", 0));

    Optional<Map<Integer, Object>> keys =
        SparkLanceShardingUtils.detectFragmentKeys(field, null, zones, Set.of(0));

    assertTrue(keys.isPresent());
    assertEquals(Collections.singletonMap(0, "east"), keys.get());
  }

  @Test
  public void testDetectFragmentKeysRejectsRangeWithinFragment() {
    ShardingField field =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("region")})
            .fields()
            .get(0);
    List<ZoneStats> zones = Collections.singletonList(new ZoneStats(0, 0, 10, "east", "west", 0));

    Optional<Map<Integer, Object>> keys =
        SparkLanceShardingUtils.detectFragmentKeys(field, null, zones, Set.of(0));

    assertFalse(keys.isPresent());
  }

  @Test
  public void testDetectFragmentKeysRejectsDifferentZoneValuesWithinFragment() {
    ShardingField field =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("region")})
            .fields()
            .get(0);
    List<ZoneStats> zones =
        Arrays.asList(
            new ZoneStats(0, 0, 10, "east", "east", 0),
            new ZoneStats(0, 10, 10, "west", "west", 0));

    Optional<Map<Integer, Object>> keys =
        SparkLanceShardingUtils.detectFragmentKeys(field, null, zones, Set.of(0));

    assertFalse(keys.isPresent());
  }

  @Test
  public void testDetectFragmentKeysWithInterleavedZonesAcrossManyFragments() {
    ShardingField field =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("region")})
            .fields()
            .get(0);
    int fragments = 1_000;
    List<ZoneStats> zones = new ArrayList<>();
    Set<Integer> live = new HashSet<>();
    Map<Integer, Object> expected = new HashMap<>();
    // Zones arrive ordered by zone, not by fragment, so each fragment's zones are interleaved.
    for (int zone = 0; zone < 2; zone++) {
      for (int fragment = 0; fragment < fragments; fragment++) {
        String value = "r" + (fragment % 97);
        zones.add(new ZoneStats(fragment, zone * 10, 10, value, value, 0));
        live.add(fragment);
        expected.put(fragment, value);
      }
    }

    Optional<Map<Integer, Object>> keys =
        SparkLanceShardingUtils.detectFragmentKeys(field, null, zones, live);

    assertTrue(keys.isPresent());
    assertEquals(expected, keys.get());
  }

  @Test
  public void testDetectFragmentKeysIsLinearInZones() {
    ShardingField field =
        SparkLanceShardingUtils.fromSparkTransforms(
                new Transform[] {Expressions.identity("region")})
            .fields()
            .get(0);
    // A table this size has 200,000 fragments with two zonemap zones each. Scanning every zone for
    // every fragment would take 8 x 10^10 comparisons; grouping zones by fragment takes 400,000.
    int fragments = 200_000;
    List<ZoneStats> zones = new ArrayList<>(2 * fragments);
    Set<Integer> live = new HashSet<>();
    for (int fragment = 0; fragment < fragments; fragment++) {
      String value = String.format("%04X", fragment % 65_536);
      zones.add(new ZoneStats(fragment, 0, 8_192, value, value, 0));
      zones.add(new ZoneStats(fragment, 8_192, 8_192, value, value, 0));
      live.add(fragment);
    }

    Optional<Map<Integer, Object>> keys =
        assertTimeoutPreemptively(
            Duration.ofSeconds(30),
            () -> SparkLanceShardingUtils.detectFragmentKeys(field, null, zones, live));

    assertTrue(keys.isPresent());
    assertEquals(fragments, keys.get().size());
    assertEquals("0001", keys.get().get(65_537));
  }
}
