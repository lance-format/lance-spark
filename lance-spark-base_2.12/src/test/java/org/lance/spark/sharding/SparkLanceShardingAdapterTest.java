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

import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SparkLanceShardingAdapterTest {

  @Test
  public void testBucketTransform() {
    Transform[] transforms = new Transform[] {Expressions.bucket(32, "col1")};
    List<SparkLanceShardingAdapter> spec = SparkLanceShardingAdapter.toSpec(transforms);
    assertEquals(1, spec.size());
    assertInstanceOf(SparkLanceShardingAdapter.Bucket.class, spec.get(0));
    assertEquals("col1", spec.get(0).getCol());
    assertEquals(32, ((SparkLanceShardingAdapter.Bucket) spec.get(0)).getNumBuckets());
  }

  @Test
  public void testIdentityTransform() {
    Transform[] transforms = new Transform[] {Expressions.identity("region")};
    List<SparkLanceShardingAdapter> spec = SparkLanceShardingAdapter.toSpec(transforms);
    assertEquals(1, spec.size());
    assertInstanceOf(SparkLanceShardingAdapter.Identity.class, spec.get(0));
    assertEquals("region", spec.get(0).getCol());
  }

  @Test
  public void testNullTransforms() {
    List<SparkLanceShardingAdapter> spec = SparkLanceShardingAdapter.toSpec(null);
    assertTrue(spec.isEmpty());
  }

  @Test
  public void testEmptyTransforms() {
    List<SparkLanceShardingAdapter> spec = SparkLanceShardingAdapter.toSpec(new Transform[0]);
    assertTrue(spec.isEmpty());
  }

  @Test
  public void testMultiColumnBucketThrows() {
    Transform[] transforms = new Transform[] {Expressions.bucket(8, "col1", "col2")};
    assertThrows(
        UnsupportedOperationException.class, () -> SparkLanceShardingAdapter.toSpec(transforms));
  }

  @Test
  public void testUnsupportedTransformThrows() {
    Transform[] transforms = new Transform[] {Expressions.years("ts")};
    assertThrows(
        UnsupportedOperationException.class, () -> SparkLanceShardingAdapter.toSpec(transforms));
  }

  @Test
  public void testFromShardingSpecUsesSourceIds() {
    ShardingField field =
        new ShardingField(
            "shard_key",
            Collections.singletonList(7),
            "identity",
            null,
            "utf8",
            Collections.emptyMap());
    ShardingSpec shardingSpec = new ShardingSpec(0, Collections.singletonList(field));

    List<SparkLanceShardingAdapter> spec =
        SparkLanceShardingAdapter.fromShardingSpec(shardingSpec, id -> id == 7 ? "region" : null);
    assertEquals(1, spec.size());
    assertInstanceOf(SparkLanceShardingAdapter.Identity.class, spec.get(0));
    assertEquals("region", spec.get(0).getCol());
  }

  @Test
  public void testFromBucketShardingSpec() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("num_buckets", "4");
    ShardingField field =
        new ShardingField(
            "shard_key", Collections.singletonList(7), "bucket", null, "int32", parameters);
    ShardingSpec shardingSpec = new ShardingSpec(0, Collections.singletonList(field));

    List<SparkLanceShardingAdapter> spec =
        SparkLanceShardingAdapter.fromShardingSpec(shardingSpec, id -> id == 7 ? "region" : null);
    assertEquals(1, spec.size());
    assertInstanceOf(SparkLanceShardingAdapter.Bucket.class, spec.get(0));
    assertEquals("region", spec.get(0).getCol());
    assertEquals(4, ((SparkLanceShardingAdapter.Bucket) spec.get(0)).getNumBuckets());
  }
}
