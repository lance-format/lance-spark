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
package org.lance.spark.utils;

import org.lance.spark.LanceConstant;
import org.lance.spark.partition.PartitionTransform;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionTransformUtilTest {

  @Test
  public void testBucketTransform() {
    Transform[] transforms = new Transform[] {Expressions.bucket(32, "col1")};
    List<PartitionTransform> spec = PartitionTransformUtil.toSpec(transforms);
    assertEquals(1, spec.size());
    assertInstanceOf(PartitionTransform.Bucket.class, spec.get(0));
    assertEquals("col1", spec.get(0).getCol());
    assertEquals(32, ((PartitionTransform.Bucket) spec.get(0)).getNumBuckets());
  }

  @Test
  public void testIdentityTransform() {
    Transform[] transforms = new Transform[] {Expressions.identity("region")};
    List<PartitionTransform> spec = PartitionTransformUtil.toSpec(transforms);
    assertEquals(1, spec.size());
    assertInstanceOf(PartitionTransform.Identity.class, spec.get(0));
    assertEquals("region", spec.get(0).getCol());
  }

  @Test
  public void testNullTransforms() {
    List<PartitionTransform> spec = PartitionTransformUtil.toSpec(null);
    assertTrue(spec.isEmpty());
  }

  @Test
  public void testEmptyTransforms() {
    List<PartitionTransform> spec = PartitionTransformUtil.toSpec(new Transform[0]);
    assertTrue(spec.isEmpty());
  }

  @Test
  public void testMultiColumnBucketThrows() {
    Transform[] transforms = new Transform[] {Expressions.bucket(8, "col1", "col2")};
    assertThrows(
        UnsupportedOperationException.class, () -> PartitionTransformUtil.toSpec(transforms));
  }

  @Test
  public void testUnsupportedTransformThrows() {
    Transform[] transforms = new Transform[] {Expressions.years("ts")};
    assertThrows(
        UnsupportedOperationException.class, () -> PartitionTransformUtil.toSpec(transforms));
  }

  @Test
  public void testParseSpecFromMemWalShardingSpec() {
    Map<String, String> props = new java.util.HashMap<>();
    props.put(
        LanceConstant.TABLE_OPT_SHARDING_SPEC,
        "{\"spec_id\":0,\"fields\":[{\"field_id\":\"bucket(4, region)\","
            + "\"source_ids\":[],\"transform\":\"bucket\","
            + "\"expression\":\"bucket(4, region)\",\"result_type\":\"int32\","
            + "\"parameters\":{\"column\":\"region\",\"num_buckets\":\"4\"}}]}");
    List<PartitionTransform> spec = PartitionTransformUtil.parseSpec(props);
    assertEquals(1, spec.size());
    assertInstanceOf(PartitionTransform.Bucket.class, spec.get(0));
    assertEquals("region", spec.get(0).getCol());
    assertEquals(4, ((PartitionTransform.Bucket) spec.get(0)).getNumBuckets());
  }

  @Test
  public void testParseSpecLegacyBucket() {
    Map<String, String> props = new java.util.HashMap<>();
    props.put("lance.partition.bucket.columns", "region");
    props.put("lance.partition.bucket.num_buckets", "4");
    List<PartitionTransform> spec = PartitionTransformUtil.parseSpec(props);
    assertEquals(1, spec.size());
    assertInstanceOf(PartitionTransform.Bucket.class, spec.get(0));
    assertEquals(4, ((PartitionTransform.Bucket) spec.get(0)).getNumBuckets());
  }

  @Test
  public void testParseSpecLegacyIdentity() {
    Map<String, String> props = new java.util.HashMap<>();
    props.put("lance.partition.columns", "region");
    List<PartitionTransform> spec = PartitionTransformUtil.parseSpec(props);
    assertEquals(1, spec.size());
    assertInstanceOf(PartitionTransform.Identity.class, spec.get(0));
  }
}
