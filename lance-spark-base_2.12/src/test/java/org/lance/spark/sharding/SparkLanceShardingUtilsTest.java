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
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

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
}
