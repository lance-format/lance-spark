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
package org.lance.spark.partition;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionFunctionTest {
  @Test
  void identityPartitionTypeParseAndType() {
    PartitionFunction.IdentityPartition function = new PartitionFunction.IdentityPartition();
    String value = "someValue";

    assertEquals(value, function.parse(value));
    assertEquals(PartitionFunction.Type.IDENTITY, function.getType());
  }

  @Test
  void identityPartitionTypePruneAvailability() {
    PartitionFunction.IdentityPartition function = new PartitionFunction.IdentityPartition();

    EqualTo equalTo = new EqualTo("col", "v");
    In in = new In("col", new Object[] {"a", "b"});
    GreaterThan greaterThan = new GreaterThan("col", 10);

    assertTrue(function.isPruneAvailable(equalTo));
    assertTrue(function.isPruneAvailable(in));
    assertFalse(function.isPruneAvailable(greaterThan));
  }

  @Test
  void bucketPartitionTypeParse() {
    int bucketSize = 10;
    PartitionFunction.BucketPartition function = new PartitionFunction.BucketPartition(bucketSize);

    String value = "someValue";
    int expectedBucket = value.hashCode() % bucketSize;

    assertEquals(String.valueOf(expectedBucket), function.parse(value));
    assertEquals(PartitionFunction.Type.BUCKET, function.getType());
  }

  @Test
  void bucketPartitionTypePruneAvailability() {
    PartitionFunction.BucketPartition function = new PartitionFunction.BucketPartition(4);

    EqualTo equalTo = new EqualTo("col", "v");
    In in = new In("col", new Object[] {"a", "b"});
    GreaterThan greaterThan = new GreaterThan("col", 10);

    assertTrue(function.isPruneAvailable(equalTo));
    assertTrue(function.isPruneAvailable(in));
    assertFalse(function.isPruneAvailable(greaterThan));
  }

  @Test
  void factoryCreatesIdentityPartitionFunction() {
    PartitionDescriptor descriptor = PartitionDescriptor.of("col", "identity", new HashMap<>());

    PartitionFunction function = descriptor.getPartitionFunction();
    assertTrue(function instanceof PartitionFunction.IdentityPartition);
    assertEquals(PartitionFunction.Type.IDENTITY, function.getType());

    String value = "value";
    assertEquals(value, function.parse(value));
  }

  @Test
  void factoryCreatesBucketPartitionFunction() {
    Map<String, String> properties = new HashMap<>();
    properties.put("num-buckets", "8");
    PartitionDescriptor descriptor = PartitionDescriptor.of("col", "bucket", properties);

    PartitionFunction function = descriptor.getPartitionFunction();
    assertTrue(function instanceof PartitionFunction.BucketPartition);
    assertEquals(PartitionFunction.Type.BUCKET, function.getType());

    String value = "value";
    int bucketSize = Integer.parseInt(properties.get("num-buckets"));
    int expectedBucket = value.hashCode() % bucketSize;
    assertEquals(String.valueOf(expectedBucket), function.parse(value));
  }

  @Test
  void factoryThrowsForUnknownFunction() {
    PartitionDescriptor descriptor = PartitionDescriptor.of("col", "unknown", new HashMap<>());

    assertThrows(IllegalArgumentException.class, () -> descriptor.getPartitionFunction());
  }
}
