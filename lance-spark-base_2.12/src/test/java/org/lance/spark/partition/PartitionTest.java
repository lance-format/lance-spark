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

import org.lance.spark.LanceDataset;
import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionTest {

  private LanceDataset newDataset(String id) {
    String uri = "file:///tmp/db/" + id + ".lance";
    return new LanceDataset(LanceSparkReadOptions.from(uri), new StructType());
  }

  @Test
  public void testPartitionConstruction() {
    PartitionFunction identity = new PartitionFunction.IdentityPartition();
    LanceDataset ds = newDataset("leaf");

    Partition leaf = Partition.leaf("country", identity.parse("US"), identity, ds);
    assertTrue(leaf.isLeaf());
    assertSame(ds, leaf.dataset());
    assertEquals("country", leaf.columnName());
    assertEquals(identity.parse("US"), leaf.value());
    assertTrue(leaf.childrenOrEmpty().isEmpty());

    List<Partition> children = new ArrayList<>();
    children.add(leaf);
    Partition inner = Partition.inner("country", identity.parse("US"), identity, children);
    assertFalse(inner.isLeaf());
    assertNull(inner.dataset());
    assertEquals("country", inner.columnName());
    assertEquals(identity.parse("US"), inner.value());
    assertEquals(children, inner.childrenOrEmpty());
  }

  @Test
  public void testColumnNamesCollectsAndExcludesRoot() {
    PartitionFunction identity = new PartitionFunction.IdentityPartition();
    PartitionFunction bucket = new PartitionFunction.BucketPartition(10);

    LanceDataset ds1 = newDataset("us_bucket1");
    LanceDataset ds2 = newDataset("us_bucket2");
    LanceDataset ds3 = newDataset("cn_bucket1");

    Partition leafUs1 = Partition.leaf("bucket", bucket.parse("u1"), bucket, ds1);
    Partition leafUs2 = Partition.leaf("bucket", bucket.parse("u2"), bucket, ds2);
    Partition leafCn1 = Partition.leaf("bucket", bucket.parse("c1"), bucket, ds3);

    List<Partition> usChildren = new ArrayList<>();
    usChildren.add(leafUs1);
    usChildren.add(leafUs2);

    List<Partition> cnChildren = new ArrayList<>();
    cnChildren.add(leafCn1);

    Partition countryUs = Partition.inner("country", identity.parse("US"), identity, usChildren);
    Partition countryCn = Partition.inner("country", identity.parse("CN"), identity, cnChildren);

    List<Partition> rootChildren = new ArrayList<>();
    rootChildren.add(countryUs);
    rootChildren.add(countryCn);

    Partition root = Partition.inner("root", "rootValue", identity, rootChildren);

    Set<String> names = root.columnNames();
    assertEquals(new HashSet<>(Arrays.asList("country", "bucket")), names);
    assertFalse(names.contains("root"));
  }

  @Test
  public void testIsPruneAvailableOnFilters() {
    List<PartitionDescriptor> partitionDescriptors = new ArrayList<>();
    partitionDescriptors.add(PartitionDescriptor.of("country", "identity", new HashMap<>()));

    // EqualTo on existing attribute and supported by partition function.
    assertTrue(PartitionUtils.isPruneAvailable(partitionDescriptors, new EqualTo("country", "US")));

    // In on existing attribute and supported by partition function.
    assertTrue(
        PartitionUtils.isPruneAvailable(
            partitionDescriptors, new In("country", new Object[] {"US", "CA"})));

    // Attribute not in partition columns.
    assertFalse(PartitionUtils.isPruneAvailable(partitionDescriptors, new EqualTo("city", "NY")));

    // Unsupported filter type even on existing attribute.
    assertFalse(
        PartitionUtils.isPruneAvailable(partitionDescriptors, new GreaterThan("country", "US")));
  }

  @Test
  public void testLeaves() {
    PartitionFunction identity = new PartitionFunction.IdentityPartition();
    PartitionFunction bucket = new PartitionFunction.BucketPartition(10);

    LanceDataset dsUs0 = newDataset("us_0");
    LanceDataset dsUs1 = newDataset("us_1");
    LanceDataset dsCn0 = newDataset("cn_0");

    Partition leafUs0 = Partition.leaf("bucket", bucket.parse("raw0"), bucket, dsUs0);
    Partition leafUs1 = Partition.leaf("bucket", bucket.parse("raw1"), bucket, dsUs1);
    Partition leafCn0 = Partition.leaf("bucket", bucket.parse("raw2"), bucket, dsCn0);

    List<Partition> usChildren = new ArrayList<>();
    usChildren.add(leafUs0);
    usChildren.add(leafUs1);
    Partition countryUs = Partition.inner("country", identity.parse("US"), identity, usChildren);

    List<Partition> cnChildren = new ArrayList<>();
    cnChildren.add(leafCn0);
    Partition countryCn = Partition.inner("country", identity.parse("CN"), identity, cnChildren);

    List<Partition> rootChildren = new ArrayList<>();
    rootChildren.add(countryUs);
    rootChildren.add(countryCn);
    Partition root = Partition.inner("root", "rootValue", identity, rootChildren);

    // Returns all leaf datasets.
    List<LanceDataset> leaves = root.leaves();
    assertEquals(3, leaves.size());
    assertTrue(leaves.contains(dsUs0));
    assertTrue(leaves.contains(dsUs1));
    assertTrue(leaves.contains(dsCn0));
  }

  @Test
  public void testResolveOrCreateExistingPathReturnsExistingLeaf() {
    PartitionFunction identity = new PartitionFunction.IdentityPartition();
    LanceDataset ds = newDataset("us");

    Partition existingLeaf = Partition.leaf("country", identity.parse("US"), identity, ds);
    List<Partition> rootChildren = new ArrayList<>();
    rootChildren.add(existingLeaf);
    Partition root = Partition.inner("root", "rootValue", identity, rootChildren);

    PartitionDescriptor pd = PartitionDescriptor.of("country", "IDENTITY", Collections.emptyMap());

    PartitionKey key =
        PartitionKey.of(Collections.singletonList(pd), Collections.singletonList("US"));

    PartitionCreator creator =
        new PartitionCreator() {
          @Override
          public LanceDataset createLeafDataset(List<String> id) {
            throw new AssertionError("createLeafDataset should not be called for existing path");
          }

          @Override
          public void createInnerNamespace(List<String> id) {
            throw new AssertionError("createInnerNamespace should not be called for existing path");
          }
        };

    Partition resolved = root.resolveOrCreate(key, creator);
    assertSame(existingLeaf, resolved);
  }

  @Test
  public void testResolveOrCreateCreatesInnerAndLeafUsingPartitionCreator() {
    PartitionFunction identity = new PartitionFunction.IdentityPartition();
    Partition root = Partition.inner("root", "rootValue", identity, new ArrayList<>());

    PartitionDescriptor countryDesc =
        PartitionDescriptor.of("country", "IDENTITY", Collections.emptyMap());

    Map<String, String> props = new HashMap<>();
    props.put("num-buckets", "10");
    PartitionDescriptor bucketDesc = PartitionDescriptor.of("bucket", "BUCKET", props);

    List<PartitionDescriptor> descriptors = Arrays.asList(countryDesc, bucketDesc);
    List<String> values = Arrays.asList("US", "user1");
    PartitionKey key = PartitionKey.of(descriptors, values);

    List<List<String>> createdInnerIds = new ArrayList<>();
    List<List<String>> createdLeafIds = new ArrayList<>();

    PartitionCreator creator =
        new PartitionCreator() {
          @Override
          public LanceDataset createLeafDataset(List<String> id) {
            createdLeafIds.add(new ArrayList<>(id));
            String uri = "file:///tmp/db/" + String.join("/", id) + ".lance";
            return new LanceDataset(LanceSparkReadOptions.from(uri), new StructType());
          }

          @Override
          public void createInnerNamespace(List<String> id) {
            createdInnerIds.add(new ArrayList<>(id));
          }
        };

    Partition leaf = root.resolveOrCreate(key, creator);

    // Verify creator invocation paths.
    assertEquals(1, createdInnerIds.size());
    String countryPartitionValue = identity.parse("US");
    assertEquals(
        Collections.singletonList("country=" + countryPartitionValue), createdInnerIds.get(0));

    assertEquals(1, createdLeafIds.size());
    String bucketPartitionValue = bucketDesc.getPartitionFunction().parse("user1");
    assertEquals(
        Arrays.asList("country=US", "bucket=" + bucketPartitionValue), createdLeafIds.get(0));

    // Verify tree structure updated.
    assertEquals(1, root.childrenOrEmpty().size());
    Partition countryPartition = root.childrenOrEmpty().get(0);
    assertEquals("country", countryPartition.columnName());
    assertEquals("US", countryPartition.value());

    assertEquals(1, countryPartition.childrenOrEmpty().size());
    Partition leafPartition = countryPartition.childrenOrEmpty().get(0);
    assertSame(leaf, leafPartition);
    assertTrue(leafPartition.isLeaf());
    assertEquals("bucket", leafPartition.columnName());

    LanceDataset createdDataset = leafPartition.dataset();
    assertNotNull(createdDataset);
    assertEquals(
        String.format("file:///tmp/db/country=US/bucket=%s.lance", bucketPartitionValue),
        createdDataset.readOptions().getDatasetUri());
  }

  @Test
  public void testResolveOrCreateOnLeafThrowsError() {
    PartitionFunction identity = new PartitionFunction.IdentityPartition();
    LanceDataset ds = newDataset("us");
    Partition leaf = Partition.leaf("country", identity.parse("US"), identity, ds);

    PartitionDescriptor pd = PartitionDescriptor.of("country", "IDENTITY", Collections.emptyMap());

    PartitionKey key =
        PartitionKey.of(Collections.singletonList(pd), Collections.singletonList("US"));

    PartitionCreator creator =
        new PartitionCreator() {
          @Override
          public LanceDataset createLeafDataset(List<String> id) {
            return null;
          }

          @Override
          public void createInnerNamespace(List<String> id) {}
        };

    assertThrows(IllegalArgumentException.class, () -> leaf.resolveOrCreate(key, creator));
  }

  private Partition preparePartition() {
    PartitionFunction identity = new PartitionFunction.IdentityPartition();
    PartitionFunction bucket = new PartitionFunction.BucketPartition(10);

    LanceDataset dsUsBucket1 = newDataset("us_bucket1");
    LanceDataset dsUsBucket2 = newDataset("us_bucket2");
    LanceDataset dsCaBucket1 = newDataset("ca_bucket1");

    Partition leafUs1 = Partition.leaf("bucket", bucket.parse("user1"), bucket, dsUsBucket1);
    Partition leafUs2 = Partition.leaf("bucket", bucket.parse("user2"), bucket, dsUsBucket2);
    Partition leafCa1 = Partition.leaf("bucket", bucket.parse("user3"), bucket, dsCaBucket1);

    List<Partition> usBuckets = new ArrayList<>();
    usBuckets.add(leafUs1);
    usBuckets.add(leafUs2);
    Partition countryUs = Partition.inner("country", identity.parse("US"), identity, usBuckets);

    List<Partition> caBuckets = new ArrayList<>();
    caBuckets.add(leafCa1);
    Partition countryCa = Partition.inner("country", identity.parse("CA"), identity, caBuckets);

    List<Partition> roots = new ArrayList<>();
    roots.add(countryUs);
    roots.add(countryCa);
    Partition root = Partition.inner("root", "rootValue", identity, roots);

    return root;
  }

  @Test
  public void testFilterEqualTo() {
    Partition root = preparePartition();

    // 1. Attribute matches leaf, value mismatched -> empty list.
    List<LanceDataset> result = root.filter(new EqualTo("bucket", "nonExistingRaw"));
    assertTrue(result.isEmpty());

    // 2. Attribute matches leaf, value matches -> single dataset.
    result = root.filter(new EqualTo("bucket", "user1"));
    assertEquals(1, result.size());
    assertEquals("us_bucket1", result.get(0).name());

    // 3. Attribute matches inner node; matching value returns all leaves under that node.
    result = root.filter(new EqualTo("country", "US"));
    assertEquals(2, result.size());
    Set<String> set = result.stream().map(d -> d.name()).collect(Collectors.toSet());
    assertTrue(set.contains("us_bucket1"));
    assertTrue(set.contains("us_bucket2"));
  }

  @Test
  public void testFilterIn() {
    Partition root = preparePartition();

    // 1. Attribute matches leaf, value mismatched.
    Object[] values = new Object[2];
    values[0] = "user5";
    values[1] = "user10";
    List<LanceDataset> result = root.filter(new In("bucket", values));
    assertEquals(0, result.size());

    // 2. Attribute matches leaf, value matched.
    values[0] = "user1";
    result = root.filter(new In("bucket", values));
    assertEquals(1, result.size());

    // 3. Attribute matches inner node; matching value returns all leaves under that node.
    result = root.filter(new In("country", new Object[] {"US", "CA"}));
    assertEquals(3, result.size());
    Set<String> set = result.stream().map(d -> d.name()).collect(Collectors.toSet());
    assertTrue(set.contains("us_bucket1"));
    assertTrue(set.contains("us_bucket2"));
    assertTrue(set.contains("ca_bucket1"));
  }
}
