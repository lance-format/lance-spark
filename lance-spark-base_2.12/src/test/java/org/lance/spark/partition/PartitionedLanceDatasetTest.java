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
import org.lance.spark.MemoryLanceNamespace;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.lance.spark.partition.PartitionUtilsTest.createPartitionTable;
import static org.lance.spark.partition.PartitionedLanceDataset.LANCE_PARTITION_COLUMNS;
import static org.lance.spark.partition.PartitionedLanceDataset.LANCE_PARTITION_SCHEMA;

public class PartitionedLanceDatasetTest {

  private PartitionedLanceDataset createDataset(
      List<PartitionDescriptor> partitionDescriptors, StructType schema) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(LANCE_PARTITION_COLUMNS, JsonUtils.encodePartitionDescriptors(partitionDescriptors));
    props.put(
        LANCE_PARTITION_SCHEMA,
        JsonUtils.encodeArrowSchema(LanceArrowUtils.toArrowSchema(schema, "UTC", false, false)));

    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    MemoryLanceNamespace namespace = MemoryLanceNamespace.create(location, new HashMap<>());
    createPartitionTable(namespace, PartitionedLanceDataset.ROOT_ID, props);

    LancePartitionConfig config =
        LancePartitionConfig.from(new HashMap<>(), "test_table", location);
    return new PartitionedLanceDataset(config);
  }

  private List<PartitionDescriptor> createPartitionDescriptors() {
    List<PartitionDescriptor> descriptors = new ArrayList<>();

    descriptors.add(PartitionDescriptor.of("country", "identity", new HashMap<>()));

    Map<String, String> bucketProps = new HashMap<>();
    bucketProps.put("num-buckets", "16");
    descriptors.add(PartitionDescriptor.of("city", "bucket", bucketProps));

    return descriptors;
  }

  @Test
  public void testResolvePartitionValues() throws Exception {
    List<PartitionDescriptor> descriptors = createPartitionDescriptors();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("root", DataTypes.StringType, false),
              DataTypes.createStructField("country", DataTypes.StringType, false),
              DataTypes.createStructField("city", DataTypes.StringType, false),
              DataTypes.createStructField("value", DataTypes.IntegerType, true)
            });
    PartitionedLanceDataset dataset = createDataset(descriptors, schema);
    InternalRow row = new GenericInternalRow(new Object[] {"rootValue", "US", "NY", 42});
    List<String> rawPartition = dataset.resolvePartitionValues(row);
    assertEquals("US", rawPartition.get(0));
    assertEquals("NY", rawPartition.get(1));
  }

  @Test
  public void testResolvePartitionValuesWithBadRow() throws Exception {
    List<PartitionDescriptor> descriptors = createPartitionDescriptors();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("root", DataTypes.StringType, false),
              DataTypes.createStructField("country", DataTypes.StringType, false),
              DataTypes.createStructField("city", DataTypes.StringType, false)
            });
    PartitionedLanceDataset dataset = createDataset(descriptors, schema);
    InternalRow row = new GenericInternalRow(new Object[] {"rootValue", "NY"});
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> dataset.resolvePartitionValues(row));
  }

  @Test
  public void testResolveOrCreatePartitionCreatesLeaf() throws Exception {
    List<PartitionDescriptor> descriptors = createPartitionDescriptors();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("country", DataTypes.StringType, false),
              DataTypes.createStructField("city", DataTypes.StringType, false)
            });
    PartitionedLanceDataset dataset = createDataset(descriptors, schema);

    Partition rootPartition = dataset.root();
    assertNotNull(rootPartition);
    assertTrue(rootPartition.childrenOrEmpty().isEmpty());

    InternalRow row = new GenericInternalRow(new Object[] {"US", "NY"});
    List<String> rawPartition = dataset.resolvePartitionValues(row);
    Partition partition = dataset.resolveOrCreatePartition(rawPartition);

    // Verify second level partition 'city'
    assertEquals(1, dataset.root().childrenOrEmpty().size());
    assertNotNull(partition);
    assertTrue(partition.isLeaf());
    assertEquals("city", partition.columnName());
    String value = descriptors.get(1).getPartitionFunction().parse("NY");
    assertEquals(value, partition.value());
    LanceDataset leafDataset = partition.dataset();
    assertEquals(schema, leafDataset.schema());
    assertEquals("city=11", leafDataset.name());

    // Verify first level partition 'country'
    partition = dataset.resolveOrCreatePartition(rawPartition.subList(0, rawPartition.size() - 1));
    assertNotNull(partition);
    assertFalse(partition.isLeaf());
    assertEquals(1, partition.childrenOrEmpty().size());
    assertEquals("country", partition.columnName());
    assertEquals("US", partition.value());

    // Second call with the same partition should return the same partition
    Partition second =
        dataset.resolveOrCreatePartition(rawPartition.subList(0, rawPartition.size() - 1));
    assertSame(partition, second);
  }

  @Test
  public void testCreateEmptyTableWithDirectoryNamespace() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("country", DataTypes.StringType, false),
              DataTypes.createStructField("city", DataTypes.StringType, false),
              DataTypes.createStructField("value", DataTypes.IntegerType, true)
            });
    Transform[] partitions =
        new Transform[] {Expressions.identity("country"), Expressions.bucket(16, "city")};
    Map<String, String> properties = new HashMap<>();
    properties.put("foo-key", "foo");

    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());

    // Create dataset
    PartitionedLanceDataset dataset =
        PartitionUtils.createEmptyPartitionTable("table", location, schema, partitions, properties);

    // Load dataset
    LancePartitionConfig config = LancePartitionConfig.from(new HashMap<>(), "table", location);
    PartitionedLanceDataset loadDataset = new PartitionedLanceDataset(config);

    // Verify equals
    assertEquals(dataset.schema(), loadDataset.schema());
    assertEquals(dataset.partitionDescriptors(), loadDataset.partitionDescriptors());
    assertEquals(dataset.config().options(), loadDataset.config().options());
  }
}
