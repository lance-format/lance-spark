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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.spark.MemoryLanceNamespace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.lance.spark.partition.PartitionUtils.LANCE_PARTITION_TABLE_ROOT;
import static org.lance.spark.partition.PartitionUtils.identToId;
import static org.lance.spark.partition.PartitionedLanceDataset.LANCE_PARTITION_COLUMNS;
import static org.lance.spark.partition.PartitionedLanceDataset.LANCE_PARTITION_SCHEMA;
import static org.lance.spark.partition.PartitionedLanceDataset.ROOT_ID;

public class PartitionUtilsTest {
  @Test
  public void testBuildPartitionDescriptors() {
    // Identity and Bucket
    Transform[] transforms =
        new Transform[] {Expressions.identity("country"), Expressions.bucket(10, "bucket")};

    List<PartitionDescriptor> descriptors = PartitionUtils.buildPartitionDescriptors(transforms);

    assertEquals(2, descriptors.size());

    PartitionDescriptor identity = descriptors.get(0);
    assertEquals("country", identity.getName());
    assertEquals("identity", identity.getFunction());
    assertEquals(Collections.emptyMap(), identity.getProperties());

    PartitionDescriptor bucket = descriptors.get(1);
    assertEquals("bucket", bucket.getName());
    assertEquals("bucket", bucket.getFunction());
    Map<String, String> props = bucket.getProperties();
    assertEquals("10", props.get("num-buckets"));
  }

  @Test
  public void testIsPartitionTableBehavior() throws Exception {
    Identifier id = Identifier.of(new String[] {"ns"}, "table");

    Map<String, String> props = new HashMap<>();
    props.put(LANCE_PARTITION_TABLE_ROOT, "memory://");

    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    MemoryLanceNamespace namespace = MemoryLanceNamespace.create(location, new HashMap<>());
    createPartitionTable(namespace, id, props);

    // 1) tag is "true" => current implementation returns false
    assertTrue(PartitionUtils.isPartitionTable(namespace, MemoryLanceNamespace.SCHEME, id));

    // 2) tag absent => treated as true by current implementation
    props.clear();
    namespace.setProperties(identToId(id), props);
    assertFalse(PartitionUtils.isPartitionTable(namespace, MemoryLanceNamespace.SCHEME, id));

    // 3) describeNamespace throws => wrapped as NoSuchTableException and yields false
    namespace.registerErrorFunction("describeNamespace", true);
    assertFalse(PartitionUtils.isPartitionTable(namespace, MemoryLanceNamespace.SCHEME, id));
  }

  @Test
  public void testConstructPartitionedDatasetInvalidPartitionColumnsJsonThrowsRuntimeException()
      throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(LANCE_PARTITION_TABLE_ROOT, "true");
    props.put(LANCE_PARTITION_COLUMNS, "not-json");
    props.put(LANCE_PARTITION_SCHEMA, validArrowSchemaJson());

    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    MemoryLanceNamespace namespace = MemoryLanceNamespace.create(location, props);
    createPartitionTable(namespace, ROOT_ID, props);

    LancePartitionConfig config = LancePartitionConfig.from(props, "table", location);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> new PartitionedLanceDataset(config));
    assertTrue(ex.getMessage().contains("Invalid partition columns json"));
  }

  @Test
  public void testConstructPartitionedDatasetInvalidPartitionSchemaJsonThrowsRuntimeException()
      throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(LANCE_PARTITION_TABLE_ROOT, "true");
    props.put(LANCE_PARTITION_COLUMNS, validPartitionColumnsJson());
    props.put(LANCE_PARTITION_SCHEMA, "not-json");

    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    MemoryLanceNamespace namespace = MemoryLanceNamespace.create(location, props);
    createPartitionTable(namespace, ROOT_ID, props);

    LancePartitionConfig config = LancePartitionConfig.from(props, "table", location);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> new PartitionedLanceDataset(config));
    assertTrue(ex.getMessage().contains("Invalid partition schema json"));
  }

  public static String validArrowSchemaJson() throws JsonProcessingException {
    List<Field> fields = new ArrayList<>();
    fields.add(
        new Field(
            "col",
            FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)),
            null));
    Schema schema = new Schema(fields);
    return JsonUtils.encodeArrowSchema(schema);
  }

  public static String validPartitionColumnsJson() throws JsonProcessingException {
    PartitionDescriptor descriptor =
        PartitionDescriptor.of("country", "identity", Collections.emptyMap());
    List<PartitionDescriptor> list = new ArrayList<>();
    list.add(descriptor);
    return JsonUtils.encodePartitionDescriptors(list);
  }

  public static void createPartitionTable(
      LanceNamespace namespace, Identifier id, Map<String, String> props) {
    CreateNamespaceRequest request;
    List<String> idList = Lists.newArrayList();
    for (String name : id.namespace()) {
      idList.add(name);

      request = new CreateNamespaceRequest();
      request.id(idList);
      namespace.createNamespace(request);
    }

    idList.add(id.name());
    request = new CreateNamespaceRequest();
    request.id(idList);
    request.setProperties(props);
    namespace.createNamespace(request);
  }

  @Test
  public void testLoadPartitionWithMemoryNamespace() throws Exception {
    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    MemoryLanceNamespace namespace = MemoryLanceNamespace.create(location, new HashMap<>());

    // root
    //  - language=english
    //    - country=US
    //    - country=CN
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.id(identToId(ROOT_ID));
    Map<String, String> props = new HashMap<>();
    props.put(LANCE_PARTITION_COLUMNS, validPartitionColumnsJson());
    props.put(LANCE_PARTITION_SCHEMA, validArrowSchemaJson());
    namespace.createNamespace(nsRequest);

    nsRequest = new CreateNamespaceRequest();
    nsRequest.id(Lists.newArrayList("root", "language=english"));
    namespace.createNamespace(nsRequest);

    CreateEmptyTableRequest createUS = new CreateEmptyTableRequest();
    createUS.id(Lists.newArrayList("root", "language=english", "country=US"));
    namespace.createEmptyTable(createUS);
    CreateEmptyTableRequest createCN = new CreateEmptyTableRequest();
    createCN.id(Lists.newArrayList("root", "language=english", "country=CN"));
    namespace.createEmptyTable(createCN);

    // Build partition descriptor for "language" and "country"
    PartitionDescriptor language =
        PartitionDescriptor.of("language", "identity", Collections.emptyMap());

    PartitionDescriptor country =
        PartitionDescriptor.of("country", "identity", Collections.emptyMap());

    List<PartitionDescriptor> descriptors = new ArrayList<>();
    descriptors.add(language);
    descriptors.add(country);

    // Spark schema with language and country
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("language", DataTypes.StringType, true),
              DataTypes.createStructField("country", DataTypes.StringType, true)
            });

    // Verify load partition
    Partition root = PartitionUtils.loadPartition(namespace, descriptors, schema, new HashMap<>());

    assertFalse(root.isLeaf());
    assertEquals("root", root.columnName());
    assertEquals("root", root.value());
    List<Partition> children = root.childrenOrEmpty();
    assertEquals(1, children.size());

    Partition child = children.get(0);
    assertEquals("language", child.columnName());
    assertEquals("english", child.value());
    children = child.childrenOrEmpty();
    assertEquals(2, children.size());

    Set<String> values = new HashSet<>();
    for (Partition par : children) {
      assertTrue(par.isLeaf());
      assertEquals("country", par.columnName());
      String value = par.value();
      values.add(value);
      assertNotNull(par.dataset());
      String uri = par.dataset().readOptions().getDatasetUri();
      assertNotNull(uri);
      assertTrue(uri.endsWith(".lance"));
      assertTrue(uri.contains("country=" + value));
    }
    assertEquals(new HashSet<>(java.util.Arrays.asList("US", "CN")), values);
  }
}
