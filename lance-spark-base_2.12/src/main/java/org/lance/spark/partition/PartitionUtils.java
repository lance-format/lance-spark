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

import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.MemoryLanceNamespace;
import org.lance.spark.internal.LanceDatasetAdapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.lance.spark.partition.LancePartitionConfig.TABLE_ROOT;
import static org.lance.spark.partition.PartitionedLanceDataset.LANCE_PARTITION_COLUMNS;
import static org.lance.spark.partition.PartitionedLanceDataset.LANCE_PARTITION_SCHEMA;

public class PartitionUtils {
  public static final String LANCE_PARTITION_TABLE_ROOT = "lance.partitioning.table-root";

  /** Verify whether the input id is a partitioned table. */
  public static boolean isPartitionTable(LanceNamespace namespace, String impl, Identifier id) {
    try {
      Map<String, String> tableProps;
      if (impl.equals("dir") || impl.equals(MemoryLanceNamespace.SCHEME)) {
        tableProps = namespaceProperties(namespace, id);
      } else if (impl.equals("hive3") || impl.equals("hive2")) {
        tableProps = tableProperties(namespace, id);
      } else {
        throw new IllegalArgumentException(
            "Unsupported namespace type for partition table: " + namespace.getClass());
      }
      return tableProps.containsKey(LANCE_PARTITION_TABLE_ROOT);
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /** Create {@link Partition} based on the root namespace and partition descriptors. */
  public static Partition loadPartition(
      LanceNamespace namespace,
      List<PartitionDescriptor> partitionDescriptors,
      StructType sparkSchema,
      Map<String, String> properties)
      throws NoSuchTableException {
    List<PartitionDescriptor> pds = new ArrayList<>();
    pds.add(PartitionDescriptor.root());
    pds.addAll(partitionDescriptors);

    List<String> rootId = identToId(PartitionedLanceDataset.ROOT_ID);

    return innerLoadPartition(namespace, rootId, pds, sparkSchema, properties);
  }

  private static Partition innerLoadPartition(
      LanceNamespace namespace,
      List<String> id,
      List<PartitionDescriptor> partitionDescriptors,
      StructType sparkSchema,
      Map<String, String> properties)
      throws NoSuchTableException {
    PartitionDescriptor partitionDescriptor = partitionDescriptors.get(0);
    if (partitionDescriptors.size() == 1) {
      // Leaf Partition
      String columnName = partitionDescriptor.getName();
      String value = normalizePartitionValue(columnName, id);
      PartitionFunction func = partitionDescriptor.getPartitionFunction();

      // Construct dataset, using storage properties and schema from partitioned table,
      // not physical table.
      String location = loadDatasetLocation(namespace, id);
      LanceSparkReadOptions readOptions =
          LanceSparkReadOptions.builder().fromOptions(properties).datasetUri(location).build();
      LanceDataset ds = new LanceDataset(readOptions, sparkSchema);

      return Partition.leaf(columnName, value, func, ds);
    } else if (partitionDescriptors.size() == 2) {
      // Parent of leaf partition
      ListTablesRequest req = new ListTablesRequest();
      req.id(id);
      ListTablesResponse resp = namespace.listTables(req);

      List<Partition> children = Lists.newArrayList();
      for (String name : resp.getTables()) {
        List<String> subId = new ArrayList<>(id);
        subId.add(name);
        Partition partition =
            innerLoadPartition(
                namespace,
                subId,
                partitionDescriptors.subList(1, partitionDescriptors.size()),
                sparkSchema,
                properties);
        children.add(partition);
      }

      String columnName = partitionDescriptor.getName();
      String value = normalizePartitionValue(columnName, id);
      PartitionFunction func = partitionDescriptor.getPartitionFunction();
      return Partition.inner(columnName, value, func, children);
    } else {
      ListNamespacesRequest req = new ListNamespacesRequest();
      req.id(id);
      ListNamespacesResponse resp = namespace.listNamespaces(req);

      List<Partition> children = Lists.newArrayList();
      for (String name : resp.getNamespaces()) {
        List<String> subId = new ArrayList<>(id);
        subId.add(name);
        Partition partition =
            innerLoadPartition(
                namespace,
                subId,
                partitionDescriptors.subList(1, partitionDescriptors.size()),
                sparkSchema,
                properties);
        children.add(partition);
      }

      String columnName = partitionDescriptor.getName();
      String value = normalizePartitionValue(columnName, id);
      PartitionFunction func = partitionDescriptor.getPartitionFunction();
      return Partition.inner(columnName, value, func, children);
    }
  }

  public static String partitionValueWithCol(String colName, String rawValue) {
    if (colName.equals("root")) {
      return rawValue;
    }
    if (rawValue.startsWith(colName + "=")) {
      return rawValue;
    }
    return colName + "=" + rawValue;
  }

  public static String normalizePartitionValue(String colName, List<String> id) {
    String rawValue = id == null || id.isEmpty() ? "root" : id.get(id.size() - 1);
    if (rawValue.startsWith(colName + "=")) {
      return rawValue.substring(colName.length() + 1);
    }
    return rawValue;
  }

  private static String loadDatasetLocation(LanceNamespace namespace, List<String> id)
      throws NoSuchTableException {
    DescribeTableRequest request = new DescribeTableRequest();
    request.id(id);
    DescribeTableResponse response;
    try {
      response = namespace.describeTable(request);
    } catch (Exception e) {
      // TODO: shall we always throw NoSuchTableException?
      throw new NoSuchTableException(id.toString(), id.toString());
    }
    return response.getLocation();
  }

  /** Transform spark partitions into {@link PartitionDescriptor} list. */
  public static List<PartitionDescriptor> buildPartitionDescriptors(Transform[] partitions) {
    List<PartitionDescriptor> partitionDescriptors = new ArrayList<>();

    for (Transform partition : partitions) {
      // Parse different partition types
      if (partition instanceof IdentityTransform) {
        IdentityTransform identityTransform = (IdentityTransform) partition;
        String name = identityTransform.ref().fieldNames()[0];
        partitionDescriptors.add(PartitionDescriptor.of(name, "identity", new HashMap<>()));
      } else if (partition instanceof BucketTransform) {
        BucketTransform bucketTransform = (BucketTransform) partition;
        List<NamedReference> nRefs =
            JavaConverters.seqAsJavaListConverter(bucketTransform.columns().toSeq()).asJava();
        String name = nRefs.get(0).fieldNames()[0];
        Map<String, String> props = new HashMap<>();
        props.put("num-buckets", String.valueOf(bucketTransform.numBuckets()));
        partitionDescriptors.add(PartitionDescriptor.of(name, "bucket", props));
      } else {
        throw new IllegalArgumentException("Unknown partition type: " + partition.getClass());
      }
    }

    return partitionDescriptors;
  }

  /** Get namespace properties of the input identifier */
  public static Map<String, String> tableProperties(LanceNamespace namespace, Identifier id)
      throws NoSuchTableException {
    DescribeTableRequest request = new DescribeTableRequest();
    request.id(identToId(id));

    Map<String, String> properties;
    try {
      DescribeTableResponse response = namespace.describeTable(request);
      properties = response.getStorageOptions();
      properties = properties == null ? Collections.emptyMap() : properties;
    } catch (Exception e) {
      throw new NoSuchTableException(id);
    }

    return properties;
  }

  /** Get namespace properties of the input identifier */
  public static Map<String, String> namespaceProperties(LanceNamespace namespace, Identifier id)
      throws NoSuchTableException {
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.id(identToId(id));

    Map<String, String> properties;
    try {
      DescribeNamespaceResponse response = namespace.describeNamespace(request);
      properties = response.getProperties();
      properties = properties == null ? Collections.emptyMap() : properties;
    } catch (Exception e) {
      throw new NoSuchTableException(id);
    }

    return properties;
  }

  public static List<String> identToId(Identifier ident) {
    List<String> ids = new ArrayList<>();
    ids.addAll(Arrays.stream(ident.namespace()).collect(Collectors.toList()));
    ids.add(ident.name());
    return ids;
  }

  public static LanceNamespace createNamespace(Map<String, String> options) {
    String location = options.get(TABLE_ROOT);
    return createNamespace(location, options);
  }

  public static LanceNamespace createNamespace(String location, Map<String, String> options) {
    if (location.startsWith(MemoryLanceNamespace.SCHEME)) {
      return MemoryLanceNamespace.create(location, options);
    } else {
      LanceNamespace ns = new DirectoryNamespace();
      ns.initialize(options, LanceDatasetAdapter.allocator);
      return ns;
    }
  }

  public static PartitionedLanceDataset createEmptyPartitionTable(
      String tableName,
      String location,
      StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> properties)
      throws NoSuchTableException {
    Map<String, String> options = new HashMap<>();
    if (properties != null) {
      options.putAll(properties);
    }
    options.put(TABLE_ROOT, location);

    // Set schema.
    Schema schema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTF", false, false);
    try {
      String schemaJson = JsonUtils.encodeArrowSchema(schema);
      options.put(LANCE_PARTITION_SCHEMA, schemaJson);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to encode schema to json", e);
    }

    // Set partition descriptors.
    List<PartitionDescriptor> partitionDescriptors =
        PartitionUtils.buildPartitionDescriptors(partitions);
    try {
      options.put(
          LANCE_PARTITION_COLUMNS, JsonUtils.encodePartitionDescriptors(partitionDescriptors));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize partition definitions", e);
    }

    // Create table root namespace with properties.
    LanceNamespace namespace = PartitionUtils.createNamespace(location, new HashMap<>());

    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.id(identToId(PartitionedLanceDataset.ROOT_ID));
    request.setProperties(options);
    namespace.createNamespace(request);

    // Load new table.
    LancePartitionConfig config = LancePartitionConfig.from(options, tableName, location);
    return new PartitionedLanceDataset(config);
  }

  public static boolean isPruneAvailable(List<PartitionDescriptor> partitionDescriptors, Filter f) {
    for (PartitionDescriptor pd : partitionDescriptors) {
      if (pd.isPruneAvailable(f)) {
        return true;
      }
    }
    return false;
  }
}
