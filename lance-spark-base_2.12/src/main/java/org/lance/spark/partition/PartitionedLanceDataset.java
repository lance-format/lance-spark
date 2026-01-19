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
import org.lance.spark.read.PartitionedLanceScanBuilder;
import org.lance.spark.write.PartitionedWrite;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.lance.spark.partition.JsonUtils.decodeJsonArrowSchema;

public class PartitionedLanceDataset
    implements SupportsRead, SupportsWrite, SupportsMetadataColumns {

  public static final String LANCE_PARTITION_COLUMNS = "lance.partitioning.partition-columns";
  public static final String LANCE_PARTITION_SCHEMA = "lance.partitioning.schema";
  public static final Identifier ROOT_ID = Identifier.of(new String[0], "root");

  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(
          TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE);

  private final LancePartitionConfig config;
  private final LanceNamespace namespace;
  private final List<PartitionDescriptor> partitionDescriptors;
  private final StructType sparkSchema;
  private final Partition root;

  public PartitionedLanceDataset(LancePartitionConfig config) throws NoSuchTableException {
    this.namespace = PartitionUtils.createNamespace(config.options());

    Map<String, String> props = PartitionUtils.namespaceProperties(namespace, ROOT_ID);
    props.putAll(config.options());
    this.config = LancePartitionConfig.create(props);

    this.partitionDescriptors = parsePartitionDescriptors(this.config.options());
    this.sparkSchema = parsePartitionSchema(this.config.options());
    this.root =
        PartitionUtils.loadPartition(
            namespace, partitionDescriptors, sparkSchema, this.config.options());
  }

  private List<PartitionDescriptor> parsePartitionDescriptors(Map<String, String> options) {
    String partitionColumnsJson = options.get(LANCE_PARTITION_COLUMNS);
    Preconditions.checkArgument(partitionColumnsJson != null && !partitionColumnsJson.isEmpty());
    try {
      return JsonUtils.decodePartitionDescriptors(partitionColumnsJson);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Invalid partition columns json: " + partitionColumnsJson, e);
    }
  }

  private StructType parsePartitionSchema(Map<String, String> options) {
    String partitionSchemaJson = options.get(LANCE_PARTITION_SCHEMA);
    Preconditions.checkArgument(partitionSchemaJson != null && !partitionSchemaJson.isEmpty());
    try {
      Schema schema = decodeJsonArrowSchema(partitionSchemaJson);
      return LanceArrowUtils.fromArrowSchema(schema);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Invalid partition schema json: " + partitionSchemaJson, e);
    }
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return new MetadataColumn[0];
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new PartitionedLanceScanBuilder(sparkSchema, partitionDescriptors, config);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new PartitionedWrite.PartitionedWriteBuilder(config, sparkSchema);
  }

  @Override
  public Transform[] partitioning() {
    Transform[] transforms = new Transform[partitionDescriptors.size()];

    for (int i = 0; i < partitionDescriptors.size(); i++) {
      PartitionDescriptor pd = partitionDescriptors.get(i);
      String function = pd.getFunction();

      if ("identity".equals(function)) {
        transforms[i] = Expressions.identity(pd.getName());
      } else if ("bucket".equals(function)) {
        int numBuckets = Integer.parseInt(pd.getProperties().get("num-buckets"));
        transforms[i] = Expressions.bucket(numBuckets, pd.getName());
      } else {
        throw new IllegalArgumentException("Unknown partition function: " + function);
      }
    }

    return transforms;
  }

  @Override
  public String name() {
    return config.tableName();
  }

  @Override
  public StructType schema() {
    return sparkSchema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  public int partitionLevels() {
    return partitionDescriptors.size();
  }

  public PartitionDescriptor partitionDescriptor(int index) {
    return partitionDescriptors.get(index);
  }

  public List<PartitionDescriptor> partitionDescriptors() {
    return partitionDescriptors;
  }

  public LancePartitionConfig config() {
    return config;
  }

  public Partition root() {
    return root;
  }

  /** Resolve physical partition from input partition values, create if not exists */
  public Partition resolveOrCreatePartition(List<String> rawPartition) {
    List<PartitionDescriptor> descriptors = new ArrayList<>(rawPartition.size() + 1);
    descriptors.add(PartitionDescriptor.root());
    descriptors.addAll(partitionDescriptors.subList(0, rawPartition.size()));

    List<String> rawValues = new ArrayList<>(rawPartition.size() + 1);
    rawValues.add("root");
    rawValues.addAll(rawPartition);

    PartitionKey key = PartitionKey.of(descriptors, rawValues);
    return root.resolveOrCreate(key, new PartitionCreatorImpl(config, namespace, sparkSchema));
  }

  /** Resolve raw partition values */
  public List<String> resolvePartitionValues(InternalRow record) {
    List<String> rawPartition = Lists.newArrayList();
    for (int i = 0; i < partitionLevels(); i++) {
      PartitionDescriptor pd = partitionDescriptor(i);

      int idx = sparkSchema.fieldIndex(pd.getName());
      DataType dt = sparkSchema.fields()[idx].dataType();

      Object obj = record.get(idx, dt);
      String raw = String.valueOf(obj);

      rawPartition.add(raw);
    }

    return rawPartition;
  }
}
