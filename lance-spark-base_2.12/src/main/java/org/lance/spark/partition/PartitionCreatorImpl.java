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

import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.LanceDatasetAdapter;
import org.lance.spark.utils.SchemaConverter;

import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.lance.spark.LanceSparkReadOptions.CONFIG_DATASET_URI;

public class PartitionCreatorImpl implements PartitionCreator {
  private static final Logger logger = LoggerFactory.getLogger(PartitionCreatorImpl.class);

  private final LancePartitionConfig config;
  private final LanceNamespace namespace;
  private final StructType sparkSchema;

  public PartitionCreatorImpl(
      LancePartitionConfig config, LanceNamespace namespace, StructType sparkSchema) {
    this.config = config;
    this.namespace = namespace;
    this.sparkSchema = sparkSchema;
  }

  @Override
  public LanceDataset createLeafDataset(List<String> id) {
    Supplier<Map<String, String>> describe =
        () -> {
          DescribeTableRequest request = new DescribeTableRequest();
          request.id(id);
          DescribeTableResponse response = namespace.describeTable(request);
          String location = response.getLocation();
          Map<String, String> props = response.getStorageOptions();
          props.put(CONFIG_DATASET_URI, location);
          return props;
        };

    Supplier<Map<String, String>> create =
        () -> {
          CreateEmptyTableRequest createRequest = new CreateEmptyTableRequest();
          createRequest.id(id);
          createRequest.setProperties(config.options());
          CreateEmptyTableResponse response = namespace.createEmptyTable(createRequest);
          String location = response.getLocation();
          Map<String, String> props = response.getStorageOptions();
          props.put(CONFIG_DATASET_URI, location);
          return props;
        };

    Map<String, String> options = findOrCreate(describe, create, id);
    String location = options.get(CONFIG_DATASET_URI);

    StructType processedSchema = SchemaConverter.processSchemaWithProperties(sparkSchema, options);
    if (LanceDatasetAdapter.getSchema(location).isEmpty()) {
      // Create lance dataset
      WriteParams.Builder writeParams = new WriteParams.Builder();
      writeParams.withStorageOptions(options);
      LanceDatasetAdapter.createDataset(location, processedSchema, writeParams.build());
    }

    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder().fromOptions(options).datasetUri(location).build();
    LanceDataset dataset = new LanceDataset(readOptions, processedSchema);
    return dataset;
  }

  @Override
  public void createInnerNamespace(List<String> id) {
    Supplier<Map<String, String>> describe =
        () -> {
          DescribeNamespaceRequest request = new DescribeNamespaceRequest();
          request.id(id);
          DescribeNamespaceResponse resp = namespace.describeNamespace(request);
          return resp.getProperties();
        };

    Supplier<Map<String, String>> create =
        () -> {
          CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
          createRequest.id(id);
          createRequest.setProperties(config.options());
          CreateNamespaceResponse resp = namespace.createNamespace(createRequest);
          return resp.getProperties();
        };

    findOrCreate(describe, create, id);
  }

  private Map<String, String> findOrCreate(
      Supplier<Map<String, String>> describe,
      Supplier<Map<String, String>> create,
      List<String> id) {
    try {
      return describe.get();
    } catch (Exception e) {
      logger.debug("Error when describing partition {}, may be there is some conflicts.", id, e);
    }

    try {
      return create.get();
    } catch (Exception e) {
      logger.debug("Error when creating partition {}, may be there is some conflicts.", id, e);
      return describe.get();
    }
  }
}
