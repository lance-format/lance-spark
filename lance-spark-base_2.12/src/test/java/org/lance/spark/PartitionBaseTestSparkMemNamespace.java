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
package org.lance.spark;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.lance.spark.partition.LancePartitionConfig.TABLE_ROOT;

public class PartitionBaseTestSparkMemNamespace extends PartitionSparkLanceNamespaceTestBase {
  @Override
  protected String getNsImpl() {
    return MemoryLanceNamespace.SCHEME;
  }

  @Override
  protected Map<String, String> getAdditionalNsConfigs() {
    Map<String, String> configs = new HashMap<>();
    String location = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    configs.put(TABLE_ROOT, location);

    LanceNamespace namespace = MemoryLanceNamespace.create(location, new HashMap<>());
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.id(Lists.asList("default", new String[0]));
    namespace.createNamespace(request);

    return configs;
  }

  @Override
  protected boolean supportsNamespace() {
    return true;
  }
}
