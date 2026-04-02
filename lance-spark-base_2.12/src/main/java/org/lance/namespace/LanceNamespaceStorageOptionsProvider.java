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
package org.lance.namespace;

import org.lance.io.StorageOptionsProvider;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Resolves storage options from a namespace table for credential vending. */
public class LanceNamespaceStorageOptionsProvider implements StorageOptionsProvider {
  private final LanceNamespace namespace;
  private final List<String> tableId;

  public LanceNamespaceStorageOptionsProvider(LanceNamespace namespace, List<String> tableId) {
    this.namespace = namespace;
    this.tableId = tableId;
  }

  @Override
  public Map<String, String> getStorageOptions() {
    DescribeTableRequest request = new DescribeTableRequest();
    tableId.forEach(request::addIdItem);
    DescribeTableResponse response = namespace.describeTable(request);
    if (response == null || response.getStorageOptions() == null) {
      return Collections.emptyMap();
    }
    return response.getStorageOptions();
  }
}
