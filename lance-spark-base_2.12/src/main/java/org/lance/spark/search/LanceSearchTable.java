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
package org.lance.spark.search;

import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class LanceSearchTable implements SupportsRead {
  private final String name;
  private final StructType schema;
  private final LanceSearchQuery query;
  private final boolean distributed;
  private final LanceSparkReadOptions readOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final Map<String, String> initialStorageOptions;

  public LanceSearchTable(
      String name,
      StructType schema,
      LanceSearchQuery query,
      boolean distributed,
      LanceSparkReadOptions readOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      Map<String, String> initialStorageOptions) {
    this.name = name;
    this.schema = schema;
    this.query = query;
    this.distributed = distributed;
    if (distributed) {
      this.readOptions = Objects.requireNonNull(readOptions, "readOptions");
      this.namespaceImpl = Objects.requireNonNull(namespaceImpl, "namespaceImpl");
      this.namespaceProperties =
          namespaceProperties == null ? Collections.emptyMap() : namespaceProperties;
      this.initialStorageOptions =
          initialStorageOptions == null ? Collections.emptyMap() : initialStorageOptions;
    } else {
      this.readOptions = null;
      this.namespaceImpl = null;
      this.namespaceProperties = Collections.emptyMap();
      this.initialStorageOptions = Collections.emptyMap();
    }
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new LanceSearchScanBuilder(
        schema,
        query,
        distributed,
        readOptions,
        namespaceImpl,
        namespaceProperties,
        initialStorageOptions);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }
}
