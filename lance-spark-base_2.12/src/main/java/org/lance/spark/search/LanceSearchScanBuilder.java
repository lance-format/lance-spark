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

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class LanceSearchScanBuilder implements ScanBuilder {
  private final StructType schema;
  private final LanceSearchQuery query;
  private final boolean distributed;
  private final LanceSparkReadOptions readOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final Map<String, String> initialStorageOptions;

  public LanceSearchScanBuilder(
      StructType schema,
      LanceSearchQuery query,
      boolean distributed,
      LanceSparkReadOptions readOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      Map<String, String> initialStorageOptions) {
    this.schema = schema;
    this.query = query;
    this.distributed = distributed;
    this.readOptions = readOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.initialStorageOptions = initialStorageOptions;
  }

  @Override
  public Scan build() {
    return new LanceSearchScan(
        schema,
        query,
        distributed,
        readOptions,
        namespaceImpl,
        namespaceProperties,
        initialStorageOptions);
  }
}
