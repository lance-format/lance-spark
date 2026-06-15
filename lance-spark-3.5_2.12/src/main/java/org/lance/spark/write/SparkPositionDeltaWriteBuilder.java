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
package org.lance.spark.write;

import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.BlobSourceContext;

import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class SparkPositionDeltaWriteBuilder implements DeltaWriteBuilder {
  private final StructType sparkSchema;
  private final LanceSparkWriteOptions writeOptions;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;
  private final boolean managedVersioning;

  /** Blob source contexts for resolving copy tokens, keyed by source dataset URI. */
  private final Map<String, BlobSourceContext> blobSourceContexts;

  public SparkPositionDeltaWriteBuilder(
      StructType sparkSchema,
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      boolean managedVersioning,
      List<String> tableId,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this.sparkSchema = sparkSchema;
    this.writeOptions = writeOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.managedVersioning = managedVersioning;
    this.tableId = tableId;
    this.blobSourceContexts = blobSourceContexts;
  }

  public DeltaWrite build() {
    return new SparkPositionDeltaWrite(
        sparkSchema,
        writeOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        managedVersioning,
        tableId,
        blobSourceContexts);
  }
}
