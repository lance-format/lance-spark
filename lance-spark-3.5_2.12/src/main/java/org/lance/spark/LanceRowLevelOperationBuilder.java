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

import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class LanceRowLevelOperationBuilder implements RowLevelOperationBuilder {
  private final RowLevelOperation.Command command;
  private final StructType sparkSchema;
  private final LanceSparkReadOptions readOptions;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;

  public LanceRowLevelOperationBuilder(
      RowLevelOperation.Command command,
      StructType sparkSchema,
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    this.command = command;
    this.sparkSchema = sparkSchema;
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
  }

  @Override
  public RowLevelOperation build() {
    return new LancePositionDeltaOperation(
        command,
        sparkSchema,
        readOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties);
  }
}
