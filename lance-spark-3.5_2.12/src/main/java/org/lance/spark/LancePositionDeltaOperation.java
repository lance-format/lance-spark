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

import org.lance.spark.read.LanceScanBuilder;
import org.lance.spark.write.SparkPositionDeltaWriteBuilder;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.SupportsDelta;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class LancePositionDeltaOperation implements RowLevelOperation, SupportsDelta {
  private final Command command;
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

  public LancePositionDeltaOperation(
      Command command,
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
  public Command command() {
    return command;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
    return new LanceScanBuilder(
        sparkSchema, readOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
  }

  @Override
  public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    // Create write options from read options for delta operations
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(readOptions.getDatasetUri())
            .storageOptions(readOptions.getStorageOptions())
            .namespace(readOptions.getNamespace())
            .tableId(readOptions.getTableId())
            .build();
    return new SparkPositionDeltaWriteBuilder(
        sparkSchema,
        writeOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        readOptions.getTableId());
  }

  @Override
  public NamedReference[] rowId() {
    NamedReference rowAddr = Expressions.column(LanceConstant.ROW_ADDRESS);
    return new NamedReference[] {rowAddr};
  }

  @Override
  public NamedReference[] requiredMetadataAttributes() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    return new NamedReference[] {segmentId};
  }

  @Override
  public boolean representUpdateAsDeleteAndInsert() {
    return true;
  }
}
