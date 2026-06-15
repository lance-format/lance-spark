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
import org.lance.spark.utils.BlobSourceContext;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.write.SparkPositionDeltaWriteBuilder;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.SupportsDelta;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
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

  private final boolean managedVersioning;

  private final String fileFormatVersion;

  private final Map<String, String> tableProperties;

  private final Map<String, BlobSourceContext> blobSourceContexts;

  public LancePositionDeltaOperation(
      Command command,
      StructType sparkSchema,
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      boolean managedVersioning,
      String fileFormatVersion,
      Map<String, String> tableProperties) {
    this(
        command,
        sparkSchema,
        readOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        managedVersioning,
        fileFormatVersion,
        tableProperties,
        Collections.emptyMap());
  }

  private LancePositionDeltaOperation(
      Command command,
      StructType sparkSchema,
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      boolean managedVersioning,
      String fileFormatVersion,
      Map<String, String> tableProperties,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this.command = command;
    this.sparkSchema = sparkSchema;
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.managedVersioning = managedVersioning;
    this.fileFormatVersion = fileFormatVersion;
    this.tableProperties = tableProperties;
    this.blobSourceContexts = blobSourceContexts;
  }

  public LancePositionDeltaOperation withBlobSourceContexts(
      Map<String, BlobSourceContext> contexts) {
    return new LancePositionDeltaOperation(
        command,
        sparkSchema,
        readOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        managedVersioning,
        fileFormatVersion,
        tableProperties,
        contexts);
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
    rejectBlobV2Descriptors(logicalWriteInfo.schema());
    LanceSparkWriteOptions.Builder writeOptionsBuilder =
        LanceSparkWriteOptions.builder()
            .datasetUri(readOptions.getDatasetUri())
            .storageOptions(readOptions.getStorageOptions())
            .namespace(readOptions.getNamespace())
            .tableId(readOptions.getTableId());
    if (fileFormatVersion != null) {
      writeOptionsBuilder.fileFormatVersion(fileFormatVersion);
    }
    if (tableProperties != null) {
      String stableRowIds =
          tableProperties.get(LanceSparkCatalogConfig.TABLE_OPT_ENABLE_STABLE_ROW_IDS);
      if (stableRowIds != null) {
        writeOptionsBuilder.enableStableRowIds(Boolean.parseBoolean(stableRowIds));
      }
    }
    LanceSparkWriteOptions writeOptions = writeOptionsBuilder.build();
    Map<String, BlobSourceContext> contexts =
        blobSourceContexts.isEmpty()
            ? BlobSourceContext.decodeFromWriteOptions(logicalWriteInfo.options())
            : blobSourceContexts;
    return new SparkPositionDeltaWriteBuilder(
        sparkSchema,
        writeOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        managedVersioning,
        readOptions.getTableId(),
        contexts);
  }

  /**
   * A blob v2 column accepts only BINARY on write. If a descriptor struct reaches the delta writer
   * it would serialize as opaque bytes and silently corrupt the column, so any row-level plan the
   * copy-through rewrite could not convert to copy tokens must fail here.
   */
  private void rejectBlobV2Descriptors(StructType writeSchema) {
    for (StructField tableField : sparkSchema.fields()) {
      if (!BlobUtils.isBlobV2SparkField(tableField)) {
        continue;
      }
      for (StructField writeField : writeSchema.fields()) {
        if (writeField.name().equals(tableField.name())
            && writeField.dataType() instanceof StructType) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot run this MERGE/UPDATE on Lance table %s: blob v2 column '%s' would "
                      + "receive descriptor structs instead of binary data. Assign the column "
                      + "directly from a blob v2 source column (deep copy) or omit it from the "
                      + "command",
                  readOptions.getDatasetUri(), tableField.name()));
        }
      }
    }
  }

  @Override
  public NamedReference[] rowId() {
    NamedReference rowAddr = Expressions.column(LanceConstant.ROW_ADDRESS);
    NamedReference rowId = Expressions.column(LanceConstant.ROW_ID);
    return new NamedReference[] {rowAddr, rowId};
  }

  @Override
  public NamedReference[] requiredMetadataAttributes() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    return new NamedReference[] {segmentId};
  }

  @Override
  public boolean representUpdateAsDeleteAndInsert() {
    return command != Command.UPDATE;
  }

  Map<String, BlobSourceContext> blobSourceContexts() {
    return blobSourceContexts;
  }
}
