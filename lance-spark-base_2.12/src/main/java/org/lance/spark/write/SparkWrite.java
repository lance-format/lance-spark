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

import org.lance.WriteParams;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Spark write implementation for Lance tables.
 *
 * <p>When the table property {@code lance.partition.columns} is set, this write requires Spark to
 * cluster (partition) the input data by those columns before writing. This ensures each Lance
 * fragment contains exactly one distinct value for the partition column(s), which is the
 * prerequisite for Storage-Partitioned Joins (SPJ) on the read path.
 */
public class SparkWrite implements Write, RequiresDistributionAndOrdering {
  private final LanceSparkWriteOptions writeOptions;
  private final StructType schema;

  /** Returns the write options used by this SparkWrite. Visible for testing. */
  LanceSparkWriteOptions getWriteOptions() {
    return writeOptions;
  }

  private final boolean overwrite;

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
  private final StagedCommit stagedCommit;
  private final Map<String, String> tableProperties;

  SparkWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean managedVersioning,
      StagedCommit stagedCommit,
      Map<String, String> tableProperties) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.stagedCommit = stagedCommit;
    this.tableProperties =
        tableProperties != null
            ? Collections.unmodifiableMap(tableProperties)
            : Collections.emptyMap();
  }

  /** Returns partition column names from the table property, empty list if unset. */
  private List<String> partitionColumnList() {
    String raw = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
    if (raw == null || raw.trim().isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  @Override
  public Distribution requiredDistribution() {
    List<String> cols = partitionColumnList();
    if (cols.isEmpty()) {
      return Distributions.unspecified();
    }
    NamedReference[] refs = cols.stream().map(Expressions::column).toArray(NamedReference[]::new);
    return Distributions.clustered(refs);
  }

  @Override
  public SortOrder[] requiredOrdering() {
    List<String> cols = partitionColumnList();
    if (cols.isEmpty()) {
      return new SortOrder[0];
    }
    return cols.stream()
        .map(
            col ->
                Expressions.sort(
                    Expressions.column(col), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
        .toArray(SortOrder[]::new);
  }

  @Override
  public BatchWrite toBatch() {
    return new LanceBatchWrite(
        schema,
        writeOptions,
        overwrite,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId,
        managedVersioning,
        stagedCommit,
        partitionColumnList());
  }

  @Override
  public StreamingWrite toStreaming() {
    throw new UnsupportedOperationException();
  }

  /** Spark write builder. */
  public static class SparkWriteBuilder implements SupportsTruncate, WriteBuilder {
    private final LanceSparkWriteOptions writeOptions;
    private final StructType schema;
    private boolean overwrite = false;
    private StagedCommit stagedCommit;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;
    private final boolean managedVersioning;
    private final Map<String, String> tableProperties;

    public SparkWriteBuilder(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        boolean managedVersioning,
        Map<String, String> tableProperties) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.managedVersioning = managedVersioning;
      this.tableProperties = tableProperties;
    }

    public void setStagedCommit(StagedCommit stagedCommit) {
      this.stagedCommit = stagedCommit;
    }

    @Override
    public Write build() {
      LanceSparkWriteOptions options =
          !overwrite
              ? writeOptions
              : LanceSparkWriteOptions.builder()
                  .storageOptions(writeOptions.getStorageOptions())
                  .namespace(writeOptions.getNamespace())
                  .tableId(writeOptions.getTableId())
                  .batchSize(writeOptions.getBatchSize())
                  .datasetUri(writeOptions.getDatasetUri())
                  .fileFormatVersion(writeOptions.getFileFormatVersion())
                  .maxBytesPerFile(writeOptions.getMaxBytesPerFile())
                  .maxRowsPerFile(writeOptions.getMaxRowsPerFile())
                  .maxRowsPerGroup(writeOptions.getMaxRowsPerGroup())
                  .queueDepth(writeOptions.getQueueDepth())
                  .useQueuedWriteBuffer(writeOptions.isUseQueuedWriteBuffer())
                  .useLargeVarTypes(writeOptions.isUseLargeVarTypes())
                  .enableStableRowIds(writeOptions.getEnableStableRowIds())
                  .writeMode(WriteParams.WriteMode.OVERWRITE)
                  .build();

      return new SparkWrite(
          schema,
          options,
          overwrite,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId,
          managedVersioning,
          stagedCommit,
          tableProperties);
    }

    @Override
    public WriteBuilder truncate() {
      this.overwrite = true;
      return this;
    }
  }
}
