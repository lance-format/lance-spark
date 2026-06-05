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

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceSchema;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.sharding.SparkLanceShardingUtils;
import org.lance.spark.utils.BlobSourceContext;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Spark write implementation for Lance tables.
 *
 * <p>When a Lance sharding spec is configured, this write asks Spark to cluster input data by the
 * sharding columns before writing and rolls fragments on sharding-key boundaries. This keeps each
 * Lance fragment within one sharding key, which is the prerequisite for Storage-Partitioned Joins
 * (SPJ) on the read path.
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
  private final ShardingSpec initialShardingSpec;
  private ShardingSpec cachedShardingSpec;
  private LanceSchema cachedLanceSchema;
  private boolean cachedShardingSpecLoaded;

  /** Per-source blob credential/open contexts keyed by source dataset URI. */
  private final Map<String, BlobSourceContext> blobSourceContexts;

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
      ShardingSpec initialShardingSpec,
      Map<String, BlobSourceContext> blobSourceContexts) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.stagedCommit = stagedCommit;
    this.initialShardingSpec = initialShardingSpec;
    this.blobSourceContexts =
        blobSourceContexts == null ? Collections.emptyMap() : blobSourceContexts;
  }

  private ShardingSpec shardingSpec() {
    if (!cachedShardingSpecLoaded) {
      if (stagedCommit != null || !SparkLanceShardingUtils.isEmpty(initialShardingSpec)) {
        cachedShardingSpec = initialShardingSpec;
      } else {
        try (Dataset dataset =
            Utils.openDatasetBuilder(writeOptions)
                .initialStorageOptions(initialStorageOptions)
                .runtimeNamespace(namespaceImpl, namespaceProperties, tableId)
                .build()) {
          cachedLanceSchema = dataset.getLanceSchema();
          cachedShardingSpec = SparkLanceShardingUtils.firstShardingSpec(dataset);
        }
      }
      cachedShardingSpecLoaded = true;
    }
    return cachedShardingSpec;
  }

  @Override
  public Distribution requiredDistribution() {
    ShardingSpec spec = shardingSpec();
    if (SparkLanceShardingUtils.isEmpty(spec)) {
      return Distributions.unspecified();
    }
    NamedReference[] refs =
        SparkLanceShardingUtils.fields(spec).stream()
            .map(field -> SparkLanceShardingUtils.toClusteringRef(field, cachedLanceSchema))
            .toArray(NamedReference[]::new);
    return Distributions.clustered(refs);
  }

  @Override
  public SortOrder[] requiredOrdering() {
    ShardingSpec spec = shardingSpec();
    if (SparkLanceShardingUtils.isEmpty(spec)) {
      return new SortOrder[0];
    }
    return SparkLanceShardingUtils.fields(spec).stream()
        .map(field -> SparkLanceShardingUtils.toSortOrder(field, cachedLanceSchema))
        .toArray(SortOrder[]::new);
  }

  @Override
  public BatchWrite toBatch() {
    ShardingSpec spec = shardingSpec();
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
        spec,
        blobSourceContexts);
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
    private final ShardingSpec shardingSpec;
    private final Map<String, BlobSourceContext> blobSourceContexts;

    public SparkWriteBuilder(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        boolean managedVersioning) {
      this(
          schema,
          writeOptions,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId,
          managedVersioning,
          null,
          Collections.emptyMap());
    }

    public SparkWriteBuilder(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        boolean managedVersioning,
        ShardingSpec shardingSpec,
        Map<String, BlobSourceContext> blobSourceContexts) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.managedVersioning = managedVersioning;
      this.shardingSpec = shardingSpec;
      this.blobSourceContexts =
          blobSourceContexts == null ? Collections.emptyMap() : blobSourceContexts;
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
                  .branchName(writeOptions.getBranchName())
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
          shardingSpec,
          blobSourceContexts);
    }

    @Override
    public WriteBuilder truncate() {
      this.overwrite = true;
      return this;
    }
  }
}
