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
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/** Spark write builder. */
public class SparkWrite implements Write {
  private final LanceSparkWriteOptions writeOptions;
  private final StructType schema;
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
  private final StagedCommit stagedCommit;

  SparkWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      StagedCommit stagedCommit) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.stagedCommit = stagedCommit;
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
        stagedCommit);
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

    public SparkWriteBuilder(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
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
                  .dataStorageVersion(writeOptions.getDataStorageVersion())
                  .maxBytesPerFile(writeOptions.getMaxBytesPerFile())
                  .maxRowsPerFile(writeOptions.getMaxRowsPerFile())
                  .maxRowsPerGroup(writeOptions.getMaxRowsPerGroup())
                  .queueDepth(writeOptions.getQueueDepth())
                  .useQueuedWriteBuffer(writeOptions.isUseQueuedWriteBuffer())
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
          stagedCommit);
    }

    @Override
    public WriteBuilder truncate() {
      this.overwrite = true;
      return this;
    }
  }
}
