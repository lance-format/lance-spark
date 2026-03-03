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

import org.lance.WriteParams;
import org.lance.WriteParams.LanceFileVersion;
import org.lance.WriteParams.WriteMode;
import org.lance.namespace.LanceNamespace;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Write-specific options for Lance Spark connector.
 *
 * <p>These options override catalog-level settings for write operations.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * LanceSparkWriteOptions options = LanceSparkWriteOptions.builder()
 *     .datasetUri("s3://bucket/path")
 *     .writeMode(WriteMode.APPEND)
 *     .maxRowsPerFile(1000000)
 *     .namespace(namespace)
 *     .tableId(tableId)
 *     .build();
 * }</pre>
 */
public class LanceSparkWriteOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String CONFIG_DATASET_URI = "path";
  public static final String CONFIG_WRITE_MODE = "write_mode";
  public static final String CONFIG_MAX_ROWS_PER_FILE = "max_row_per_file";
  public static final String CONFIG_MAX_ROWS_PER_GROUP = "max_rows_per_group";
  public static final String CONFIG_MAX_BYTES_PER_FILE = "max_bytes_per_file";
  public static final String CONFIG_DATA_STORAGE_VERSION = "data_storage_version";
  public static final String CONFIG_USE_QUEUED_WRITE_BUFFER = "use_queued_write_buffer";
  public static final String CONFIG_QUEUE_DEPTH = "queue_depth";
  public static final String CONFIG_BATCH_SIZE = "batch_size";

  private static final WriteMode DEFAULT_WRITE_MODE = WriteMode.APPEND;
  private static final boolean DEFAULT_USE_QUEUED_WRITE_BUFFER = false;
  private static final int DEFAULT_QUEUE_DEPTH = 8;
  // Changed from 512 to 8192 for better write performance consistency with read path
  private static final int DEFAULT_BATCH_SIZE = 8192;

  private final String datasetUri;
  private final WriteMode writeMode;
  private final Integer maxRowsPerFile;
  private final Integer maxRowsPerGroup;
  private final Long maxBytesPerFile;
  private final LanceFileVersion dataStorageVersion;
  private final boolean useQueuedWriteBuffer;
  private final int queueDepth;
  private final int batchSize;
  private final Map<String, String> storageOptions;

  /** The namespace for credential vending. Transient as LanceNamespace is not serializable. */
  private transient LanceNamespace namespace;

  /** The table identifier within the namespace, used for credential refresh. */
  private final List<String> tableId;

  private LanceSparkWriteOptions(Builder builder) {
    this.datasetUri = builder.datasetUri;
    this.writeMode = builder.writeMode;
    this.maxRowsPerFile = builder.maxRowsPerFile;
    this.maxRowsPerGroup = builder.maxRowsPerGroup;
    this.maxBytesPerFile = builder.maxBytesPerFile;
    this.dataStorageVersion = builder.dataStorageVersion;
    this.useQueuedWriteBuffer = builder.useQueuedWriteBuffer;
    this.queueDepth = builder.queueDepth;
    this.batchSize = builder.batchSize;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.namespace = builder.namespace;
    this.tableId = builder.tableId;
  }

  /** Creates a new builder for LanceSparkWriteOptions. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates write options from a map of properties and dataset URI.
   *
   * @param properties the properties map
   * @param datasetUri the dataset URI
   * @return a new LanceSparkWriteOptions
   */
  public static LanceSparkWriteOptions from(Map<String, String> properties, String datasetUri) {
    return builder().datasetUri(datasetUri).fromOptions(properties).build();
  }

  /**
   * Creates write options from a dataset URI only.
   *
   * @param datasetUri the dataset URI
   * @return a new LanceSparkWriteOptions
   */
  public static LanceSparkWriteOptions from(String datasetUri) {
    return builder().datasetUri(datasetUri).build();
  }

  // ========== Getters ==========

  public String getDatasetUri() {
    return datasetUri;
  }

  public WriteMode getWriteMode() {
    return writeMode;
  }

  public Integer getMaxRowsPerFile() {
    return maxRowsPerFile;
  }

  public Integer getMaxRowsPerGroup() {
    return maxRowsPerGroup;
  }

  public Long getMaxBytesPerFile() {
    return maxBytesPerFile;
  }

  public LanceFileVersion getDataStorageVersion() {
    return dataStorageVersion;
  }

  public boolean isUseQueuedWriteBuffer() {
    return useQueuedWriteBuffer;
  }

  public int getQueueDepth() {
    return queueDepth;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  public LanceNamespace getNamespace() {
    return namespace;
  }

  public List<String> getTableId() {
    return tableId;
  }

  public boolean hasNamespace() {
    return namespace != null && tableId != null;
  }

  /**
   * Sets the namespace for this options. Used after deserialization to restore the namespace.
   *
   * @param namespace the namespace to set
   */
  public void setNamespace(LanceNamespace namespace) {
    this.namespace = namespace;
  }

  /**
   * Creates a StorageOptionsProvider for dynamic credential refresh.
   *
   * @return a StorageOptionsProvider if namespace is configured, null otherwise
   */
  public org.lance.io.StorageOptionsProvider getStorageOptionsProvider() {
    if (namespace != null && tableId != null) {
      return new org.lance.namespace.LanceNamespaceStorageOptionsProvider(namespace, tableId);
    }
    return null;
  }

  /**
   * Returns whether the write mode is overwrite.
   *
   * @return true if write mode is OVERWRITE
   */
  public boolean isOverwrite() {
    return writeMode == WriteMode.OVERWRITE;
  }

  /**
   * Converts this to Lance WriteParams for the native library.
   *
   * @return WriteParams for the Lance native library
   */
  public WriteParams toWriteParams() {
    WriteParams.Builder builder = new WriteParams.Builder();
    builder.withMode(writeMode);
    if (maxRowsPerFile != null) {
      builder.withMaxRowsPerFile(maxRowsPerFile);
    }
    if (maxRowsPerGroup != null) {
      builder.withMaxRowsPerGroup(maxRowsPerGroup);
    }
    if (maxBytesPerFile != null) {
      builder.withMaxBytesPerFile(maxBytesPerFile);
    }
    if (dataStorageVersion != null) {
      builder.withDataStorageVersion(dataStorageVersion);
    }
    if (!storageOptions.isEmpty()) {
      builder.withStorageOptions(storageOptions);
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    LanceSparkWriteOptions that = (LanceSparkWriteOptions) o;
    return useQueuedWriteBuffer == that.useQueuedWriteBuffer
        && queueDepth == that.queueDepth
        && batchSize == that.batchSize
        && Objects.equals(datasetUri, that.datasetUri)
        && writeMode == that.writeMode
        && Objects.equals(maxRowsPerFile, that.maxRowsPerFile)
        && Objects.equals(maxRowsPerGroup, that.maxRowsPerGroup)
        && Objects.equals(maxBytesPerFile, that.maxBytesPerFile)
        && dataStorageVersion == that.dataStorageVersion
        && Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(tableId, that.tableId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        datasetUri,
        writeMode,
        maxRowsPerFile,
        maxRowsPerGroup,
        maxBytesPerFile,
        dataStorageVersion,
        useQueuedWriteBuffer,
        queueDepth,
        batchSize,
        storageOptions,
        tableId);
  }

  /** Builder for creating LanceSparkWriteOptions instances. */
  public static class Builder {
    private String datasetUri;
    private WriteMode writeMode = DEFAULT_WRITE_MODE;
    private Integer maxRowsPerFile;
    private Integer maxRowsPerGroup;
    private Long maxBytesPerFile;
    private LanceFileVersion dataStorageVersion;
    private boolean useQueuedWriteBuffer = DEFAULT_USE_QUEUED_WRITE_BUFFER;
    private int queueDepth = DEFAULT_QUEUE_DEPTH;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private Map<String, String> storageOptions = new HashMap<>();
    private LanceNamespace namespace;
    private List<String> tableId;

    private Builder() {}

    public Builder datasetUri(String datasetUri) {
      this.datasetUri = datasetUri;
      return this;
    }

    public Builder writeMode(WriteMode writeMode) {
      this.writeMode = writeMode;
      return this;
    }

    public Builder maxRowsPerFile(Integer maxRowsPerFile) {
      this.maxRowsPerFile = maxRowsPerFile;
      return this;
    }

    public Builder maxRowsPerGroup(Integer maxRowsPerGroup) {
      this.maxRowsPerGroup = maxRowsPerGroup;
      return this;
    }

    public Builder maxBytesPerFile(Long maxBytesPerFile) {
      this.maxBytesPerFile = maxBytesPerFile;
      return this;
    }

    public Builder dataStorageVersion(LanceFileVersion dataStorageVersion) {
      this.dataStorageVersion = dataStorageVersion;
      return this;
    }

    public Builder useQueuedWriteBuffer(boolean useQueuedWriteBuffer) {
      this.useQueuedWriteBuffer = useQueuedWriteBuffer;
      return this;
    }

    public Builder queueDepth(int queueDepth) {
      this.queueDepth = queueDepth;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder storageOptions(Map<String, String> storageOptions) {
      this.storageOptions = new HashMap<>(storageOptions);
      return this;
    }

    public Builder namespace(LanceNamespace namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder tableId(List<String> tableId) {
      this.tableId = tableId;
      return this;
    }

    /**
     * Parses options from a map, extracting write-specific settings.
     *
     * @param options the options map
     * @return this builder
     */
    public Builder fromOptions(Map<String, String> options) {
      this.storageOptions = new HashMap<>(options);
      if (options.containsKey(CONFIG_WRITE_MODE)) {
        this.writeMode = WriteMode.valueOf(options.get(CONFIG_WRITE_MODE).toUpperCase());
      }
      if (options.containsKey(CONFIG_MAX_ROWS_PER_FILE)) {
        this.maxRowsPerFile = Integer.parseInt(options.get(CONFIG_MAX_ROWS_PER_FILE));
      }
      if (options.containsKey(CONFIG_MAX_ROWS_PER_GROUP)) {
        this.maxRowsPerGroup = Integer.parseInt(options.get(CONFIG_MAX_ROWS_PER_GROUP));
      }
      if (options.containsKey(CONFIG_MAX_BYTES_PER_FILE)) {
        this.maxBytesPerFile = Long.parseLong(options.get(CONFIG_MAX_BYTES_PER_FILE));
      }
      if (options.containsKey(CONFIG_DATA_STORAGE_VERSION)) {
        this.dataStorageVersion =
            LanceFileVersion.valueOf(options.get(CONFIG_DATA_STORAGE_VERSION));
      }
      if (options.containsKey(CONFIG_USE_QUEUED_WRITE_BUFFER)) {
        this.useQueuedWriteBuffer =
            Boolean.parseBoolean(options.get(CONFIG_USE_QUEUED_WRITE_BUFFER));
      }
      if (options.containsKey(CONFIG_QUEUE_DEPTH)) {
        this.queueDepth = Integer.parseInt(options.get(CONFIG_QUEUE_DEPTH));
      }
      if (options.containsKey(CONFIG_BATCH_SIZE)) {
        int parsedBatchSize = Integer.parseInt(options.get(CONFIG_BATCH_SIZE));
        Preconditions.checkArgument(parsedBatchSize > 0, "batch_size must be positive");
        this.batchSize = parsedBatchSize;
      }
      return this;
    }

    /**
     * Merges catalog config options as defaults (write options override).
     *
     * @param catalogConfig the catalog config
     * @return this builder
     */
    public Builder withCatalogDefaults(LanceSparkCatalogConfig catalogConfig) {
      // Merge storage options: catalog options are defaults, current options override
      Map<String, String> merged = new HashMap<>(catalogConfig.getStorageOptions());
      merged.putAll(this.storageOptions);
      this.storageOptions = merged;
      return this;
    }

    public LanceSparkWriteOptions build() {
      if (datasetUri == null) {
        throw new IllegalArgumentException("datasetUri is required");
      }
      return new LanceSparkWriteOptions(this);
    }
  }
}
