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
  public static final String CONFIG_FILE_FORMAT_VERSION = "file_format_version";
  public static final String CONFIG_USE_QUEUED_WRITE_BUFFER = "use_queued_write_buffer";
  public static final String CONFIG_QUEUE_DEPTH = "queue_depth";
  public static final String CONFIG_BATCH_SIZE = "batch_size";
  public static final String CONFIG_ENABLE_STABLE_ROW_IDS = "enable_stable_row_ids";
  public static final String CONFIG_USE_LARGE_VAR_TYPES = "use_large_var_types";
  public static final String CONFIG_MAX_BATCH_BYTES = "max_batch_bytes";
  public static final String CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD = "blob_pack_file_size_threshold";

  private static final WriteMode DEFAULT_WRITE_MODE = WriteMode.APPEND;
  private static final boolean DEFAULT_USE_QUEUED_WRITE_BUFFER = false;
  private static final int DEFAULT_QUEUE_DEPTH = 8;
  // Changed from 512 to 8192 for better write performance consistency with read path
  private static final int DEFAULT_BATCH_SIZE = 8192;
  private static final boolean DEFAULT_USE_LARGE_VAR_TYPES = false;
  // Default max bytes per batch (256MB). Batches flush when either row count or byte size
  // is reached, whichever comes first. This prevents OOM when rows are very large.
  public static final long DEFAULT_MAX_BATCH_BYTES = 256L * 1024 * 1024;

  private final String datasetUri;
  private final WriteMode writeMode;
  private final Integer maxRowsPerFile;
  private final Integer maxRowsPerGroup;
  private final Long maxBytesPerFile;
  private final String fileFormatVersion;
  private final boolean useQueuedWriteBuffer;
  private final int queueDepth;
  private final int batchSize;
  // Boxed so we can represent "unset" (null): when null, callers omit the flag and lance-core
  // inherits from the manifest (e.g. append without re-specifying). Staged commit uses primitives.
  private final Boolean enableStableRowIds;
  private final boolean useLargeVarTypes;
  private final long maxBatchBytes;
  // Boxed: null means "unset" so lance-core uses its own default (1 GiB as of 6.0.0-beta.1).
  private final Long blobPackFileSizeThreshold;
  private final Map<String, String> storageOptions;

  /** The namespace for credential vending. Transient as LanceNamespace is not serializable. */
  private transient LanceNamespace namespace;

  /** The table identifier within the namespace, used for credential refresh. */
  private final List<String> tableId;

  /** Use this version to open the dataset and apply write if set. */
  private final Long version;

  private LanceSparkWriteOptions(Builder builder) {
    this.datasetUri = builder.datasetUri;
    this.writeMode = builder.writeMode;
    this.maxRowsPerFile = builder.maxRowsPerFile;
    this.maxRowsPerGroup = builder.maxRowsPerGroup;
    this.maxBytesPerFile = builder.maxBytesPerFile;
    this.fileFormatVersion = builder.fileFormatVersion;
    this.useQueuedWriteBuffer = builder.useQueuedWriteBuffer;
    this.queueDepth = builder.queueDepth;
    this.batchSize = builder.batchSize;
    this.enableStableRowIds = builder.enableStableRowIds;
    this.useLargeVarTypes = builder.useLargeVarTypes;
    this.maxBatchBytes = builder.maxBatchBytes;
    this.blobPackFileSizeThreshold = builder.blobPackFileSizeThreshold;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.namespace = builder.namespace;
    this.tableId = builder.tableId;
    this.version = builder.version;
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

  public String getFileFormatVersion() {
    return fileFormatVersion;
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

  /** Nullable when the write option was not specified (see field comment above). */
  public Boolean getEnableStableRowIds() {
    return enableStableRowIds;
  }

  public boolean isUseLargeVarTypes() {
    return useLargeVarTypes;
  }

  public long getMaxBatchBytes() {
    return maxBatchBytes;
  }

  /** Nullable: when unset, lance-core uses its own default for blob v2 pack sidecar size. */
  public Long getBlobPackFileSizeThreshold() {
    return blobPackFileSizeThreshold;
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

  public Long getVersion() {
    return version;
  }

  /** Returns a builder pre-populated with all fields from this instance. */
  public Builder toBuilder() {
    return builder()
        .datasetUri(datasetUri)
        .writeMode(writeMode)
        .maxRowsPerFile(maxRowsPerFile)
        .maxRowsPerGroup(maxRowsPerGroup)
        .maxBytesPerFile(maxBytesPerFile)
        .fileFormatVersion(fileFormatVersion)
        .useQueuedWriteBuffer(useQueuedWriteBuffer)
        .queueDepth(queueDepth)
        .batchSize(batchSize)
        .maxBatchBytes(maxBatchBytes)
        .enableStableRowIds(enableStableRowIds)
        .useLargeVarTypes(useLargeVarTypes)
        .blobPackFileSizeThreshold(blobPackFileSizeThreshold)
        .storageOptions(storageOptions)
        .namespace(namespace)
        .tableId(tableId)
        .version(version);
  }

  /** Returns a copy of these options with version set to the given version. */
  public LanceSparkWriteOptions withVersion(long version) {
    return toBuilder().version(version).build();
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
    return toWriteParams(null);
  }

  /**
   * Converts this to Lance {@link WriteParams}, merging driver-side {@code initialStorageOptions}
   * into the base storage options. Pass {@code null} for driver-side callers that do not have
   * describeTable() credentials.
   */
  public WriteParams toWriteParams(Map<String, String> initialStorageOptions) {
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
    if (fileFormatVersion != null) {
      builder.withDataStorageVersion(fileFormatVersion);
    }
    if (enableStableRowIds != null) {
      builder.withEnableStableRowIds(enableStableRowIds);
    }
    if (blobPackFileSizeThreshold != null) {
      builder.withBlobPackFileSizeThreshold(blobPackFileSizeThreshold);
    }
    Map<String, String> merged =
        LanceRuntime.mergeStorageOptions(storageOptions, initialStorageOptions);
    if (!merged.isEmpty()) {
      builder.withStorageOptions(merged);
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
        && useLargeVarTypes == that.useLargeVarTypes
        && maxBatchBytes == that.maxBatchBytes
        && Objects.equals(datasetUri, that.datasetUri)
        && writeMode == that.writeMode
        && Objects.equals(maxRowsPerFile, that.maxRowsPerFile)
        && Objects.equals(maxRowsPerGroup, that.maxRowsPerGroup)
        && Objects.equals(maxBytesPerFile, that.maxBytesPerFile)
        && Objects.equals(fileFormatVersion, that.fileFormatVersion)
        && Objects.equals(enableStableRowIds, that.enableStableRowIds)
        && Objects.equals(blobPackFileSizeThreshold, that.blobPackFileSizeThreshold)
        && Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(tableId, that.tableId)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        datasetUri,
        writeMode,
        maxRowsPerFile,
        maxRowsPerGroup,
        maxBytesPerFile,
        fileFormatVersion,
        useQueuedWriteBuffer,
        queueDepth,
        batchSize,
        enableStableRowIds,
        useLargeVarTypes,
        maxBatchBytes,
        blobPackFileSizeThreshold,
        storageOptions,
        tableId,
        version);
  }

  /** Builder for creating LanceSparkWriteOptions instances. */
  public static class Builder {
    private String datasetUri;
    private WriteMode writeMode = DEFAULT_WRITE_MODE;
    private Integer maxRowsPerFile;
    private Integer maxRowsPerGroup;
    private Long maxBytesPerFile;
    private String fileFormatVersion;
    private boolean useQueuedWriteBuffer = DEFAULT_USE_QUEUED_WRITE_BUFFER;
    private int queueDepth = DEFAULT_QUEUE_DEPTH;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private Boolean enableStableRowIds;
    private boolean useLargeVarTypes = DEFAULT_USE_LARGE_VAR_TYPES;
    private long maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
    private Long blobPackFileSizeThreshold;
    private Map<String, String> storageOptions = new HashMap<>();
    private LanceNamespace namespace;
    private List<String> tableId;
    private Long version;

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

    public Builder fileFormatVersion(String fileFormatVersion) {
      this.fileFormatVersion = fileFormatVersion;
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

    public Builder enableStableRowIds(Boolean enableStableRowIds) {
      this.enableStableRowIds = enableStableRowIds;
      return this;
    }

    public Builder useLargeVarTypes(boolean useLargeVarTypes) {
      this.useLargeVarTypes = useLargeVarTypes;
      return this;
    }

    public Builder maxBatchBytes(long maxBatchBytes) {
      Preconditions.checkArgument(maxBatchBytes > 0, "maxBatchBytes must be positive");
      this.maxBatchBytes = maxBatchBytes;
      return this;
    }

    public Builder blobPackFileSizeThreshold(Long blobPackFileSizeThreshold) {
      Preconditions.checkArgument(
          blobPackFileSizeThreshold == null || blobPackFileSizeThreshold > 0,
          "blobPackFileSizeThreshold must be positive");
      this.blobPackFileSizeThreshold = blobPackFileSizeThreshold;
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

    /** Pin opens to this dataset manifest version. */
    public Builder version(Long version) {
      this.version = version;
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
      if (options.containsKey(CONFIG_FILE_FORMAT_VERSION)) {
        this.fileFormatVersion = options.get(CONFIG_FILE_FORMAT_VERSION);
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
      if (options.containsKey(CONFIG_ENABLE_STABLE_ROW_IDS)) {
        this.enableStableRowIds = Boolean.parseBoolean(options.get(CONFIG_ENABLE_STABLE_ROW_IDS));
      }
      if (options.containsKey(CONFIG_USE_LARGE_VAR_TYPES)) {
        this.useLargeVarTypes = Boolean.parseBoolean(options.get(CONFIG_USE_LARGE_VAR_TYPES));
      }
      if (options.containsKey(CONFIG_MAX_BATCH_BYTES)) {
        long parsedMaxBatchBytes = Long.parseLong(options.get(CONFIG_MAX_BATCH_BYTES));
        Preconditions.checkArgument(parsedMaxBatchBytes > 0, "max_batch_bytes must be positive");
        this.maxBatchBytes = parsedMaxBatchBytes;
      }
      if (options.containsKey(CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD)) {
        long parsed = Long.parseLong(options.get(CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD));
        Preconditions.checkArgument(parsed > 0, "blob_pack_file_size_threshold must be positive");
        this.blobPackFileSizeThreshold = parsed;
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
