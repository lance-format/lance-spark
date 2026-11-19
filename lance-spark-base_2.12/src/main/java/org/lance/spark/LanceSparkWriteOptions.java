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
import org.lance.spark.utils.FileFormatVersionResolver;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Write-specific options for Lance Spark connector.
 *
 * <p>Options supplied via {@link Builder#fromOptions(Map)} override catalog-level settings for
 * write operations. Typed Builder setters applied before {@link
 * Builder#withCatalogDefaults(LanceSparkCatalogConfig)} are superseded by catalog values for the
 * same key (see that method's javadoc).
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
  private static final long serialVersionUID = 2L;

  public static final String CONFIG_DATASET_URI = "path";
  public static final String CONFIG_WRITE_MODE = "write_mode";
  public static final String CONFIG_TRANSACTION_PROPERTIES = "transaction_properties";
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
  private final String fallbackFileFormatVersion;
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

  /** Use this reference to open the dataset and apply write if set. */
  private final LanceRef ref;

  /** Catalog and cache backend configuration for process-local session reuse. */
  private final String catalogName;

  private final String indexCacheBackend;

  private final String metadataCacheBackend;
  private final Map<String, String> transactionProperties;

  private LanceSparkWriteOptions(Builder builder) {
    this.datasetUri = builder.datasetUri;
    this.writeMode = builder.writeMode;
    this.maxRowsPerFile = builder.maxRowsPerFile;
    this.maxRowsPerGroup = builder.maxRowsPerGroup;
    this.maxBytesPerFile = builder.maxBytesPerFile;
    this.fileFormatVersion = builder.fileFormatVersion;
    this.fallbackFileFormatVersion = builder.fallbackFileFormatVersion;
    this.useQueuedWriteBuffer = builder.useQueuedWriteBuffer;
    this.queueDepth = builder.queueDepth;
    this.batchSize = builder.batchSize;
    this.enableStableRowIds = builder.enableStableRowIds;
    this.useLargeVarTypes = builder.useLargeVarTypes;
    this.maxBatchBytes = builder.maxBatchBytes;
    this.blobPackFileSizeThreshold = builder.blobPackFileSizeThreshold;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.storageOptions.keySet().removeIf(TransactionProperties::isOption);
    // Internal-only context is decoded by the writer, never forwarded to Lance storage.
    this.storageOptions.remove(LanceConstant.BLOB_SOURCE_CONTEXTS_KEY);
    this.transactionProperties = builder.transactionProperties;
    this.namespace = builder.namespace;
    this.tableId = builder.tableId;
    this.ref = builder.ref;
    this.catalogName = builder.catalogName;
    this.indexCacheBackend = builder.indexCacheBackend;
    this.metadataCacheBackend = builder.metadataCacheBackend;
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
    return fileFormatVersion != null ? fileFormatVersion : fallbackFileFormatVersion;
  }

  /**
   * Returns the file format version explicitly requested via write options (or {@link
   * Builder#fileFormatVersion(String)}), or {@code null} when inherited from the existing dataset.
   * Committers pass only this explicit value to {@code CommitBuilder.storageFormat(...)} so
   * overwrites and replaces do not mutate an existing dataset's default version unintentionally.
   */
  public String getExplicitFileFormatVersion() {
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

  public LanceRef getRef() {
    return ref;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getIndexCacheBackend() {
    return indexCacheBackend;
  }

  public String getMetadataCacheBackend() {
    return metadataCacheBackend;
  }

  public Map<String, String> getTransactionProperties() {
    return transactionProperties == null ? Collections.emptyMap() : transactionProperties;
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
        .fallbackFileFormatVersion(fallbackFileFormatVersion)
        .useQueuedWriteBuffer(useQueuedWriteBuffer)
        .queueDepth(queueDepth)
        .batchSize(batchSize)
        .maxBatchBytes(maxBatchBytes)
        .enableStableRowIds(enableStableRowIds)
        .useLargeVarTypes(useLargeVarTypes)
        .blobPackFileSizeThreshold(blobPackFileSizeThreshold)
        .storageOptions(storageOptions)
        .transactionProperties(getTransactionProperties())
        .namespace(namespace)
        .tableId(tableId)
        .ref(ref)
        .catalogName(catalogName)
        .indexCacheBackend(indexCacheBackend)
        .metadataCacheBackend(metadataCacheBackend);
  }

  /** Returns a copy of these options with the given reference. */
  public LanceSparkWriteOptions withRef(LanceRef ref) {
    return toBuilder().ref(ref).build();
  }

  /**
   * Converts these write options to read options that open the same dataset at the same ref. Used
   * by executors that must read what the driver pinned, such as DELETE writing deletion files.
   *
   * <p>Read-only flags (block_size, cache sizes, executor_credential_refresh) are re-parsed from
   * {@code storageOptions} rather than copied field by field, so they cannot be dropped by a caller
   * that forgets one. The ref is applied last because {@code storageOptions} may also carry one.
   */
  public LanceSparkReadOptions toReadOptions() {
    return LanceSparkReadOptions.builder()
        .datasetUri(datasetUri)
        .fromOptions(storageOptions)
        .namespace(namespace)
        .tableId(tableId)
        .catalogName(catalogName)
        .indexCacheBackend(indexCacheBackend)
        .metadataCacheBackend(metadataCacheBackend)
        .ref(ref)
        .build();
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
    String dataStorageVersion = getFileFormatVersion();
    if (dataStorageVersion != null) {
      builder.withDataStorageVersion(dataStorageVersion);
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
        && Objects.equals(fallbackFileFormatVersion, that.fallbackFileFormatVersion)
        && Objects.equals(enableStableRowIds, that.enableStableRowIds)
        && Objects.equals(blobPackFileSizeThreshold, that.blobPackFileSizeThreshold)
        && Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(getTransactionProperties(), that.getTransactionProperties())
        && Objects.equals(tableId, that.tableId)
        && Objects.equals(ref, that.ref)
        && Objects.equals(catalogName, that.catalogName)
        && Objects.equals(indexCacheBackend, that.indexCacheBackend)
        && Objects.equals(metadataCacheBackend, that.metadataCacheBackend);
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
        fallbackFileFormatVersion,
        useQueuedWriteBuffer,
        queueDepth,
        batchSize,
        enableStableRowIds,
        useLargeVarTypes,
        maxBatchBytes,
        blobPackFileSizeThreshold,
        storageOptions,
        getTransactionProperties(),
        tableId,
        ref,
        catalogName,
        indexCacheBackend,
        metadataCacheBackend);
  }

  /** Builder for creating LanceSparkWriteOptions instances. */
  public static class Builder {
    private String datasetUri;
    private WriteMode writeMode = DEFAULT_WRITE_MODE;
    private Integer maxRowsPerFile;
    private Integer maxRowsPerGroup;
    private Long maxBytesPerFile;
    private String fileFormatVersion;
    private String fallbackFileFormatVersion;
    private boolean useQueuedWriteBuffer = DEFAULT_USE_QUEUED_WRITE_BUFFER;
    private int queueDepth = DEFAULT_QUEUE_DEPTH;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private Boolean enableStableRowIds;
    private boolean useLargeVarTypes = DEFAULT_USE_LARGE_VAR_TYPES;
    private long maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
    private Long blobPackFileSizeThreshold;
    private Map<String, String> storageOptions = new HashMap<>();
    private Map<String, String> transactionProperties = Collections.emptyMap();
    private LanceNamespace namespace;
    private List<String> tableId;
    private LanceRef ref;
    private String catalogName;
    private String indexCacheBackend;
    private String metadataCacheBackend;

    private Builder() {}

    public Builder transactionProperties(Map<String, String> properties) {
      this.transactionProperties = TransactionProperties.snapshot(properties);
      return this;
    }

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

    /**
     * Sets the dataset-derived data-file format version used when no explicit {@code
     * file_format_version} write option is supplied. Upgrades incompatible defaults via {@link
     * FileFormatVersionResolver} without marking the version as an explicit manifest override.
     */
    public Builder fallbackFileFormatVersion(StructType schema, String tableFileFormatVersion) {
      this.fallbackFileFormatVersion =
          FileFormatVersionResolver.resolveForDataFiles(schema, tableFileFormatVersion);
      return this;
    }

    private Builder fallbackFileFormatVersion(String fallbackFileFormatVersion) {
      this.fallbackFileFormatVersion = fallbackFileFormatVersion;
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

    /** Pin opens to this dataset reference. */
    public Builder ref(LanceRef ref) {
      this.ref = ref;
      return this;
    }

    public Builder catalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    public Builder indexCacheBackend(String indexCacheBackend) {
      this.indexCacheBackend = indexCacheBackend;
      return this;
    }

    public Builder metadataCacheBackend(String metadataCacheBackend) {
      this.metadataCacheBackend = metadataCacheBackend;
      return this;
    }

    /**
     * Resolves catalog defaults, supported table defaults, and per-operation options, in that
     * order.
     *
     * <p>Only tuning options are read from table properties. Spark's {@code option.} aliases,
     * storage credentials, and operation selectors are not table defaults.
     *
     * @param defaults catalog options, including storage options
     * @param tableProperties persisted table properties (or the staged table's properties)
     * @param options explicit options for this operation
     * @return this builder
     */
    public Builder fromOptions(
        Map<String, String> defaults,
        Map<String, String> tableProperties,
        Map<String, String> options) {
      Map<String, String> catalog = new HashMap<>(defaults);
      catalog.keySet().removeIf(TransactionProperties::isOption);
      return parseOptions(new LanceOptionParser(catalog, tableProperties, options), options);
    }

    /**
     * Parses options from a map, extracting write-specific settings.
     *
     * @param rawOptions the options map
     * @return this builder
     */
    public Builder fromOptions(Map<String, String> rawOptions) {
      return parseOptions(
          new LanceOptionParser(Collections.emptyMap(), Collections.emptyMap(), rawOptions),
          rawOptions);
    }

    private Builder parseOptions(LanceOptionParser parser, Map<String, String> operationOptions) {
      Map<String, String> rawOptions = parser.resolvedOptions();
      CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(rawOptions);
      Preconditions.checkArgument(
          !options.containsKey(LanceSparkReadOptions.CONFIG_BRANCH),
          "The branch option is read-only");
      Preconditions.checkArgument(
          !options.containsKey(LanceSparkReadOptions.CONFIG_TAG), "The tag option is read-only");
      this.transactionProperties = Collections.emptyMap();
      this.transactionProperties = TransactionProperties.fromOptions(operationOptions);
      this.writeMode =
          parser
              .option(CONFIG_WRITE_MODE)
              .parse(this.writeMode, raw -> WriteMode.valueOf(raw.toUpperCase(Locale.ROOT)));
      this.maxRowsPerFile =
          parser
              .option(CONFIG_MAX_ROWS_PER_FILE)
              .tableProperty(CONFIG_MAX_ROWS_PER_FILE)
              .intValue(this.maxRowsPerFile);
      this.maxRowsPerGroup =
          parser
              .option(CONFIG_MAX_ROWS_PER_GROUP)
              .tableProperty(CONFIG_MAX_ROWS_PER_GROUP)
              .intValue(this.maxRowsPerGroup);
      this.maxBytesPerFile =
          parser
              .option(CONFIG_MAX_BYTES_PER_FILE)
              .tableProperty(CONFIG_MAX_BYTES_PER_FILE)
              .longValue(this.maxBytesPerFile);
      this.fileFormatVersion =
          parser.option(CONFIG_FILE_FORMAT_VERSION).stringValue(this.fileFormatVersion);
      this.useQueuedWriteBuffer =
          parser
              .option(CONFIG_USE_QUEUED_WRITE_BUFFER)
              .tableProperty(CONFIG_USE_QUEUED_WRITE_BUFFER)
              .booleanValue(this.useQueuedWriteBuffer);
      this.queueDepth =
          parser
              .option(CONFIG_QUEUE_DEPTH)
              .tableProperty(CONFIG_QUEUE_DEPTH)
              .intValue(this.queueDepth);
      this.batchSize =
          parser
              .option(CONFIG_BATCH_SIZE)
              .tableProperty(CONFIG_BATCH_SIZE)
              .positiveIntValue(this.batchSize);
      this.enableStableRowIds =
          parser.option(CONFIG_ENABLE_STABLE_ROW_IDS).booleanValue(this.enableStableRowIds);
      this.useLargeVarTypes =
          parser
              .option(CONFIG_USE_LARGE_VAR_TYPES)
              .tableProperty(CONFIG_USE_LARGE_VAR_TYPES)
              .booleanValue(this.useLargeVarTypes);
      this.maxBatchBytes =
          parser
              .option(CONFIG_MAX_BATCH_BYTES)
              .tableProperty(CONFIG_MAX_BATCH_BYTES)
              .positiveLongValue(this.maxBatchBytes);
      this.blobPackFileSizeThreshold =
          parser
              .option(CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD)
              .tableProperty(CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD)
              .positiveLongValue(this.blobPackFileSizeThreshold);
      this.storageOptions = parser.resolvedOptions();
      this.storageOptions.keySet().removeIf(TransactionProperties::isOption);
      return this;
    }

    /**
     * Merges catalog config options as defaults; options supplied via {@link #fromOptions(Map)}
     * override catalog values.
     *
     * <p>Re-parses the merged map through {@link #fromOptions(Map)} so that catalog-level write
     * settings (e.g. {@code spark.sql.catalog.<name>.<key>}) are promoted into their typed Builder
     * fields and take effect on write paths that do not supply per-write {@code .option(...)}
     * overrides. Mirrors {@code LanceSparkReadOptions.Builder#withCatalogDefaults}. Typed values
     * previously assigned via Builder setters are re-derived from the merged map, so a catalog
     * default wins over an earlier setter call for the same key. Call this after {@link
     * #fromOptions(Map)} / {@link #storageOptions(Map)}: a later {@code fromOptions} call replaces
     * the storage options map and discards the merged catalog defaults.
     *
     * @param catalogConfig the catalog config
     * @return this builder
     */
    public Builder withCatalogDefaults(LanceSparkCatalogConfig catalogConfig) {
      // Merge storage options: catalog options are defaults, current options override
      Map<String, String> merged = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      merged.putAll(catalogConfig.getStorageOptions());
      merged.keySet().removeIf(TransactionProperties::isOption);
      merged.putAll(this.storageOptions);
      this.indexCacheBackend = catalogConfig.getIndexCacheBackend();
      this.metadataCacheBackend = catalogConfig.getMetadataCacheBackend();
      Map<String, String> operationProperties = this.transactionProperties;
      return fromOptions(merged).transactionProperties(operationProperties);
    }

    public LanceSparkWriteOptions build() {
      if (datasetUri == null) {
        throw new IllegalArgumentException("datasetUri is required");
      }
      return new LanceSparkWriteOptions(this);
    }
  }
}
