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

import org.lance.ReadOptions;
import org.lance.io.StorageOptionsProvider;
import org.lance.ipc.Query;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.LanceNamespaceStorageOptionsProvider;
import org.lance.spark.utils.QueryUtils;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Read-specific options for Lance Spark connector.
 *
 * <p>These options override catalog-level settings for read operations.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * LanceSparkReadOptions options = LanceSparkReadOptions.builder()
 *     .datasetUri("s3://bucket/path")
 *     .pushDownFilters(true)
 *     .batchSize(1024)
 *     .namespace(namespace)
 *     .tableId(tableId)
 *     .build();
 * }</pre>
 */
public class LanceSparkReadOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String CONFIG_DATASET_URI = "path";
  public static final String CONFIG_PUSH_DOWN_FILTERS = "pushDownFilters";
  public static final String CONFIG_BLOCK_SIZE = "block_size";
  public static final String CONFIG_VERSION = "version";
  public static final String CONFIG_INDEX_CACHE_SIZE = "index_cache_size";
  public static final String CONFIG_METADATA_CACHE_SIZE = "metadata_cache_size";
  public static final String CONFIG_BATCH_SIZE = "batch_size";
  public static final String CONFIG_TOP_N_PUSH_DOWN = "topN_push_down";

  public static final String CONFIG_NEAREST = "nearest";
  public static final String LANCE_FILE_SUFFIX = ".lance";

  private static final boolean DEFAULT_PUSH_DOWN_FILTERS = true;
  private static final int DEFAULT_BATCH_SIZE = 512;
  private static final boolean DEFAULT_TOP_N_PUSH_DOWN = true;

  private final String datasetUri;
  private final String dbPath;
  private final String datasetName;
  private final boolean pushDownFilters;
  private final Integer blockSize;
  private final Integer version;
  private final String tag;
  private final Integer indexCacheSize;
  private final Integer metadataCacheSize;
  private final int batchSize;
  private final Query nearest;
  private final boolean topNPushDown;
  private final Map<String, String> storageOptions;

  /** The namespace for credential vending. Transient as LanceNamespace is not serializable. */
  private transient LanceNamespace namespace;

  /** The table identifier within the namespace, used for credential refresh. */
  private final List<String> tableId;

  private LanceSparkReadOptions(Builder builder) {
    this.datasetUri = builder.datasetUri;
    String[] paths = extractDbPathAndDatasetName(datasetUri);
    this.dbPath = paths[0];
    this.datasetName = paths[1];
    this.pushDownFilters = builder.pushDownFilters;
    this.blockSize = builder.blockSize;
    this.version = builder.version;
    this.tag = builder.tag;
    this.indexCacheSize = builder.indexCacheSize;
    this.metadataCacheSize = builder.metadataCacheSize;
    this.batchSize = builder.batchSize;
    this.nearest = builder.nearest;
    this.topNPushDown = builder.topNPushDown;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.namespace = builder.namespace;
    this.tableId = builder.tableId;
  }

  /** Creates a new builder for LanceSparkReadOptions. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates read options from a map of properties. The path key must be present.
   *
   * @param properties the properties map containing 'path' key
   * @return a new LanceSparkReadOptions
   */
  public static LanceSparkReadOptions from(Map<String, String> properties) {
    String datasetUri = properties.get(CONFIG_DATASET_URI);
    if (datasetUri == null) {
      throw new IllegalArgumentException("Missing required option: " + CONFIG_DATASET_URI);
    }
    return builder().datasetUri(datasetUri).fromOptions(properties).build();
  }

  /**
   * Creates read options from a map of properties and dataset URI.
   *
   * @param properties the properties map
   * @param datasetUri the dataset URI
   * @return a new LanceSparkReadOptions
   */
  public static LanceSparkReadOptions from(Map<String, String> properties, String datasetUri) {
    return builder().datasetUri(datasetUri).fromOptions(properties).build();
  }

  /**
   * Creates read options from a dataset URI only.
   *
   * @param datasetUri the dataset URI
   * @return a new LanceSparkReadOptions
   */
  public static LanceSparkReadOptions from(String datasetUri) {
    return builder().datasetUri(datasetUri).build();
  }

  // ========== Helper methods ==========

  private static String[] extractDbPathAndDatasetName(String datasetUri) {
    if (datasetUri == null) {
      throw new IllegalArgumentException("The dataset uri should not be null");
    }

    int lastSlashIndex = datasetUri.lastIndexOf('/');
    if (lastSlashIndex == -1) {
      throw new IllegalArgumentException("Invalid dataset uri: " + datasetUri);
    }

    String dbPath = datasetUri.substring(0, lastSlashIndex + 1);
    String datasetNameWithSuffix = datasetUri.substring(lastSlashIndex + 1);
    String datasetName;
    if (datasetUri.endsWith(LANCE_FILE_SUFFIX)) {
      datasetName =
          datasetNameWithSuffix.substring(
              0, datasetNameWithSuffix.length() - LANCE_FILE_SUFFIX.length());
    } else {
      datasetName = datasetNameWithSuffix;
    }

    return new String[] {dbPath, datasetName};
  }

  // ========== Getters ==========

  public String getDatasetUri() {
    return datasetUri;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public boolean isPushDownFilters() {
    return pushDownFilters;
  }

  public Integer getBlockSize() {
    return blockSize;
  }

  public Integer getVersion() {
    return version;
  }

  public String getTag() {
    return tag;
  }

  public Integer getIndexCacheSize() {
    return indexCacheSize;
  }

  public Integer getMetadataCacheSize() {
    return metadataCacheSize;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Query getNearest() {
    return nearest;
  }

  public boolean isTopNPushDown() {
    return topNPushDown;
  }

  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  public String getNearestJson() {
    return QueryUtils.queryToString(nearest);
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
  public StorageOptionsProvider getStorageOptionsProvider() {
    if (namespace != null && tableId != null) {
      return new LanceNamespaceStorageOptionsProvider(namespace, tableId);
    }
    return null;
  }

  /**
   * Converts this to Lance ReadOptions for the native library.
   *
   * @return ReadOptions for the Lance native library
   */
  public ReadOptions toReadOptions() {
    ReadOptions.Builder builder = new ReadOptions.Builder();
    if (blockSize != null) {
      builder.setBlockSize(blockSize);
    }
    if (version != null) {
      builder.setVersion(version);
    }
    if (tag != null) {
      builder.setTag(tag);
    }
    if (indexCacheSize != null) {
      builder.setIndexCacheSize(indexCacheSize);
    }
    if (metadataCacheSize != null) {
      builder.setMetadataCacheSize(metadataCacheSize);
    }
    builder.setStorageOptions(storageOptions);
    StorageOptionsProvider provider = getStorageOptionsProvider();
    if (provider != null) {
      builder.setStorageOptionsProvider(provider);
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceSparkReadOptions that = (LanceSparkReadOptions) o;
    return pushDownFilters == that.pushDownFilters
        && batchSize == that.batchSize
        && topNPushDown == that.topNPushDown
        && Objects.equals(datasetUri, that.datasetUri)
        && Objects.equals(blockSize, that.blockSize)
        && Objects.equals(version, that.version)
        && Objects.equals(tag, that.tag)
        && Objects.equals(indexCacheSize, that.indexCacheSize)
        && Objects.equals(metadataCacheSize, that.metadataCacheSize)
        && Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(tableId, that.tableId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        datasetUri,
        pushDownFilters,
        blockSize,
        version,
        tag,
        indexCacheSize,
        metadataCacheSize,
        batchSize,
        topNPushDown,
        storageOptions,
        tableId);
  }

  /** Builder for creating LanceSparkReadOptions instances. */
  public static class Builder {
    private String datasetUri;
    private boolean pushDownFilters = DEFAULT_PUSH_DOWN_FILTERS;
    private Integer blockSize;
    private Query nearest;
    private Integer version;
    private String tag;
    private Integer indexCacheSize;
    private Integer metadataCacheSize;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean topNPushDown = DEFAULT_TOP_N_PUSH_DOWN;
    private Map<String, String> storageOptions = new HashMap<>();
    private LanceNamespace namespace;
    private List<String> tableId;

    private Builder() {}

    public Builder datasetUri(String datasetUri) {
      this.datasetUri = datasetUri;
      return this;
    }

    public Builder pushDownFilters(boolean pushDownFilters) {
      this.pushDownFilters = pushDownFilters;
      return this;
    }

    public Builder blockSize(Integer blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public Builder nearest(Query nearest) {
      this.nearest = nearest;
      return this;
    }

    public Builder nearest(String json) {
      try {
        this.nearest = QueryUtils.stringToQuery(json);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse nearest query from json: " + json, e);
      }
      return this;
    }

    public Builder version(Integer version) {
      this.version = version;
      return this;
    }

    public Builder tag(String tag) {
      this.tag = tag;
      return this;
    }

    public Builder indexCacheSize(Integer indexCacheSize) {
      this.indexCacheSize = indexCacheSize;
      return this;
    }

    public Builder metadataCacheSize(Integer metadataCacheSize) {
      this.metadataCacheSize = metadataCacheSize;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder topNPushDown(boolean topNPushDown) {
      this.topNPushDown = topNPushDown;
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
     * Parses options from a map, extracting read-specific settings.
     *
     * @param options the options map
     * @return this builder
     */
    public Builder fromOptions(Map<String, String> options) {
      this.storageOptions = new HashMap<>(options);
      if (options.containsKey(CONFIG_PUSH_DOWN_FILTERS)) {
        this.pushDownFilters = Boolean.parseBoolean(options.get(CONFIG_PUSH_DOWN_FILTERS));
      }
      if (options.containsKey(CONFIG_BLOCK_SIZE)) {
        this.blockSize = Integer.parseInt(options.get(CONFIG_BLOCK_SIZE));
      }
      if (options.containsKey(CONFIG_VERSION)) {
        String versionValue = options.get(CONFIG_VERSION);
        try {
          this.version = Integer.parseInt(versionValue);
        } catch (NumberFormatException e) {
          this.tag = versionValue;
        }
      }
      if (options.containsKey(CONFIG_INDEX_CACHE_SIZE)) {
        this.indexCacheSize = Integer.parseInt(options.get(CONFIG_INDEX_CACHE_SIZE));
      }
      if (options.containsKey(CONFIG_METADATA_CACHE_SIZE)) {
        this.metadataCacheSize = Integer.parseInt(options.get(CONFIG_METADATA_CACHE_SIZE));
      }
      if (options.containsKey(CONFIG_BATCH_SIZE)) {
        int parsedBatchSize = Integer.parseInt(options.get(CONFIG_BATCH_SIZE));
        Preconditions.checkArgument(parsedBatchSize > 0, "batch_size must be positive");
        this.batchSize = parsedBatchSize;
      }
      if (options.containsKey(CONFIG_TOP_N_PUSH_DOWN)) {
        this.topNPushDown = Boolean.parseBoolean(options.get(CONFIG_TOP_N_PUSH_DOWN));
      }
      if (options.containsKey(CONFIG_NEAREST)) {
        String json = options.get(CONFIG_NEAREST);
        nearest(json);
      }
      return this;
    }

    /**
     * Merges catalog config options as defaults (read options override).
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

    public LanceSparkReadOptions build() {
      if (datasetUri == null) {
        throw new IllegalArgumentException("datasetUri is required");
      }
      return new LanceSparkReadOptions(this);
    }
  }
}
