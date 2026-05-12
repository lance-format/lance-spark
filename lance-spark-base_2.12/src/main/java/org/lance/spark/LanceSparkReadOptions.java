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
import org.lance.ipc.Query;
import org.lance.namespace.LanceNamespace;
import org.lance.spark.utils.QueryUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

  /** Per-scan kill-switch for CBO column-stats reporting. */
  public static final String CONFIG_CBO_COLUMN_STATS_ENABLED = "cbo.column.stats.enabled";

  /**
   * Per-scan cap on the number of projected columns for which we load and aggregate zonemap stats
   * during {@link org.apache.spark.sql.connector.read.Statistics#columnStats()}. Bounds driver-side
   * memory + I/O.
   */
  public static final String CONFIG_CBO_COLUMN_STATS_MAX_COLUMNS = "cbo.column.stats.max.columns";

  public static final String CONFIG_NEAREST = "nearest";

  /**
   * Whether executors should rebuild the namespace client and re-fetch storage options via {@code
   * namespace.describeTable()} when opening a dataset for fragment scans.
   *
   * <p>When {@code true} (the default), executors reconstruct the namespace client and route the
   * dataset open through the namespace path. This keeps the Rust-side storage-options provider
   * attached so that short-lived vended credentials returned by {@code describeTable()} (e.g. STS
   * tokens from Iceberg REST, Polaris, Unity) can be refreshed mid-scan.
   *
   * <p>When {@code false}, executors open the dataset directly by URI using the storage options the
   * driver already obtained (passed in via {@code initialStorageOptions}). This skips the eager
   * {@code describeTable()} RPC on every fragment scan, which is required for catalogs whose
   * backing service authenticates per-call (e.g. Hive Metastore over Kerberos): executors typically
   * do not have a Kerberos TGT and the call would otherwise fail with {@code GSS initiate failed}.
   *
   * <p>Whether disabling this option actually costs anything depends on the namespace impl:
   *
   * <ul>
   *   <li>{@code Hive2Namespace} / {@code Hive3Namespace}: {@code describeTable()} returns only the
   *       table location, never storage options. The refresh callback is a no-op, so setting this
   *       option to {@code false} has no downside. The underlying object-store credentials (e.g.
   *       IAM-role / {@code hive-site.xml} / env-vars on the executor) are rotated by the storage
   *       client SDK independently of Lance.
   *   <li>{@code GlueNamespace}: storage options come from a static {@code
   *       config.getStorageOptions()} and are typically not time-bound; setting {@code false} is
   *       usually safe unless you rely on LakeFormation-vended temporary credentials.
   *   <li>{@code IcebergNamespace} (REST), {@code PolarisNamespace}, {@code UnityNamespace}: {@code
   *       describeTable()} commonly returns vended temporary credentials. Leave this option at the
   *       default ({@code true}) unless every scan is guaranteed to finish within the credential
   *       TTL.
   * </ul>
   */
  public static final String CONFIG_EXECUTOR_CREDENTIAL_REFRESH = "executor_credential_refresh";

  public static final String LANCE_FILE_SUFFIX = ".lance";

  private static final boolean DEFAULT_PUSH_DOWN_FILTERS = true;
  // Changed from 512 to 8192 for better OLAP scan performance (33x improvement)
  private static final int DEFAULT_BATCH_SIZE = 8192;
  private static final boolean DEFAULT_TOP_N_PUSH_DOWN = true;
  private static final boolean DEFAULT_EXECUTOR_CREDENTIAL_REFRESH = true;
  private static final boolean DEFAULT_CBO_COLUMN_STATS_ENABLED = true;
  // Tuned at SF=10 TPC-DS: cap=8 keeps colStats coverage at 83% (filter columns always
  // load; the cap only bounds extra projected columns) while cutting per-scan zone-stats
  // load ~8× vs cap=64. Net runtime moves from −29% (cap=8) to +38% (cap=64) with no
  // change in plan shape — driver-side I/O is the binding constraint, not coverage breadth.
  private static final int DEFAULT_CBO_COLUMN_STATS_MAX_COLUMNS = 8;

  private final String datasetUri;
  private final String dbPath;
  private final String datasetName;
  private final boolean pushDownFilters;
  private final Integer blockSize;
  private final Integer version;
  private final Integer indexCacheSize;
  private final Integer metadataCacheSize;
  private final int batchSize;
  private transient Query nearest;
  private final boolean topNPushDown;
  private final boolean cboColumnStatsEnabled;
  private final int cboColumnStatsMaxColumns;
  private final Map<String, String> storageOptions;

  /** The namespace for credential vending. Transient as LanceNamespace is not serializable. */
  private transient LanceNamespace namespace;

  /** The table identifier within the namespace, used for credential refresh. */
  private final List<String> tableId;

  /** The catalog name for cache isolation when multiple catalogs are configured. */
  private final String catalogName;

  /**
   * Whether executors should rebuild the namespace client for credential refresh. See {@link
   * #CONFIG_EXECUTOR_CREDENTIAL_REFRESH} for details.
   */
  private final boolean executorCredentialRefresh;

  private LanceSparkReadOptions(Builder builder) {
    this.datasetUri = builder.datasetUri;
    String[] paths = extractDbPathAndDatasetName(datasetUri);
    this.dbPath = paths[0];
    this.datasetName = paths[1];
    this.pushDownFilters = builder.pushDownFilters;
    this.blockSize = builder.blockSize;
    this.version = builder.version;
    this.indexCacheSize = builder.indexCacheSize;
    this.metadataCacheSize = builder.metadataCacheSize;
    this.batchSize = builder.batchSize;
    this.nearest = builder.nearest;
    this.topNPushDown = builder.topNPushDown;
    this.cboColumnStatsEnabled = builder.cboColumnStatsEnabled;
    this.cboColumnStatsMaxColumns = builder.cboColumnStatsMaxColumns;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.namespace = builder.namespace;
    this.tableId = builder.tableId;
    this.catalogName = builder.catalogName;
    this.executorCredentialRefresh = builder.executorCredentialRefresh;
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

  public boolean isCboColumnStatsEnabled() {
    return cboColumnStatsEnabled;
  }

  public int getCboColumnStatsMaxColumns() {
    return cboColumnStatsMaxColumns;
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

  public String getCatalogName() {
    return catalogName;
  }

  /**
   * Returns whether executors should rebuild the namespace client and route the dataset open
   * through the namespace path (for credential refresh). See {@link
   * #CONFIG_EXECUTOR_CREDENTIAL_REFRESH}.
   */
  public boolean isExecutorCredentialRefresh() {
    return executorCredentialRefresh;
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
   * Creates a copy of this options with a different version.
   *
   * <p>This is used to pin the version during scan planning for snapshot isolation.
   *
   * @param newVersion the version to use
   * @return a new LanceSparkReadOptions with the specified version
   */
  public LanceSparkReadOptions withVersion(int newVersion) {
    return builder()
        .datasetUri(this.datasetUri)
        .pushDownFilters(this.pushDownFilters)
        .blockSize(this.blockSize)
        .version(newVersion)
        .indexCacheSize(this.indexCacheSize)
        .metadataCacheSize(this.metadataCacheSize)
        .batchSize(this.batchSize)
        .nearest(this.nearest)
        .topNPushDown(this.topNPushDown)
        .cboColumnStatsEnabled(this.cboColumnStatsEnabled)
        .cboColumnStatsMaxColumns(this.cboColumnStatsMaxColumns)
        .storageOptions(this.storageOptions)
        .namespace(this.namespace)
        .tableId(this.tableId)
        .catalogName(this.catalogName)
        .executorCredentialRefresh(this.executorCredentialRefresh)
        .build();
  }

  /**
   * Converts this to Lance ReadOptions for the native library.
   *
   * @return ReadOptions for the Lance native library
   */
  public ReadOptions toReadOptions() {
    ReadOptions.Builder builder = new ReadOptions.Builder();
    builder.setSession(LanceRuntime.session());
    if (blockSize != null) {
      builder.setBlockSize(blockSize);
    }
    if (version != null) {
      builder.setVersion(version);
    }
    if (indexCacheSize != null) {
      builder.setIndexCacheSize(indexCacheSize);
    }
    if (metadataCacheSize != null) {
      builder.setMetadataCacheSize(metadataCacheSize);
    }
    if (!storageOptions.isEmpty()) {
      builder.setStorageOptions(storageOptions);
    }
    return builder.build();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeObject(QueryUtils.queryToString(nearest));
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    String json = (String) in.readObject();
    this.nearest = QueryUtils.stringToQuery(json);
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
        && executorCredentialRefresh == that.executorCredentialRefresh
        && Objects.equals(nearest, that.nearest)
        && Objects.equals(datasetUri, that.datasetUri)
        && Objects.equals(blockSize, that.blockSize)
        && Objects.equals(version, that.version)
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
        indexCacheSize,
        metadataCacheSize,
        batchSize,
        nearest,
        topNPushDown,
        storageOptions,
        tableId,
        executorCredentialRefresh);
  }

  /** Builder for creating LanceSparkReadOptions instances. */
  public static class Builder {
    private String datasetUri;
    private boolean pushDownFilters = DEFAULT_PUSH_DOWN_FILTERS;
    private Integer blockSize;
    private Query nearest;
    private Integer version;
    private Integer indexCacheSize;
    private Integer metadataCacheSize;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean topNPushDown = DEFAULT_TOP_N_PUSH_DOWN;
    private boolean cboColumnStatsEnabled = DEFAULT_CBO_COLUMN_STATS_ENABLED;
    private int cboColumnStatsMaxColumns = DEFAULT_CBO_COLUMN_STATS_MAX_COLUMNS;
    private Map<String, String> storageOptions = new HashMap<>();
    private LanceNamespace namespace;
    private List<String> tableId;
    private String catalogName;
    private boolean executorCredentialRefresh = DEFAULT_EXECUTOR_CREDENTIAL_REFRESH;

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

    public Builder cboColumnStatsEnabled(boolean cboColumnStatsEnabled) {
      this.cboColumnStatsEnabled = cboColumnStatsEnabled;
      return this;
    }

    public Builder cboColumnStatsMaxColumns(int cboColumnStatsMaxColumns) {
      Preconditions.checkArgument(
          cboColumnStatsMaxColumns >= 0, "cbo.column.stats.max.columns must be >= 0");
      this.cboColumnStatsMaxColumns = cboColumnStatsMaxColumns;
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

    public Builder catalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    public Builder executorCredentialRefresh(boolean executorCredentialRefresh) {
      this.executorCredentialRefresh = executorCredentialRefresh;
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
      parseTypedFlags(options);
      return this;
    }

    /**
     * Merges catalog config options as defaults (read options override).
     *
     * <p>Also promotes recognized typed flags from the catalog config into their corresponding
     * Builder fields so that catalog-level settings (e.g. {@code spark.sql.catalog.<name>.<key>})
     * take effect on paths that do not later go through {@link #fromOptions(Map)} — notably SQL DML
     * (DELETE / UPDATE / MERGE INTO) and plain SELECT without per-read {@code .option(...)}.
     *
     * @param catalogConfig the catalog config
     * @return this builder
     */
    public Builder withCatalogDefaults(LanceSparkCatalogConfig catalogConfig) {
      // Merge storage options: catalog options are defaults, current options override
      Map<String, String> merged = new HashMap<>(catalogConfig.getStorageOptions());
      merged.putAll(this.storageOptions);
      return fromOptions(merged);
    }

    /**
     * Applies typed-flag parsing for every known read option present in {@code opts}. Shared by
     * {@link #fromOptions(Map)} and {@link #withCatalogDefaults(LanceSparkCatalogConfig)} so that
     * both call sites stay in sync and catalog-level configs reach the typed fields.
     */
    private void parseTypedFlags(Map<String, String> opts) {
      if (opts.containsKey(CONFIG_PUSH_DOWN_FILTERS)) {
        this.pushDownFilters = Boolean.parseBoolean(opts.get(CONFIG_PUSH_DOWN_FILTERS));
      }
      if (opts.containsKey(CONFIG_BLOCK_SIZE)) {
        this.blockSize = Integer.parseInt(opts.get(CONFIG_BLOCK_SIZE));
      }
      if (opts.containsKey(CONFIG_VERSION)) {
        this.version = Integer.parseInt(opts.get(CONFIG_VERSION));
      }
      if (opts.containsKey(CONFIG_INDEX_CACHE_SIZE)) {
        this.indexCacheSize = Integer.parseInt(opts.get(CONFIG_INDEX_CACHE_SIZE));
      }
      if (opts.containsKey(CONFIG_METADATA_CACHE_SIZE)) {
        this.metadataCacheSize = Integer.parseInt(opts.get(CONFIG_METADATA_CACHE_SIZE));
      }
      if (opts.containsKey(CONFIG_BATCH_SIZE)) {
        int parsedBatchSize = Integer.parseInt(opts.get(CONFIG_BATCH_SIZE));
        Preconditions.checkArgument(parsedBatchSize > 0, "batch_size must be positive");
        this.batchSize = parsedBatchSize;
      }
      if (opts.containsKey(CONFIG_TOP_N_PUSH_DOWN)) {
        this.topNPushDown = Boolean.parseBoolean(opts.get(CONFIG_TOP_N_PUSH_DOWN));
      }
      if (opts.containsKey(CONFIG_NEAREST)) {
        nearest(opts.get(CONFIG_NEAREST));
      }
      if (opts.containsKey(CONFIG_EXECUTOR_CREDENTIAL_REFRESH)) {
        this.executorCredentialRefresh =
            Boolean.parseBoolean(opts.get(CONFIG_EXECUTOR_CREDENTIAL_REFRESH));
      }
      if (opts.containsKey(CONFIG_CBO_COLUMN_STATS_ENABLED)) {
        this.cboColumnStatsEnabled =
            Boolean.parseBoolean(opts.get(CONFIG_CBO_COLUMN_STATS_ENABLED));
      }
      if (opts.containsKey(CONFIG_CBO_COLUMN_STATS_MAX_COLUMNS)) {
        int parsed = Integer.parseInt(opts.get(CONFIG_CBO_COLUMN_STATS_MAX_COLUMNS));
        Preconditions.checkArgument(parsed >= 0, "cbo.column.stats.max.columns must be >= 0");
        this.cboColumnStatsMaxColumns = parsed;
      }
    }

    public LanceSparkReadOptions build() {
      if (datasetUri == null) {
        throw new IllegalArgumentException("datasetUri is required");
      }
      return new LanceSparkReadOptions(this);
    }
  }
}
