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
import org.lance.ipc.FullTextQuery;
import org.lance.namespace.LanceNamespace;
import org.lance.spark.utils.FullTextQueryUtils;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
  private static final long serialVersionUID = 5L;

  public static final String CONFIG_DATASET_URI = "path";
  public static final String CONFIG_PUSH_DOWN_FILTERS = "pushDownFilters";
  public static final String CONFIG_BLOCK_SIZE = "block_size";
  public static final String CONFIG_VERSION = "version";
  public static final String CONFIG_BRANCH = "branch";
  public static final String CONFIG_TAG = "tag";
  public static final String CONFIG_INDEX_CACHE_SIZE = "index_cache_size";
  public static final String CONFIG_METADATA_CACHE_SIZE = "metadata_cache_size";
  public static final String CONFIG_BATCH_SIZE = "batch_size";
  public static final String CONFIG_USE_SCALAR_INDEX = "use_scalar_index";
  public static final String CONFIG_RUNTIME_FILTER_MAX_KEYS = "runtime_filter_max_keys";
  public static final String CONFIG_RUNTIME_FILTER_MAX_BYTES = "runtime_filter_max_bytes";
  public static final String CONFIG_TOP_N_PUSH_DOWN = "topN_push_down";
  public static final String CONFIG_NEAREST = "nearest";
  public static final String CONFIG_FULL_TEXT_QUERY = "fullTextQuery";

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

  /** Enable Arrow batches when supported, otherwise use the regular reader. Default: false. */
  public static final String CONFIG_ARROW_BATCH_READS = "arrow_batch_reads";

  public static final String SPARK_SESSION_CONFIG_PREFIX = "spark.sql.lance.";

  private static final List<String> READ_OPTION_KEYS =
      Collections.unmodifiableList(
          Arrays.asList(
              CONFIG_DATASET_URI,
              CONFIG_PUSH_DOWN_FILTERS,
              CONFIG_BLOCK_SIZE,
              CONFIG_VERSION,
              CONFIG_BRANCH,
              CONFIG_TAG,
              CONFIG_INDEX_CACHE_SIZE,
              CONFIG_METADATA_CACHE_SIZE,
              CONFIG_BATCH_SIZE,
              CONFIG_USE_SCALAR_INDEX,
              CONFIG_RUNTIME_FILTER_MAX_KEYS,
              CONFIG_RUNTIME_FILTER_MAX_BYTES,
              CONFIG_TOP_N_PUSH_DOWN,
              CONFIG_NEAREST,
              CONFIG_FULL_TEXT_QUERY,
              CONFIG_EXECUTOR_CREDENTIAL_REFRESH,
              CONFIG_ARROW_BATCH_READS));

  private static final List<String> SESSION_OPTION_KEYS =
      Collections.unmodifiableList(
          Arrays.asList(
              CONFIG_PUSH_DOWN_FILTERS,
              CONFIG_BLOCK_SIZE,
              CONFIG_VERSION,
              CONFIG_BRANCH,
              CONFIG_TAG,
              CONFIG_INDEX_CACHE_SIZE,
              CONFIG_METADATA_CACHE_SIZE,
              CONFIG_BATCH_SIZE,
              CONFIG_USE_SCALAR_INDEX,
              CONFIG_RUNTIME_FILTER_MAX_KEYS,
              CONFIG_RUNTIME_FILTER_MAX_BYTES,
              CONFIG_TOP_N_PUSH_DOWN,
              CONFIG_FULL_TEXT_QUERY,
              CONFIG_EXECUTOR_CREDENTIAL_REFRESH,
              CONFIG_ARROW_BATCH_READS,
              LanceSparkWriteOptions.CONFIG_WRITE_MODE,
              LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_FILE,
              LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_GROUP,
              LanceSparkWriteOptions.CONFIG_MAX_BYTES_PER_FILE,
              LanceSparkWriteOptions.CONFIG_FILE_FORMAT_VERSION,
              LanceSparkWriteOptions.CONFIG_USE_QUEUED_WRITE_BUFFER,
              LanceSparkWriteOptions.CONFIG_QUEUE_DEPTH,
              LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS,
              LanceSparkWriteOptions.CONFIG_USE_LARGE_VAR_TYPES,
              LanceSparkWriteOptions.CONFIG_MAX_BATCH_BYTES,
              LanceSparkWriteOptions.CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD));

  static final Set<String> RECOGNIZED_READ_OPTION_LOWER_KEYS = buildLowerKeySet(READ_OPTION_KEYS);

  private static final Map<String, String> CANONICAL_SESSION_OPTION_KEYS =
      buildCanonicalKeyMap(SESSION_OPTION_KEYS);

  private static Set<String> buildLowerKeySet(List<String> keys) {
    Set<String> set = new HashSet<>(keys.size());
    for (String key : keys) {
      set.add(key.toLowerCase(Locale.ROOT));
    }
    return Collections.unmodifiableSet(set);
  }

  private static Map<String, String> buildCanonicalKeyMap(List<String> keys) {
    Map<String, String> map = new HashMap<>(keys.size());
    for (String key : keys) {
      map.put(key.toLowerCase(Locale.ROOT), key);
    }
    return Collections.unmodifiableMap(map);
  }

  public static final String LANCE_FILE_SUFFIX = ".lance";

  private static final boolean DEFAULT_PUSH_DOWN_FILTERS = true;
  // Changed from 512 to 8192 for better OLAP scan performance (33x improvement)
  private static final int DEFAULT_BATCH_SIZE = 8192;
  private static final boolean DEFAULT_USE_SCALAR_INDEX = true;
  private static final int DEFAULT_RUNTIME_FILTER_MAX_KEYS = 100_000;
  private static final int DEFAULT_RUNTIME_FILTER_MAX_BYTES = 4_194_304;
  private static final boolean DEFAULT_TOP_N_PUSH_DOWN = true;
  private static final boolean DEFAULT_EXECUTOR_CREDENTIAL_REFRESH = true;
  private static final boolean DEFAULT_ARROW_BATCH_READS = false;

  private final String datasetUri;
  private final String datasetName;
  private final boolean pushDownFilters;
  private final Integer blockSize;
  private final LanceRef ref;
  private final Integer indexCacheSize;
  private final Integer metadataCacheSize;
  private final int batchSize;
  private transient FullTextQuery fullTextQuery;
  private final boolean useScalarIndex;
  private final int runtimeFilterMaxKeys;
  private final int runtimeFilterMaxBytes;
  private final boolean topNPushDown;
  private final Map<String, String> storageOptions;

  /** The namespace for credential vending. Transient as LanceNamespace is not serializable. */
  private transient LanceNamespace namespace;

  /** The table identifier within the namespace, used for credential refresh. */
  private final List<String> tableId;

  /** The catalog name for cache isolation when multiple catalogs are configured. */
  private final String catalogName;

  /** Registered cache backend URIs for the process-local catalog session. */
  private final String indexCacheBackend;

  private final String metadataCacheBackend;

  /**
   * Whether executors should rebuild the namespace client for credential refresh. See {@link
   * #CONFIG_EXECUTOR_CREDENTIAL_REFRESH} for details.
   */
  private final boolean executorCredentialRefresh;

  private final boolean arrowBatchReads;

  private LanceSparkReadOptions(Builder builder) {
    this.datasetUri = builder.datasetUri;
    this.datasetName = extractDatasetName(datasetUri);
    this.pushDownFilters = builder.pushDownFilters;
    this.blockSize = builder.blockSize;
    this.ref = builder.ref;
    this.indexCacheSize = builder.indexCacheSize;
    this.metadataCacheSize = builder.metadataCacheSize;
    this.batchSize = builder.batchSize;
    this.fullTextQuery = builder.fullTextQuery;
    this.useScalarIndex = builder.useScalarIndex;
    this.runtimeFilterMaxKeys = builder.runtimeFilterMaxKeys;
    this.runtimeFilterMaxBytes = builder.runtimeFilterMaxBytes;
    this.topNPushDown = builder.topNPushDown;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.namespace = builder.namespace;
    this.tableId = builder.tableId;
    this.catalogName = builder.catalogName;
    this.indexCacheBackend = builder.indexCacheBackend;
    this.metadataCacheBackend = builder.metadataCacheBackend;
    this.executorCredentialRefresh = builder.executorCredentialRefresh;
    this.arrowBatchReads = builder.arrowBatchReads;
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
    CaseInsensitiveStringMap ciProps = new CaseInsensitiveStringMap(properties);
    String datasetUri = ciProps.get(CONFIG_DATASET_URI);
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
    return builder().datasetUri(datasetUri).fromOptions(Collections.emptyMap()).build();
  }

  // ========== Helper methods ==========

  /**
   * Strips a single pair of matching surrounding single ({@code '...'}) or double ({@code "..."})
   * quotes after trimming whitespace. Spark SQL {@code SET key = 'value'} retains the surrounding
   * quotes in the raw configuration value.
   */
  public static String stripSurroundingQuotes(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.length() >= 2) {
      char first = trimmed.charAt(0);
      char last = trimmed.charAt(trimmed.length() - 1);
      if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
        return trimmed.substring(1, trimmed.length() - 1).trim();
      }
    }
    return trimmed;
  }

  /**
   * Extracts recognized {@code spark.sql.lance.<key>} session configurations from the active or
   * default {@link SparkSession}, stripping any surrounding quotes added by Spark SQL {@code SET}.
   */
  public static Map<String, String> extractActiveSessionOptions() {
    try {
      Option<SparkSession> sessionOpt = SparkSession.getActiveSession();
      if (sessionOpt.isEmpty()) {
        sessionOpt = SparkSession.getDefaultSession();
      }
      if (sessionOpt.isEmpty()) {
        return Collections.emptyMap();
      }
      SparkSession spark = sessionOpt.get();
      if (spark.sparkContext().isStopped()) {
        return Collections.emptyMap();
      }
      Map<String, String> sessionOpts = new HashMap<>();
      Iterator<Tuple2<String, String>> iter = spark.conf().getAll().iterator();
      while (iter.hasNext()) {
        Tuple2<String, String> entry = iter.next();
        String confKey = entry._1();
        String confVal = entry._2();
        if (confKey != null
            && confVal != null
            && confKey.regionMatches(
                true, 0, SPARK_SESSION_CONFIG_PREFIX, 0, SPARK_SESSION_CONFIG_PREFIX.length())) {
          String optName = confKey.substring(SPARK_SESSION_CONFIG_PREFIX.length());
          String canonical = CANONICAL_SESSION_OPTION_KEYS.get(optName.toLowerCase(Locale.ROOT));
          if (canonical != null) {
            sessionOpts.put(canonical, stripSurroundingQuotes(confVal));
          }
        }
      }
      return sessionOpts;
    } catch (Exception e) {
      return Collections.emptyMap();
    }
  }

  /**
   * Merges {@code overrides} into {@code target}, matching recognized connector option keys
   * case-insensitively so that higher-precedence options replace lower-precedence entries
   * regardless of key casing while preserving case-sensitive storage option keys.
   */
  static void mergeRecognizedOptions(
      Map<String, String> target,
      Map<String, String> overrides,
      Set<String> recognizedLowerKeys,
      boolean clearConflictingRefs) {
    if (overrides == null || overrides.isEmpty()) {
      return;
    }
    if (clearConflictingRefs) {
      boolean hasVersion = false;
      boolean hasBranch = false;
      boolean hasTag = false;
      for (String key : overrides.keySet()) {
        if (key == null) {
          continue;
        }
        if (key.equalsIgnoreCase(CONFIG_VERSION)) {
          hasVersion = true;
        } else if (key.equalsIgnoreCase(CONFIG_BRANCH)) {
          hasBranch = true;
        } else if (key.equalsIgnoreCase(CONFIG_TAG)) {
          hasTag = true;
        }
      }
      int refCount = (hasVersion ? 1 : 0) + (hasBranch ? 1 : 0) + (hasTag ? 1 : 0);
      Preconditions.checkArgument(refCount <= 1, "Specify only one of branch, tag, or version");
      if (refCount == 1) {
        target
            .keySet()
            .removeIf(
                existing ->
                    existing != null
                        && (existing.equalsIgnoreCase(CONFIG_VERSION)
                            || existing.equalsIgnoreCase(CONFIG_BRANCH)
                            || existing.equalsIgnoreCase(CONFIG_TAG)));
      }
    }
    for (Map.Entry<String, String> entry : overrides.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key != null && recognizedLowerKeys.contains(key.toLowerCase(Locale.ROOT))) {
        target.keySet().removeIf(existing -> existing != null && existing.equalsIgnoreCase(key));
      }
      target.put(key, value);
    }
  }

  private static String extractDatasetName(String datasetUri) {
    if (datasetUri == null) {
      throw new IllegalArgumentException("The dataset uri should not be null");
    }

    int lastSlashIndex = datasetUri.lastIndexOf('/');
    if (lastSlashIndex == -1) {
      throw new IllegalArgumentException("Invalid dataset uri: " + datasetUri);
    }

    String datasetNameWithSuffix = datasetUri.substring(lastSlashIndex + 1);
    if (datasetUri.endsWith(LANCE_FILE_SUFFIX)) {
      return datasetNameWithSuffix.substring(
          0, datasetNameWithSuffix.length() - LANCE_FILE_SUFFIX.length());
    }
    return datasetNameWithSuffix;
  }

  // ========== Getters ==========

  public String getDatasetUri() {
    return datasetUri;
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

  public LanceRef getRef() {
    return ref;
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

  public FullTextQuery getFullTextQuery() {
    return fullTextQuery;
  }

  public boolean isUseScalarIndex() {
    return useScalarIndex;
  }

  public int getRuntimeFilterMaxKeys() {
    return runtimeFilterMaxKeys;
  }

  public int getRuntimeFilterMaxBytes() {
    return runtimeFilterMaxBytes;
  }

  public boolean isTopNPushDown() {
    return topNPushDown;
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

  public String getCatalogName() {
    return catalogName;
  }

  public String getIndexCacheBackend() {
    return indexCacheBackend;
  }

  public String getMetadataCacheBackend() {
    return metadataCacheBackend;
  }

  /**
   * Returns whether executors should rebuild the namespace client and route the dataset open
   * through the namespace path (for credential refresh). See {@link
   * #CONFIG_EXECUTOR_CREDENTIAL_REFRESH}.
   */
  public boolean isExecutorCredentialRefresh() {
    return executorCredentialRefresh;
  }

  /** Returns whether Arrow batch reads are enabled. See {@link #CONFIG_ARROW_BATCH_READS}. */
  public boolean isArrowBatchReadsEnabled() {
    return arrowBatchReads;
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
   * Creates a copy of these options with a different reference.
   *
   * <p>This is used to pin the resolved reference during scan planning for snapshot isolation.
   *
   * @param newRef the reference to use
   * @return new read options with the specified reference
   */
  public LanceSparkReadOptions withRef(LanceRef newRef) {
    return toBuilder().ref(newRef).build();
  }

  /** Creates a copy of these options with {@link #CONFIG_ARROW_BATCH_READS} set as given. */
  public LanceSparkReadOptions withArrowBatchReads(boolean arrowBatchReads) {
    return toBuilder().arrowBatchReads(arrowBatchReads).build();
  }

  /** Creates a builder holding these options. */
  public Builder toBuilder() {
    return builder()
        .datasetUri(this.datasetUri)
        .pushDownFilters(this.pushDownFilters)
        .blockSize(this.blockSize)
        .ref(this.ref)
        .indexCacheSize(this.indexCacheSize)
        .metadataCacheSize(this.metadataCacheSize)
        .batchSize(this.batchSize)
        .fullTextQuery(this.fullTextQuery)
        .useScalarIndex(this.useScalarIndex)
        .runtimeFilterMaxKeys(this.runtimeFilterMaxKeys)
        .runtimeFilterMaxBytes(this.runtimeFilterMaxBytes)
        .topNPushDown(this.topNPushDown)
        .storageOptions(this.storageOptions)
        .namespace(this.namespace)
        .tableId(this.tableId)
        .catalogName(this.catalogName)
        .indexCacheBackend(this.indexCacheBackend)
        .metadataCacheBackend(this.metadataCacheBackend)
        .executorCredentialRefresh(this.executorCredentialRefresh)
        .arrowBatchReads(this.arrowBatchReads);
  }

  /**
   * Converts this to Lance ReadOptions for the native library.
   *
   * @return ReadOptions for the Lance native library
   */
  public ReadOptions toReadOptions() {
    ReadOptions.Builder builder = new ReadOptions.Builder();
    builder.setSession(LanceRuntime.session(catalogName, indexCacheBackend, metadataCacheBackend));
    if (blockSize != null) {
      builder.setBlockSize(blockSize);
    }
    if (ref != null && ref.isMain()) {
      ref.getVersionNumber().ifPresent(builder::setVersion);
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
    out.writeObject(FullTextQueryUtils.fullTextQueryToString(fullTextQuery));
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    String ftsJson = (String) in.readObject();
    this.fullTextQuery = FullTextQueryUtils.stringToFullTextQuery(ftsJson);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceSparkReadOptions that = (LanceSparkReadOptions) o;
    return pushDownFilters == that.pushDownFilters
        && batchSize == that.batchSize
        && useScalarIndex == that.useScalarIndex
        && runtimeFilterMaxKeys == that.runtimeFilterMaxKeys
        && runtimeFilterMaxBytes == that.runtimeFilterMaxBytes
        && topNPushDown == that.topNPushDown
        && executorCredentialRefresh == that.executorCredentialRefresh
        && arrowBatchReads == that.arrowBatchReads
        && FullTextQueryUtils.equals(fullTextQuery, that.fullTextQuery)
        && Objects.equals(datasetUri, that.datasetUri)
        && Objects.equals(blockSize, that.blockSize)
        && Objects.equals(ref, that.ref)
        && Objects.equals(indexCacheSize, that.indexCacheSize)
        && Objects.equals(metadataCacheSize, that.metadataCacheSize)
        && Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(tableId, that.tableId)
        && Objects.equals(catalogName, that.catalogName)
        && Objects.equals(indexCacheBackend, that.indexCacheBackend)
        && Objects.equals(metadataCacheBackend, that.metadataCacheBackend);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        datasetUri,
        pushDownFilters,
        blockSize,
        ref,
        indexCacheSize,
        metadataCacheSize,
        batchSize,
        FullTextQueryUtils.fullTextQueryToString(fullTextQuery),
        useScalarIndex,
        runtimeFilterMaxKeys,
        runtimeFilterMaxBytes,
        topNPushDown,
        storageOptions,
        tableId,
        catalogName,
        indexCacheBackend,
        metadataCacheBackend,
        executorCredentialRefresh,
        arrowBatchReads);
  }

  /** Builder for creating LanceSparkReadOptions instances. */
  public static class Builder {
    private String datasetUri;
    private boolean pushDownFilters = DEFAULT_PUSH_DOWN_FILTERS;
    private Integer blockSize;
    private FullTextQuery fullTextQuery;
    private LanceRef ref;
    private Integer indexCacheSize;
    private Integer metadataCacheSize;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean useScalarIndex = DEFAULT_USE_SCALAR_INDEX;
    private int runtimeFilterMaxKeys = DEFAULT_RUNTIME_FILTER_MAX_KEYS;
    private int runtimeFilterMaxBytes = DEFAULT_RUNTIME_FILTER_MAX_BYTES;
    private boolean topNPushDown = DEFAULT_TOP_N_PUSH_DOWN;
    private Map<String, String> storageOptions = new HashMap<>();
    private LanceNamespace namespace;
    private List<String> tableId;
    private String catalogName;
    private String indexCacheBackend;
    private String metadataCacheBackend;
    private boolean executorCredentialRefresh = DEFAULT_EXECUTOR_CREDENTIAL_REFRESH;
    private boolean arrowBatchReads = DEFAULT_ARROW_BATCH_READS;

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

    public Builder fullTextQuery(FullTextQuery fullTextQuery) {
      this.fullTextQuery = fullTextQuery;
      return this;
    }

    public Builder ref(LanceRef ref) {
      this.ref = ref;
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

    public Builder useScalarIndex(boolean useScalarIndex) {
      this.useScalarIndex = useScalarIndex;
      return this;
    }

    public Builder runtimeFilterMaxKeys(int runtimeFilterMaxKeys) {
      Preconditions.checkArgument(
          runtimeFilterMaxKeys > 0, "runtime_filter_max_keys must be positive");
      this.runtimeFilterMaxKeys = runtimeFilterMaxKeys;
      return this;
    }

    public Builder runtimeFilterMaxBytes(int runtimeFilterMaxBytes) {
      Preconditions.checkArgument(
          runtimeFilterMaxBytes > 0, "runtime_filter_max_bytes must be positive");
      this.runtimeFilterMaxBytes = runtimeFilterMaxBytes;
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

    public Builder executorCredentialRefresh(boolean executorCredentialRefresh) {
      this.executorCredentialRefresh = executorCredentialRefresh;
      return this;
    }

    public Builder arrowBatchReads(boolean arrowBatchReads) {
      this.arrowBatchReads = arrowBatchReads;
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
      Map<String, String> effective = new HashMap<>(extractActiveSessionOptions());
      mergeRecognizedOptions(effective, options, RECOGNIZED_READ_OPTION_LOWER_KEYS, true);
      parseTypedFlags(effective);
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
      Map<String, String> userOptions = new HashMap<>(this.storageOptions);
      Map<String, String> mergedStorage = new HashMap<>(catalogConfig.getStorageOptions());
      mergeRecognizedOptions(mergedStorage, userOptions, RECOGNIZED_READ_OPTION_LOWER_KEYS, true);
      this.storageOptions = mergedStorage;
      this.indexCacheBackend = catalogConfig.getIndexCacheBackend();
      this.metadataCacheBackend = catalogConfig.getMetadataCacheBackend();

      Map<String, String> effective = new HashMap<>(catalogConfig.getStorageOptions());
      mergeRecognizedOptions(
          effective, extractActiveSessionOptions(), RECOGNIZED_READ_OPTION_LOWER_KEYS, true);
      mergeRecognizedOptions(effective, userOptions, RECOGNIZED_READ_OPTION_LOWER_KEYS, true);
      parseTypedFlags(effective);
      return this;
    }

    /**
     * Applies typed-flag parsing for every known read option present in {@code opts}. Shared by
     * {@link #fromOptions(Map)} and {@link #withCatalogDefaults(LanceSparkCatalogConfig)} so that
     * both call sites stay in sync and catalog-level configs reach the typed fields.
     */
    private void parseTypedFlags(Map<String, String> opts) {
      CaseInsensitiveStringMap ciOpts = new CaseInsensitiveStringMap(opts);
      Preconditions.checkArgument(
          !ciOpts.containsKey(CONFIG_NEAREST),
          "The nearest read option is no longer supported; use VECTOR_SEARCH table function");
      Preconditions.checkArgument(
          (ciOpts.containsKey(CONFIG_VERSION) ? 1 : 0)
                  + (ciOpts.containsKey(CONFIG_BRANCH) ? 1 : 0)
                  + (ciOpts.containsKey(CONFIG_TAG) ? 1 : 0)
              <= 1,
          "Specify only one of branch, tag, or version");
      if (ciOpts.containsKey(CONFIG_PUSH_DOWN_FILTERS)) {
        this.pushDownFilters =
            Boolean.parseBoolean(stripSurroundingQuotes(ciOpts.get(CONFIG_PUSH_DOWN_FILTERS)));
      }
      if (ciOpts.containsKey(CONFIG_BLOCK_SIZE)) {
        this.blockSize = Integer.parseInt(stripSurroundingQuotes(ciOpts.get(CONFIG_BLOCK_SIZE)));
      }
      if (ciOpts.containsKey(CONFIG_VERSION)) {
        this.ref =
            LanceRef.ofMain(Long.parseLong(stripSurroundingQuotes(ciOpts.get(CONFIG_VERSION))));
      } else if (ciOpts.containsKey(CONFIG_BRANCH)) {
        this.ref = LanceRef.ofBranch(stripSurroundingQuotes(ciOpts.get(CONFIG_BRANCH)));
      } else if (ciOpts.containsKey(CONFIG_TAG)) {
        this.ref = LanceRef.ofTag(stripSurroundingQuotes(ciOpts.get(CONFIG_TAG)));
      }
      if (ciOpts.containsKey(CONFIG_INDEX_CACHE_SIZE)) {
        this.indexCacheSize =
            Integer.parseInt(stripSurroundingQuotes(ciOpts.get(CONFIG_INDEX_CACHE_SIZE)));
      }
      if (ciOpts.containsKey(CONFIG_METADATA_CACHE_SIZE)) {
        this.metadataCacheSize =
            Integer.parseInt(stripSurroundingQuotes(ciOpts.get(CONFIG_METADATA_CACHE_SIZE)));
      }
      if (ciOpts.containsKey(CONFIG_BATCH_SIZE)) {
        int parsedBatchSize =
            Integer.parseInt(stripSurroundingQuotes(ciOpts.get(CONFIG_BATCH_SIZE)));
        Preconditions.checkArgument(parsedBatchSize > 0, "batch_size must be positive");
        this.batchSize = parsedBatchSize;
      }
      if (ciOpts.containsKey(CONFIG_TOP_N_PUSH_DOWN)) {
        this.topNPushDown =
            Boolean.parseBoolean(stripSurroundingQuotes(ciOpts.get(CONFIG_TOP_N_PUSH_DOWN)));
      }
      if (ciOpts.containsKey(CONFIG_FULL_TEXT_QUERY)) {
        String json = ciOpts.get(CONFIG_FULL_TEXT_QUERY);
        this.fullTextQuery = FullTextQueryUtils.stringToFullTextQuery(json);
      }
      if (ciOpts.containsKey(CONFIG_USE_SCALAR_INDEX)) {
        this.useScalarIndex =
            Boolean.parseBoolean(stripSurroundingQuotes(ciOpts.get(CONFIG_USE_SCALAR_INDEX)));
      }
      if (ciOpts.containsKey(CONFIG_RUNTIME_FILTER_MAX_KEYS)) {
        runtimeFilterMaxKeys(
            Integer.parseInt(stripSurroundingQuotes(ciOpts.get(CONFIG_RUNTIME_FILTER_MAX_KEYS))));
      }
      if (ciOpts.containsKey(CONFIG_RUNTIME_FILTER_MAX_BYTES)) {
        runtimeFilterMaxBytes(
            Integer.parseInt(stripSurroundingQuotes(ciOpts.get(CONFIG_RUNTIME_FILTER_MAX_BYTES))));
      }
      if (ciOpts.containsKey(CONFIG_EXECUTOR_CREDENTIAL_REFRESH)) {
        this.executorCredentialRefresh =
            Boolean.parseBoolean(
                stripSurroundingQuotes(ciOpts.get(CONFIG_EXECUTOR_CREDENTIAL_REFRESH)));
      }
      if (ciOpts.containsKey(CONFIG_ARROW_BATCH_READS)) {
        this.arrowBatchReads =
            Boolean.parseBoolean(stripSurroundingQuotes(ciOpts.get(CONFIG_ARROW_BATCH_READS)));
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
