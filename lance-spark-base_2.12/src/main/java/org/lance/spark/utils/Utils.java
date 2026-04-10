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
package org.lance.spark.utils;

import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.Version;
import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkCatalogConfig;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Utils {

  public static long parseVersion(String version) {
    return Long.parseUnsignedLong(version);
  }

  public static long findVersion(List<Version> versions, long timestamp) {
    long versionID = -1;
    Instant instant = instantFromTimestamp(timestamp);
    for (Version version : versions) {
      // Truncate version timestamp to microsecond precision to match Spark's
      // microsecond timestamp resolution, avoiding sub-microsecond mismatches
      Instant versionInstant = truncateToMicros(version.getDataTime().toInstant());
      if (versionInstant.compareTo(instant) <= 0) {
        versionID = version.getId();
      } else {
        break;
      }
    }
    if (versionID == -1) {
      throw new IllegalArgumentException("No version found with timestamp: " + timestamp);
    }
    return versionID;
  }

  /** Returns a new builder for opening a dataset from read options. */
  public static OpenDatasetBuilder openDatasetBuilder(LanceSparkReadOptions readOptions) {
    return new OpenDatasetBuilder(readOptions);
  }

  /** Returns a new builder for opening a dataset from write options. */
  public static OpenDatasetBuilder openDatasetBuilder(LanceSparkWriteOptions writeOptions) {
    return new OpenDatasetBuilder(writeOptions);
  }

  /**
   * Builder for dataset opens that merge driver-side {@code initialStorageOptions} into the base
   * storage options and attach a managed {@link LanceRuntime} session.
   */
  public static class OpenDatasetBuilder {
    private final String uri;
    private final LanceNamespace namespace;
    private final List<String> tableId;
    private final Map<String, String> storageOptions;
    private final String catalogName;
    private final Long version;
    private final Integer blockSize;
    private final Integer indexCacheSize;
    private final Integer metadataCacheSize;

    private Map<String, String> initialStorageOptions;

    private OpenDatasetBuilder(LanceSparkReadOptions opts) {
      this.uri = opts.getDatasetUri();
      this.storageOptions = opts.getStorageOptions();
      this.version = opts.getVersion() != null ? opts.getVersion().longValue() : null;
      this.catalogName = opts.getCatalogName();
      this.namespace = opts.getNamespace();
      this.tableId = opts.getTableId();
      this.blockSize = opts.getBlockSize();
      this.indexCacheSize = opts.getIndexCacheSize();
      this.metadataCacheSize = opts.getMetadataCacheSize();
    }

    private OpenDatasetBuilder(LanceSparkWriteOptions opts) {
      this.uri = opts.getDatasetUri();
      this.storageOptions = opts.getStorageOptions();
      this.namespace = opts.getNamespace();
      this.tableId = opts.getTableId();
      this.catalogName = null;
      this.version = opts.getVersion();
      this.blockSize = null;
      this.indexCacheSize = null;
      this.metadataCacheSize = null;
    }

    /** Sets initial storage options from {@code describeTable()}. */
    public OpenDatasetBuilder initialStorageOptions(Map<String, String> initialStorageOptions) {
      this.initialStorageOptions = initialStorageOptions;
      return this;
    }

    public Dataset build() {
      Map<String, String> base = storageOptions != null ? storageOptions : Collections.emptyMap();
      Map<String, String> merged = LanceRuntime.mergeStorageOptions(base, initialStorageOptions);

      ReadOptions.Builder roBuilder =
          new ReadOptions.Builder()
              .setStorageOptions(merged)
              .setSession(
                  catalogName != null ? LanceRuntime.session(catalogName) : LanceRuntime.session());
      if (version != null) {
        roBuilder.setVersion(version);
      }
      if (blockSize != null) {
        roBuilder.setBlockSize(blockSize);
      }
      if (indexCacheSize != null) {
        roBuilder.setIndexCacheSize(indexCacheSize);
      }
      if (metadataCacheSize != null) {
        roBuilder.setMetadataCacheSize(metadataCacheSize);
      }

      if (namespace != null && tableId != null) {
        return Dataset.open()
            .allocator(LanceRuntime.allocator())
            .namespaceClient(namespace)
            .tableId(tableId)
            .readOptions(roBuilder.build())
            .build();
      }
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(uri)
          .readOptions(roBuilder.build())
          .build();
    }
  }

  /**
   * Creates LanceSparkReadOptions for this catalog.
   *
   * @param location the dataset URI
   * @param catalogConfig catalog configuration
   * @param versionId optional dataset version id
   * @param namespace optional namespace for credential vending
   * @param tableId optional table identifier
   * @param catalogName catalog name for cache isolation
   * @return a new LanceSparkReadOptions with catalog settings
   */
  public static LanceSparkReadOptions createReadOptions(
      String location,
      LanceSparkCatalogConfig catalogConfig,
      Optional<Long> versionId,
      Optional<LanceNamespace> namespace,
      Optional<List<String>> tableId,
      String catalogName) {
    LanceSparkReadOptions.Builder builder =
        LanceSparkReadOptions.builder()
            .datasetUri(location)
            .withCatalogDefaults(catalogConfig)
            .catalogName(catalogName);

    if (versionId.isPresent()) {
      builder.version(versionId.get().intValue());
    }
    if (tableId.isPresent()) {
      builder.tableId(tableId.get());
    }
    if (namespace.isPresent()) {
      builder.namespace(namespace.get());
    }

    return builder.build();
  }

  // Determine if the timestamp is in microseconds or nanoseconds and convert to Instant
  private static Instant instantFromTimestamp(long timestamp) {
    if (timestamp <= 0) {
      throw new IllegalArgumentException("Timestamp must be greater than zero");
    }
    return instantFromEpochMicros(timestamp);
  }

  private static Instant instantFromEpochMicros(long epochMicros) {
    long sec = Math.floorDiv(epochMicros, 1_000_000L);
    long nanoAdj = Math.floorMod(epochMicros, 1_000_000L) * 1_000L;
    return Instant.ofEpochSecond(sec, nanoAdj);
  }

  private static Instant truncateToMicros(Instant instant) {
    long nanos = instant.getNano();
    long truncatedNanos = (nanos / 1_000L) * 1_000L;
    return Instant.ofEpochSecond(instant.getEpochSecond(), truncatedNanos);
  }
}
