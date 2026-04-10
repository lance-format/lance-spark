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
package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Cache for Lance datasets with session sharing.
 *
 * <p>This cache ensures:
 *
 * <ul>
 *   <li>Snapshot isolation: datasets are keyed by (URI, version) to ensure all workers see the same
 *       data
 *   <li>Session sharing: all datasets use the global session from LanceRuntime for efficient cache
 *       sharing
 *   <li>Fragment caching: fragments are pre-loaded and cached per dataset
 * </ul>
 */
public class LanceDatasetCache {

  private static final Logger LOG = LoggerFactory.getLogger(LanceDatasetCache.class);

  /** Cached dataset with pre-loaded fragments. */
  public static class CachedDataset {
    private final Dataset dataset;
    private final Map<Integer, Fragment> fragments;

    CachedDataset(Dataset dataset) {
      this.dataset = dataset;
      this.fragments =
          dataset.getFragments().stream().collect(Collectors.toMap(Fragment::getId, f -> f));
    }

    public Dataset getDataset() {
      return dataset;
    }

    public Map<Integer, Fragment> getFragments() {
      return fragments;
    }

    public Fragment getFragment(int fragmentId) {
      return fragments.get(fragmentId);
    }
  }

  /**
   * Cache key for dataset lookup.
   *
   * <p>The key uses (catalogName, URI, version) to ensure immutable dataset caching with
   * per-catalog isolation. The version is always explicit because it's resolved during scan
   * planning - this ensures snapshot isolation. The catalogName ensures that multiple Lance
   * catalogs in the same Spark application have isolated caches.
   */
  public static class DatasetCacheKey {
    private final String catalogName;
    private final String uri;
    private final Long version;
    private final String namespaceImpl;
    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    // Carried for the open call but not used in equals/hashCode.
    private final LanceSparkReadOptions readOptions;
    private final Map<String, String> initialStorageOptions;

    public DatasetCacheKey(
        LanceSparkReadOptions readOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties) {
      this.readOptions = readOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.catalogName =
          readOptions.getCatalogName() != null
              ? readOptions.getCatalogName()
              : LanceRuntime.DEFAULT_CATALOG;
      this.uri = readOptions.getDatasetUri();
      this.version = readOptions.getVersion() != null ? (long) readOptions.getVersion() : null;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = readOptions.getTableId();
    }

    public String getNamespaceImpl() {
      return namespaceImpl;
    }

    public Map<String, String> getNamespaceProperties() {
      return namespaceProperties;
    }

    public List<String> getTableId() {
      return tableId;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DatasetCacheKey that = (DatasetCacheKey) o;
      return Objects.equals(catalogName, that.catalogName)
          && Objects.equals(uri, that.uri)
          && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
      return Objects.hash(catalogName, uri, version);
    }

    @Override
    public String toString() {
      return String.format(
          "DatasetCacheKey(catalog=%s, uri=%s, version=%s)", catalogName, uri, version);
    }
  }

  private static final LoadingCache<DatasetCacheKey, CachedDataset> CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(100)
          .expireAfterAccess(1, TimeUnit.HOURS)
          .removalListener(
              (RemovalListener<DatasetCacheKey, CachedDataset>)
                  notification -> {
                    CachedDataset cached = notification.getValue();
                    if (cached != null && cached.getDataset() != null) {
                      LOG.debug(
                          "Closing cached dataset: {} (reason: {})",
                          notification.getKey(),
                          notification.getCause());
                      cached.getDataset().close();
                    }
                  })
          .build(
              new CacheLoader<DatasetCacheKey, CachedDataset>() {
                @Override
                public CachedDataset load(DatasetCacheKey key) {
                  LOG.debug("Opening dataset for cache: {}", key);
                  Dataset dataset =
                      Utils.openDatasetBuilder(key.readOptions)
                          .initialStorageOptions(key.initialStorageOptions)
                          .build();
                  return new CachedDataset(dataset);
                }
              });

  private LanceDatasetCache() {}

  /**
   * Gets a cached dataset for the given read options.
   *
   * <p>The version in readOptions should always be explicit (resolved during scan planning) to
   * ensure snapshot isolation. A dataset at a specific version is immutable, so caching by (URI,
   * version) is safe.
   *
   * @param readOptions the read options (must have explicit version for snapshot isolation)
   * @param initialStorageOptions initial storage options from describeTable()
   * @param namespaceImpl namespace implementation type
   * @param namespaceProperties namespace connection properties
   * @return the cached dataset
   */
  public static CachedDataset getDataset(
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    DatasetCacheKey key =
        new DatasetCacheKey(readOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
    try {
      return CACHE.get(key);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to get cached dataset: " + key, e);
    }
  }

  /**
   * Gets a fragment from the cached dataset.
   *
   * @param readOptions the read options (must have explicit version for snapshot isolation)
   * @param fragmentId the fragment ID
   * @param initialStorageOptions initial storage options from describeTable()
   * @param namespaceImpl namespace implementation type
   * @param namespaceProperties namespace connection properties
   * @return the fragment
   */
  public static Fragment getFragment(
      LanceSparkReadOptions readOptions,
      int fragmentId,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    CachedDataset cached =
        getDataset(readOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
    Fragment fragment = cached.getFragment(fragmentId);
    if (fragment == null) {
      throw new IllegalStateException(
          String.format(
              "Fragment %d not found in dataset at %s (version=%s). Available fragments: %s",
              fragmentId,
              readOptions.getDatasetUri(),
              readOptions.getVersion(),
              cached.getFragments().keySet()));
    }
    return fragment;
  }

  /** Clears the cache. Primarily for testing. */
  public static void clearCache() {
    CACHE.invalidateAll();
  }

  /** Returns the current cache size. Primarily for testing. */
  public static long cacheSize() {
    return CACHE.size();
  }
}
