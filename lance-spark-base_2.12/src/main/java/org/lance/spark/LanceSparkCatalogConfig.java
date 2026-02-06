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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Catalog-level configuration for Lance Spark connector.
 *
 * <p>This class contains only catalog-level settings that apply to all operations. For
 * operation-specific settings, use {@link LanceSparkReadOptions} or {@link LanceSparkWriteOptions}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * LanceSparkCatalogConfig config = LanceSparkCatalogConfig.builder()
 *     .storageOptions(options)
 *     .build();
 * }</pre>
 */
public class LanceSparkCatalogConfig {

  private static final String STORAGE_PREFIX = "storage.";

  private final Map<String, String> storageOptions;

  private LanceSparkCatalogConfig(Builder builder) {
    this.storageOptions = Collections.unmodifiableMap(new HashMap<>(builder.storageOptions));
  }

  /** Creates a new builder for LanceSparkCatalogConfig. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a config from a map of options.
   *
   * @param catalogOptions the options map
   * @return a new LanceSparkCatalogConfig
   */
  public static LanceSparkCatalogConfig from(Map<String, String> catalogOptions) {
    Map<String, String> nativeOptions = new HashMap<>();

    for (Map.Entry<String, String> entry : catalogOptions.entrySet()) {
      String fullKey = entry.getKey();
      String value = entry.getValue();
      if (fullKey == null || value == null) {
        continue;
      }
      if (!fullKey.startsWith(STORAGE_PREFIX)) {
        nativeOptions.put(fullKey, value);
        continue;
      }

      String nativeKey = fullKey.substring(STORAGE_PREFIX.length());
      nativeOptions.put(nativeKey, value);
    }

    return builder().storageOptions(nativeOptions).build();
  }

  /**
   * Returns the storage options for cloud storage access.
   *
   * @return unmodifiable map of storage options
   */
  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    LanceSparkCatalogConfig that = (LanceSparkCatalogConfig) o;
    return Objects.equals(storageOptions, that.storageOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageOptions);
  }

  /** Builder for creating LanceSparkCatalogConfig instances. */
  public static class Builder {
    private Map<String, String> storageOptions = new HashMap<>();

    private Builder() {}

    /**
     * Sets the storage options for cloud storage access.
     *
     * @param storageOptions the storage options map
     * @return this builder
     */
    public Builder storageOptions(Map<String, String> storageOptions) {
      this.storageOptions = new HashMap<>(storageOptions);
      return this;
    }

    /**
     * Builds the LanceSparkCatalogConfig.
     *
     * @return the built LanceSparkCatalogConfig
     */
    public LanceSparkCatalogConfig build() {
      return new LanceSparkCatalogConfig(this);
    }
  }
}
