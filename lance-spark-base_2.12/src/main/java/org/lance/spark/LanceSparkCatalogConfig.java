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
 * <p>Configuration is split into two maps:
 *
 * <ul>
 *   <li>{@code storageOptions} - Cloud storage credentials and connection settings
 *   <li>{@code tableOptions} - Default options applied to new table creation
 * </ul>
 *
 * <p>Known table option keys:
 *
 * <ul>
 *   <li>{@code enable_stable_row_ids} - Enable stable row IDs and CDF support (default: false)
 * </ul>
 */
public class LanceSparkCatalogConfig {

  private static final String STORAGE_PREFIX = "storage.";

  /** Known table option keys. */
  public static final String TABLE_OPT_ENABLE_STABLE_ROW_IDS = "enable_stable_row_ids";

  private final Map<String, String> storageOptions;
  private final Map<String, String> tableOptions;

  private LanceSparkCatalogConfig(Builder builder) {
    this.storageOptions = Collections.unmodifiableMap(new HashMap<>(builder.storageOptions));
    this.tableOptions = Collections.unmodifiableMap(new HashMap<>(builder.tableOptions));
  }

  /** Creates a new builder for LanceSparkCatalogConfig. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a config from a map of catalog options.
   *
   * <p>Keys prefixed with {@code storage.} are stripped and placed in {@code storageOptions}. Known
   * table-level keys are placed in {@code tableOptions}.
   *
   * @param catalogOptions the options map
   * @return a new LanceSparkCatalogConfig
   */
  public static LanceSparkCatalogConfig from(Map<String, String> catalogOptions) {
    Map<String, String> nativeOptions = new HashMap<>();
    Map<String, String> tableOpts = new HashMap<>();

    for (Map.Entry<String, String> entry : catalogOptions.entrySet()) {
      String fullKey = entry.getKey();
      String value = entry.getValue();
      if (fullKey == null || value == null) {
        continue;
      }
      if (fullKey.startsWith(STORAGE_PREFIX)) {
        String nativeKey = fullKey.substring(STORAGE_PREFIX.length());
        nativeOptions.put(nativeKey, value);
      } else {
        nativeOptions.put(fullKey, value);
      }
    }

    // Extract known table options
    String stableRowIds = catalogOptions.get(TABLE_OPT_ENABLE_STABLE_ROW_IDS);
    if (stableRowIds != null) {
      tableOpts.put(TABLE_OPT_ENABLE_STABLE_ROW_IDS, stableRowIds);
    }

    return builder().storageOptions(nativeOptions).tableOptions(tableOpts).build();
  }

  /**
   * Returns the storage options for cloud storage access.
   *
   * @return unmodifiable map of storage options
   */
  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  /**
   * Returns the table options that apply to new table creation.
   *
   * @return unmodifiable map of table options
   */
  public Map<String, String> getTableOptions() {
    return tableOptions;
  }

  /**
   * Returns whether stable row IDs should be enabled, based on catalog-level table options.
   *
   * @return true if stable row IDs are enabled by default
   */
  public boolean isEnableStableRowIds() {
    return "true"
        .equalsIgnoreCase(tableOptions.getOrDefault(TABLE_OPT_ENABLE_STABLE_ROW_IDS, "false"));
  }

  /**
   * Returns whether stable row IDs should be enabled, allowing per-table TBLPROPERTIES to override
   * the catalog-level default.
   *
   * @param tableProperties the TBLPROPERTIES from CREATE TABLE
   * @return true if stable row IDs are enabled
   */
  public boolean isEnableStableRowIds(Map<String, String> tableProperties) {
    String override = tableProperties.get(TABLE_OPT_ENABLE_STABLE_ROW_IDS);
    if (override != null) {
      return "true".equalsIgnoreCase(override);
    }
    return isEnableStableRowIds();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    LanceSparkCatalogConfig that = (LanceSparkCatalogConfig) o;
    return Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(tableOptions, that.tableOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageOptions, tableOptions);
  }

  /** Builder for creating LanceSparkCatalogConfig instances. */
  public static class Builder {
    private Map<String, String> storageOptions = new HashMap<>();
    private Map<String, String> tableOptions = new HashMap<>();

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
     * Sets the table options for new table creation.
     *
     * @param tableOptions the table options map
     * @return this builder
     */
    public Builder tableOptions(Map<String, String> tableOptions) {
      this.tableOptions = new HashMap<>(tableOptions);
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
