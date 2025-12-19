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
 *     .driverAllocatorSize(1024L * 1024 * 1024)
 *     .executorAllocatorSize(512L * 1024 * 1024)
 *     .storageOptions(options)
 *     .build();
 * }</pre>
 */
public class LanceSparkCatalogConfig {

  /** Catalog config key for driver allocator size. */
  public static final String DRIVER_ALLOCATOR_SIZE_KEY = "driver_allocator_size";

  /** Catalog config key for executor allocator size. */
  public static final String EXECUTOR_ALLOCATOR_SIZE_KEY = "executor_allocator_size";

  private final Long driverAllocatorSize;
  private final Long executorAllocatorSize;
  private final Map<String, String> storageOptions;

  private LanceSparkCatalogConfig(Builder builder) {
    this.driverAllocatorSize = builder.driverAllocatorSize;
    this.executorAllocatorSize = builder.executorAllocatorSize;
    this.storageOptions = Collections.unmodifiableMap(new HashMap<>(builder.storageOptions));
  }

  /** Creates a new builder for LanceSparkCatalogConfig. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a config from a map of options.
   *
   * @param options the options map
   * @return a new LanceSparkCatalogConfig
   */
  public static LanceSparkCatalogConfig from(Map<String, String> options) {
    return builder().storageOptions(options).build();
  }

  // ========== Getters ==========

  /**
   * Returns the driver allocator size in bytes.
   *
   * @return the driver allocator size, or null to use global allocator
   */
  public Long getDriverAllocatorSize() {
    return driverAllocatorSize;
  }

  /**
   * Returns the executor allocator size in bytes.
   *
   * @return the executor allocator size, or null to use global allocator
   */
  public Long getExecutorAllocatorSize() {
    return executorAllocatorSize;
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
    return Objects.equals(driverAllocatorSize, that.driverAllocatorSize)
        && Objects.equals(executorAllocatorSize, that.executorAllocatorSize)
        && Objects.equals(storageOptions, that.storageOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(driverAllocatorSize, executorAllocatorSize, storageOptions);
  }

  /** Builder for creating LanceSparkCatalogConfig instances. */
  public static class Builder {
    private Long driverAllocatorSize;
    private Long executorAllocatorSize;
    private Map<String, String> storageOptions = new HashMap<>();

    private Builder() {}

    /**
     * Sets the driver allocator size in bytes.
     *
     * @param driverAllocatorSize the size in bytes, or null to use global allocator
     * @return this builder
     */
    public Builder driverAllocatorSize(Long driverAllocatorSize) {
      this.driverAllocatorSize = driverAllocatorSize;
      return this;
    }

    /**
     * Sets the executor allocator size in bytes.
     *
     * @param executorAllocatorSize the size in bytes, or null to use global allocator
     * @return this builder
     */
    public Builder executorAllocatorSize(Long executorAllocatorSize) {
      this.executorAllocatorSize = executorAllocatorSize;
      return this;
    }

    /**
     * Sets the storage options for cloud storage access.
     *
     * @param storageOptions the storage options map
     * @return this builder
     */
    public Builder storageOptions(Map<String, String> storageOptions) {
      this.storageOptions = new HashMap<>(storageOptions);
      // Parse allocator sizes from options if present
      if (storageOptions.containsKey(DRIVER_ALLOCATOR_SIZE_KEY)) {
        this.driverAllocatorSize = Long.parseLong(storageOptions.get(DRIVER_ALLOCATOR_SIZE_KEY));
      }
      if (storageOptions.containsKey(EXECUTOR_ALLOCATOR_SIZE_KEY)) {
        this.executorAllocatorSize =
            Long.parseLong(storageOptions.get(EXECUTOR_ALLOCATOR_SIZE_KEY));
      }
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
