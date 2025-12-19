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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.TaskContext;

/**
 * Runtime utilities for Lance Spark connector.
 *
 * <p>This class manages Arrow buffer allocators with support for:
 *
 * <ul>
 *   <li>Global allocator with size configurable via environment variable
 *   <li>Per-catalog allocators with size configurable via Spark catalog config
 *   <li>Separate allocator configurations for driver and executor
 * </ul>
 *
 * <p>Usage:
 *
 * <pre>{@code
 * // Using global allocator (backward compatible)
 * BufferAllocator allocator = LanceRuntime.allocator();
 *
 * // Using config-based allocator (recommended for catalogs)
 * // Each catalog should create and cache one LanceRuntime instance
 * LanceSparkCatalogConfig config = LanceSparkCatalogConfig.from(options);
 * LanceRuntime runtime = new LanceRuntime(config);
 * BufferAllocator allocator = runtime.getBufferAllocator();
 * }</pre>
 */
public final class LanceRuntime {

  /** Environment variable for global allocator size. */
  public static final String ENV_ALLOCATOR_SIZE = "LANCE_ALLOCATOR_SIZE";

  /** Default allocator size (unlimited). */
  public static final long DEFAULT_ALLOCATOR_SIZE = Long.MAX_VALUE;

  /** Global allocator (lazy initialized based on env var). */
  private static volatile BufferAllocator GLOBAL_ALLOCATOR;

  /** The allocator for this runtime instance. */
  private final BufferAllocator allocator;

  /**
   * Creates a LanceRuntime that uses the global allocator.
   *
   * <p>The global allocator size is determined by the {@link #ENV_ALLOCATOR_SIZE} environment
   * variable. If not set, defaults to {@link #DEFAULT_ALLOCATOR_SIZE}.
   */
  public LanceRuntime() {
    this.allocator = globalAllocator();
  }

  /**
   * Creates a LanceRuntime with allocator determined by the catalog config.
   *
   * <p>This constructor automatically detects whether we're running on driver or executor by
   * checking if TaskContext is available.
   *
   * @param config the catalog configuration containing allocator settings
   */
  public LanceRuntime(LanceSparkCatalogConfig config) {
    this(config, TaskContext.get() != null);
  }

  /**
   * Creates a LanceRuntime with allocator determined by the catalog config.
   *
   * @param config the catalog configuration containing allocator settings
   * @param isExecutor true if running on executor, false if on driver
   */
  public LanceRuntime(LanceSparkCatalogConfig config, boolean isExecutor) {
    if (config == null) {
      this.allocator = globalAllocator();
      return;
    }

    Long configuredSize =
        isExecutor ? config.getExecutorAllocatorSize() : config.getDriverAllocatorSize();

    if (configuredSize != null) {
      // Create a new allocator with the configured size
      this.allocator = new RootAllocator(configuredSize);
    } else {
      // Fall back to global allocator
      this.allocator = globalAllocator();
    }
  }

  /**
   * Returns the buffer allocator for this runtime instance.
   *
   * <p>The returned allocator is either:
   *
   * <ul>
   *   <li>A per-catalog allocator if catalog-specific size is configured
   *   <li>The global allocator otherwise
   * </ul>
   *
   * @return the buffer allocator to use
   */
  public BufferAllocator getBufferAllocator() {
    return allocator;
  }

  /**
   * Returns the global shared Arrow buffer allocator.
   *
   * <p>This method is provided for backward compatibility. New code should prefer creating a
   * LanceRuntime instance and using {@link #getBufferAllocator()}.
   *
   * @return the global buffer allocator
   */
  public static BufferAllocator allocator() {
    return globalAllocator();
  }

  /**
   * Returns the global allocator, initializing it if necessary.
   *
   * @return the global buffer allocator
   */
  private static BufferAllocator globalAllocator() {
    if (GLOBAL_ALLOCATOR == null) {
      synchronized (LanceRuntime.class) {
        if (GLOBAL_ALLOCATOR == null) {
          long size = getGlobalAllocatorSize();
          GLOBAL_ALLOCATOR = new RootAllocator(size);
        }
      }
    }
    return GLOBAL_ALLOCATOR;
  }

  /**
   * Gets the global allocator size from environment variable.
   *
   * @return the allocator size, or DEFAULT_ALLOCATOR_SIZE if not configured
   */
  private static long getGlobalAllocatorSize() {
    String envSize = System.getenv(ENV_ALLOCATOR_SIZE);
    if (envSize != null && !envSize.isEmpty()) {
      try {
        return Long.parseLong(envSize);
      } catch (NumberFormatException e) {
        // Fall through to default
      }
    }
    return DEFAULT_ALLOCATOR_SIZE;
  }

  /**
   * Clears the global allocator. This is primarily for testing purposes.
   *
   * <p>WARNING: This closes the global allocator. Do not call while it may be in use.
   */
  static void clearGlobalAllocator() {
    synchronized (LanceRuntime.class) {
      if (GLOBAL_ALLOCATOR != null) {
        GLOBAL_ALLOCATOR.close();
        GLOBAL_ALLOCATOR = null;
      }
    }
  }
}
