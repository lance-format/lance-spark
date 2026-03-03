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

import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.LanceNamespaceStorageOptionsProvider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime utilities for Lance Spark connector.
 *
 * <p>This class manages a global Arrow buffer allocator, a shared Session for cache efficiency, and
 * provides helper methods for namespace operations.
 *
 * <p>Session cache sizes can be configured via environment variables:
 *
 * <ul>
 *   <li>{@code LANCE_INDEX_CACHE_SIZE} - Index cache size in bytes (default: 256MB)
 *   <li>{@code LANCE_METADATA_CACHE_SIZE} - Metadata cache size in bytes (default: 256MB)
 * </ul>
 *
 * <p>Usage:
 *
 * <pre>{@code
 * BufferAllocator allocator = LanceRuntime.allocator();
 * Session session = LanceRuntime.session();
 * LanceNamespace ns = LanceRuntime.createNamespace(impl, properties);
 * }</pre>
 */
public final class LanceRuntime {

  /** Environment variable for allocator size. */
  public static final String ENV_ALLOCATOR_SIZE = "LANCE_ALLOCATOR_SIZE";

  /** Environment variable for index cache size in bytes. */
  public static final String ENV_INDEX_CACHE_SIZE = "LANCE_INDEX_CACHE_SIZE";

  /** Environment variable for metadata cache size in bytes. */
  public static final String ENV_METADATA_CACHE_SIZE = "LANCE_METADATA_CACHE_SIZE";

  /** Default allocator size (unlimited). */
  public static final long DEFAULT_ALLOCATOR_SIZE = Long.MAX_VALUE;

  /** Default catalog name used when no catalog is specified. */
  public static final String DEFAULT_CATALOG = "default";

  /** Global allocator (lazy initialized based on env var). */
  private static volatile BufferAllocator GLOBAL_ALLOCATOR;

  /** Per-catalog sessions for cache isolation (lazy initialized). */
  private static final Map<String, Session> CATALOG_SESSIONS = new ConcurrentHashMap<>();

  private LanceRuntime() {}

  /**
   * Returns the global shared Arrow buffer allocator.
   *
   * <p>The allocator size is determined by the {@link #ENV_ALLOCATOR_SIZE} environment variable. If
   * not set, defaults to {@link #DEFAULT_ALLOCATOR_SIZE}.
   *
   * @return the global buffer allocator
   */
  public static BufferAllocator allocator() {
    if (GLOBAL_ALLOCATOR == null) {
      synchronized (LanceRuntime.class) {
        if (GLOBAL_ALLOCATOR == null) {
          long size = getAllocatorSize();
          GLOBAL_ALLOCATOR = new RootAllocator(size);
        }
      }
    }
    return GLOBAL_ALLOCATOR;
  }

  /**
   * Returns the session for the default catalog.
   *
   * <p>This is equivalent to calling {@link #session(String)} with {@link #DEFAULT_CATALOG}.
   *
   * @return the session for the default catalog
   */
  public static Session session() {
    return session(DEFAULT_CATALOG);
  }

  /**
   * Returns the session for a specific catalog.
   *
   * <p>Each catalog has its own session with isolated index and metadata caches. This allows
   * multiple Lance catalogs in the same Spark application to have separate caches.
   *
   * <p>Cache sizes can be configured via environment variables:
   *
   * <ul>
   *   <li>{@link #ENV_INDEX_CACHE_SIZE} - Index cache size in bytes
   *   <li>{@link #ENV_METADATA_CACHE_SIZE} - Metadata cache size in bytes
   * </ul>
   *
   * @param catalogName the catalog name for cache isolation
   * @return the session for the specified catalog
   */
  public static Session session(String catalogName) {
    String key = catalogName != null ? catalogName : DEFAULT_CATALOG;
    return CATALOG_SESSIONS.computeIfAbsent(key, k -> createSession());
  }

  private static Session createSession() {
    Session.Builder builder = Session.builder();

    Long indexCacheSize = getEnvLong(ENV_INDEX_CACHE_SIZE);
    if (indexCacheSize != null) {
      builder.indexCacheSizeBytes(indexCacheSize);
    }

    Long metadataCacheSize = getEnvLong(ENV_METADATA_CACHE_SIZE);
    if (metadataCacheSize != null) {
      builder.metadataCacheSizeBytes(metadataCacheSize);
    }

    return builder.build();
  }

  /**
   * Gets the allocator size from environment variable.
   *
   * @return the allocator size, or DEFAULT_ALLOCATOR_SIZE if not configured
   */
  private static long getAllocatorSize() {
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
   * Gets a long value from environment variable.
   *
   * @param envVar the environment variable name
   * @return the long value, or null if not configured or invalid
   */
  private static Long getEnvLong(String envVar) {
    String envValue = System.getenv(envVar);
    if (envValue != null && !envValue.isEmpty()) {
      try {
        return Long.parseLong(envValue);
      } catch (NumberFormatException e) {
        // Fall through to null
      }
    }
    return null;
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

  /**
   * Clears all catalog sessions. This is primarily for testing purposes.
   *
   * <p>WARNING: This closes all sessions. Do not call while they may be in use.
   */
  static void clearGlobalSession() {
    synchronized (LanceRuntime.class) {
      for (Session session : CATALOG_SESSIONS.values()) {
        session.close();
      }
      CATALOG_SESSIONS.clear();
    }
  }

  /**
   * Checks if namespace configuration is available for credential refresh.
   *
   * @param namespaceImpl the namespace implementation type
   * @param tableId the table identifier
   * @return true if namespace config is available (namespaceImpl and tableId are non-null)
   */
  public static boolean hasNamespaceConfig(String namespaceImpl, List<String> tableId) {
    return namespaceImpl != null && tableId != null;
  }

  /**
   * Creates a storage options provider for credential refresh if namespace config is available.
   *
   * @param namespaceImpl the namespace implementation type
   * @param namespaceProperties the namespace connection properties (can be null)
   * @param tableId the table identifier
   * @return a LanceNamespaceStorageOptionsProvider, or null if namespace config not available
   */
  public static LanceNamespaceStorageOptionsProvider getOrCreateStorageOptionsProvider(
      String namespaceImpl, Map<String, String> namespaceProperties, List<String> tableId) {
    if (!hasNamespaceConfig(namespaceImpl, tableId)) {
      return null;
    }
    LanceNamespace ns = LanceNamespace.connect(namespaceImpl, namespaceProperties, allocator());
    return new LanceNamespaceStorageOptionsProvider(ns, tableId);
  }

  /**
   * Creates a namespace connection.
   *
   * @param namespaceImpl the namespace implementation type
   * @param namespaceProperties the namespace connection properties (can be null)
   * @return a LanceNamespace connection, or null if namespaceImpl is null
   */
  public static LanceNamespace getOrCreateNamespace(
      String namespaceImpl, Map<String, String> namespaceProperties) {
    if (namespaceImpl == null) {
      return null;
    }
    return LanceNamespace.connect(namespaceImpl, namespaceProperties, allocator());
  }

  /**
   * Merges base storage options with initial storage options from namespace.describeTable().
   *
   * @param baseOptions the base storage options
   * @param initialStorageOptions initial options from describeTable (can be null)
   * @return merged storage options map
   */
  public static Map<String, String> mergeStorageOptions(
      Map<String, String> baseOptions, Map<String, String> initialStorageOptions) {
    Map<String, String> merged = new HashMap<>(baseOptions);
    if (initialStorageOptions != null && !initialStorageOptions.isEmpty()) {
      merged.putAll(initialStorageOptions);
    }
    return merged;
  }
}
