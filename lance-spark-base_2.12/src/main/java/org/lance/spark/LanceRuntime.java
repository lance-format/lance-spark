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
import org.lance.namespace.model.QueryTableRequest;
import org.lance.otel.LanceMetrics;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.MeterProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 *   <li>{@code LANCE_INDEX_CACHE_SIZE} - Index cache size in bytes
 *   <li>{@code LANCE_METADATA_CACHE_SIZE} - Metadata cache size in bytes
 *   <li>{@code LANCE_INDEX_CACHE_BACKEND} - Registered index cache backend URI
 *   <li>{@code LANCE_METADATA_CACHE_BACKEND} - Registered metadata cache backend URI
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
  private static final Logger LOG = LoggerFactory.getLogger(LanceRuntime.class);

  /** Environment variable for allocator size. */
  public static final String ENV_ALLOCATOR_SIZE = "LANCE_ALLOCATOR_SIZE";

  /** Environment variable for index cache size in bytes. */
  public static final String ENV_INDEX_CACHE_SIZE = "LANCE_INDEX_CACHE_SIZE";

  /** Environment variable for metadata cache size in bytes. */
  public static final String ENV_METADATA_CACHE_SIZE = "LANCE_METADATA_CACHE_SIZE";

  /** Environment variable for the registered index cache backend URI. */
  public static final String ENV_INDEX_CACHE_BACKEND = "LANCE_INDEX_CACHE_BACKEND";

  /** Environment variable for the registered metadata cache backend URI. */
  public static final String ENV_METADATA_CACHE_BACKEND = "LANCE_METADATA_CACHE_BACKEND";

  /** Spark configuration key that enables Lance OpenTelemetry metrics in each JVM. */
  public static final String SPARK_CONF_OPEN_TELEMETRY_ENABLED = "spark.lance.otel.enabled";

  /** Environment variable fallback for enabling Lance OpenTelemetry metrics in each JVM. */
  public static final String ENV_OPEN_TELEMETRY_ENABLED = "LANCE_SPARK_OTEL_ENABLED";

  /** Default allocator size (unlimited). */
  public static final long DEFAULT_ALLOCATOR_SIZE = Long.MAX_VALUE;

  /** Default catalog name used when no catalog is specified. */
  public static final String DEFAULT_CATALOG = "default";

  /** Global allocator (lazy initialized based on env var). */
  private static volatile BufferAllocator GLOBAL_ALLOCATOR;

  /** Per-catalog sessions for cache isolation (lazy initialized). */
  private static final Map<String, CatalogSession> CATALOG_SESSIONS = new ConcurrentHashMap<>();

  /** External namespace implementation aliases used by Spark catalog configuration. */
  private static final Map<String, String> EXTERNAL_NAMESPACE_IMPLS =
      createExternalNamespaceImpls();

  /** Namespace implementations that can use driver-resolved URI and storage options on tasks. */
  private static final Map<String, Boolean> USE_NAMESPACE_ON_WORKERS =
      createUseNamespaceOnWorkers();

  /** Cached {@code queryTable} support per namespace impl alias or class name. */
  private static final Map<String, Boolean> QUERY_TABLE_SUPPORT = new ConcurrentHashMap<>();

  /** Invalid OpenTelemetry values already reported in this JVM. */
  private static final Set<String> REPORTED_INVALID_OPEN_TELEMETRY_VALUES =
      ConcurrentHashMap.newKeySet();

  /** Process-local OpenTelemetry bridge initialization state. */
  private static volatile OpenTelemetryInitialization OPEN_TELEMETRY_INITIALIZATION =
      OpenTelemetryInitialization.NOT_ATTEMPTED;

  /** OpenTelemetry provider used by the current bridge instruments. */
  private static volatile MeterProvider OPEN_TELEMETRY_PROVIDER;

  private LanceRuntime() {}

  private static Map<String, String> createExternalNamespaceImpls() {
    Map<String, String> impls = new HashMap<>();
    impls.put("glue", "org.lance.namespace.glue.GlueNamespace");
    impls.put("hive2", "org.lance.namespace.hive2.Hive2Namespace");
    impls.put("hive3", "org.lance.namespace.hive3.Hive3Namespace");
    impls.put("iceberg", "org.lance.namespace.iceberg.IcebergNamespace");
    impls.put("unity", "org.lance.namespace.unity.UnityNamespace");
    impls.put("polaris", "org.lance.namespace.polaris.PolarisNamespace");
    return Collections.unmodifiableMap(impls);
  }

  private static Map<String, Boolean> createUseNamespaceOnWorkers() {
    Map<String, Boolean> impls = new HashMap<>();
    impls.put("glue", false);
    return Collections.unmodifiableMap(impls);
  }

  static void registerKnownNamespaceImpl(String namespaceImpl) {
    String className = EXTERNAL_NAMESPACE_IMPLS.get(namespaceImpl);
    if (className != null && !LanceNamespace.isRegistered(namespaceImpl)) {
      LanceNamespace.registerNamespaceImpl(namespaceImpl, className);
    }
  }

  public static boolean useNamespaceOnWorkers(String namespaceImpl) {
    return USE_NAMESPACE_ON_WORKERS.getOrDefault(namespaceImpl, true);
  }

  /**
   * Returns whether the namespace implementation executes queries server-side through {@code
   * queryTable}.
   *
   * <p>{@link LanceNamespace#queryTable} is a default interface method that throws {@link
   * org.lance.namespace.errors.UnsupportedOperationException}, and catalog-only implementations
   * such as Glue, Hive, and Iceberg never override it. Callers use this to decide whether a query
   * can be pushed to the namespace or has to be executed against the dataset directly.
   *
   * @param namespaceImpl the namespace implementation alias or class name
   * @return true if the implementation overrides {@code queryTable}
   */
  public static boolean supportsQueryTable(String namespaceImpl) {
    if (namespaceImpl == null) {
      return false;
    }
    return QUERY_TABLE_SUPPORT.computeIfAbsent(namespaceImpl, LanceRuntime::probeQueryTable);
  }

  private static boolean probeQueryTable(String namespaceImpl) {
    registerKnownNamespaceImpl(namespaceImpl);
    String className = LanceNamespace.NATIVE_IMPLS.get(namespaceImpl);
    if (className == null) {
      className = LanceNamespace.REGISTERED_IMPLS.get(namespaceImpl);
    }
    if (className == null) {
      className = namespaceImpl;
    }
    try {
      Method queryTable = Class.forName(className).getMethod("queryTable", QueryTableRequest.class);
      return !queryTable.isDefault();
    } catch (ClassNotFoundException | NoSuchMethodException | LinkageError e) {
      // Treat an unresolvable implementation as unsupported; connect() reports the real error.
      return false;
    }
  }

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
    return CATALOG_SESSIONS.computeIfAbsent(
            key, k -> CatalogSession.managed(createSessionConfiguration(null, null)))
        .session;
  }

  /**
   * Returns the session for a catalog with registered cache backends selected by URI.
   *
   * <p>The first call for a catalog creates its process-local session. Later calls must request the
   * same backend configuration. Cache backends cannot be changed after the session has been used;
   * use a different catalog or restart the JVM to switch an existing catalog.
   *
   * <p>Catalog backend URIs take precedence over the corresponding environment backend and cache
   * size settings. A {@code null} URI falls back to {@link #ENV_INDEX_CACHE_BACKEND} or {@link
   * #ENV_METADATA_CACHE_BACKEND}, then to the legacy cache size environment variable.
   *
   * @param catalogName the catalog name for cache isolation
   * @param indexCacheBackendUri index cache backend URI, or null for the environment/default
   * @param metadataCacheBackendUri metadata cache backend URI, or null for the environment/default
   * @return the session for the specified catalog
   */
  public static Session session(
      String catalogName, String indexCacheBackendUri, String metadataCacheBackendUri) {
    validateOptionalBackendUri(indexCacheBackendUri, "indexCacheBackendUri");
    validateOptionalBackendUri(metadataCacheBackendUri, "metadataCacheBackendUri");
    if (!hasText(indexCacheBackendUri) && !hasText(metadataCacheBackendUri)) {
      return session(catalogName);
    }

    String key = catalogName != null ? catalogName : DEFAULT_CATALOG;
    SessionConfiguration requested =
        createSessionConfiguration(indexCacheBackendUri, metadataCacheBackendUri);
    return CATALOG_SESSIONS.compute(
            key,
            (ignored, current) -> {
              if (current == null) {
                return CatalogSession.managed(requested);
              }
              if (!requested.equals(current.configuration)) {
                throw new IllegalStateException(
                    "Lance session for catalog '"
                        + key
                        + "' is already initialized with a different cache configuration");
              }
              return current;
            })
        .session;
  }

  /**
   * Registers a Java SDK session for a Spark catalog.
   *
   * <p>Use this when the session was built programmatically, for example with structured {@code
   * CacheBackendConfig} instances. Registration must happen before the catalog is first used in
   * each driver or executor JVM. Ownership transfers to {@code LanceRuntime}; callers must not
   * close the session after registration.
   *
   * @param catalogName the catalog that should use the session
   * @param session the open Java SDK session to register
   */
  public static void registerSession(String catalogName, Session session) {
    Objects.requireNonNull(session, "session must not be null");
    if (session.isClosed()) {
      throw new IllegalArgumentException("session must be open");
    }

    String key = catalogName != null ? catalogName : DEFAULT_CATALOG;
    CATALOG_SESSIONS.compute(
        key,
        (ignored, current) -> {
          if (current == null) {
            return CatalogSession.registered(session);
          }
          if (current.session.isSameAs(session)) {
            return current;
          }
          throw new IllegalStateException(
              "Lance session for catalog '" + key + "' is already initialized");
        });
  }

  private static void validateOptionalBackendUri(String backendUri, String name) {
    if (backendUri != null && !hasText(backendUri)) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }

  /**
   * Installs Lance's JNI-backed OpenTelemetry metrics bridge when enabled.
   *
   * <p>The bridge is process-global and {@link LanceMetrics#instrument()} is idempotent, so Spark
   * driver and executor code paths can call this before Lance work starts in each JVM.
   *
   * @return true if OpenTelemetry was enabled and the bridge was installed, false otherwise
   */
  public static boolean enableOpenTelemetry() {
    if (!isOpenTelemetryEnabled()) {
      return false;
    }

    MeterProvider meterProvider = GlobalOpenTelemetry.get().getMeterProvider();
    OpenTelemetryInitialization initialization = OPEN_TELEMETRY_INITIALIZATION;
    if (initialization == OpenTelemetryInitialization.FAILED) {
      return false;
    }
    if (initialization == OpenTelemetryInitialization.INSTALLED
        && OPEN_TELEMETRY_PROVIDER == meterProvider) {
      return true;
    }

    synchronized (LanceRuntime.class) {
      initialization = OPEN_TELEMETRY_INITIALIZATION;
      if (initialization == OpenTelemetryInitialization.FAILED) {
        return false;
      }
      if (initialization == OpenTelemetryInitialization.INSTALLED
          && OPEN_TELEMETRY_PROVIDER == meterProvider) {
        return true;
      }

      boolean instrumented = LanceMetrics.instrument(meterProvider);
      if (instrumented) {
        OPEN_TELEMETRY_PROVIDER = meterProvider;
        OPEN_TELEMETRY_INITIALIZATION = OpenTelemetryInitialization.INSTALLED;
      } else {
        OPEN_TELEMETRY_INITIALIZATION = OpenTelemetryInitialization.FAILED;
        LOG.warn(
            "Lance OpenTelemetry metrics were enabled, but the native metrics recorder could not"
                + " be installed. Another Rust metrics recorder may already be installed in this"
                + " JVM.");
      }
      return instrumented;
    }
  }

  static boolean isOpenTelemetryEnabled() {
    SparkEnv sparkEnv = SparkEnv.get();
    String sparkConfigured =
        sparkEnv == null ? null : sparkEnv.conf().get(SPARK_CONF_OPEN_TELEMETRY_ENABLED, null);
    return resolveOpenTelemetryEnabled(
        sparkConfigured,
        System.getProperty(SPARK_CONF_OPEN_TELEMETRY_ENABLED),
        System.getenv(ENV_OPEN_TELEMETRY_ENABLED));
  }

  static boolean resolveOpenTelemetryEnabled(
      String sparkConfigured, String systemConfigured, String environmentConfigured) {
    if (hasText(sparkConfigured)) {
      return parseOpenTelemetryEnabled(sparkConfigured, "Spark configuration");
    }
    if (hasText(systemConfigured)) {
      return parseOpenTelemetryEnabled(systemConfigured, "JVM system property");
    }
    if (hasText(environmentConfigured)) {
      return parseOpenTelemetryEnabled(environmentConfigured, "environment variable");
    }
    return false;
  }

  private static boolean parseOpenTelemetryEnabled(String configured, String source) {
    String normalized = configured.trim();
    if ("true".equalsIgnoreCase(normalized)) {
      return true;
    }
    if ("false".equalsIgnoreCase(normalized)) {
      return false;
    }

    String reportedValue = source + '\0' + configured;
    if (REPORTED_INVALID_OPEN_TELEMETRY_VALUES.add(reportedValue)) {
      LOG.warn(
          "Invalid boolean value '{}' for {} from {}; expected 'true' or 'false'. OpenTelemetry"
              + " metrics will remain disabled.",
          configured,
          SPARK_CONF_OPEN_TELEMETRY_ENABLED,
          source);
    }
    return false;
  }

  private static boolean hasText(String value) {
    return value != null && !value.trim().isEmpty();
  }

  private static SessionConfiguration createSessionConfiguration(
      String indexCacheBackendUri, String metadataCacheBackendUri) {
    String indexBackend =
        firstNonBlank(indexCacheBackendUri, System.getenv(ENV_INDEX_CACHE_BACKEND));
    String metadataBackend =
        firstNonBlank(metadataCacheBackendUri, System.getenv(ENV_METADATA_CACHE_BACKEND));
    Long indexCacheSize = indexBackend == null ? getEnvLong(ENV_INDEX_CACHE_SIZE) : null;
    Long metadataCacheSize = metadataBackend == null ? getEnvLong(ENV_METADATA_CACHE_SIZE) : null;
    return new SessionConfiguration(
        indexBackend, metadataBackend, indexCacheSize, metadataCacheSize);
  }

  private static Session createSession(SessionConfiguration configuration) {
    Session.Builder builder = Session.builder();

    if (configuration.indexCacheBackendUri != null) {
      builder.indexCacheBackend(configuration.indexCacheBackendUri);
    } else if (configuration.indexCacheSize != null) {
      builder.indexCacheSizeBytes(configuration.indexCacheSize);
    }

    if (configuration.metadataCacheBackendUri != null) {
      builder.metadataCacheBackend(configuration.metadataCacheBackendUri);
    } else if (configuration.metadataCacheSize != null) {
      builder.metadataCacheSizeBytes(configuration.metadataCacheSize);
    }

    return builder.build();
  }

  private static String firstNonBlank(String primary, String fallback) {
    if (hasText(primary)) {
      return primary.trim();
    }
    if (hasText(fallback)) {
      return fallback.trim();
    }
    return null;
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
      for (CatalogSession catalogSession : CATALOG_SESSIONS.values()) {
        catalogSession.session.close();
      }
      CATALOG_SESSIONS.clear();
    }
  }

  static void clearOpenTelemetry() {
    synchronized (LanceRuntime.class) {
      LanceMetrics.close();
      OPEN_TELEMETRY_PROVIDER = null;
      OPEN_TELEMETRY_INITIALIZATION = OpenTelemetryInitialization.NOT_ATTEMPTED;
      REPORTED_INVALID_OPEN_TELEMETRY_VALUES.clear();
    }
  }

  private enum OpenTelemetryInitialization {
    NOT_ATTEMPTED,
    INSTALLED,
    FAILED
  }

  private static final class CatalogSession {
    private final Session session;
    private final SessionConfiguration configuration;

    private CatalogSession(Session session, SessionConfiguration configuration) {
      this.session = session;
      this.configuration = configuration;
    }

    private static CatalogSession managed(SessionConfiguration configuration) {
      return new CatalogSession(createSession(configuration), configuration);
    }

    private static CatalogSession registered(Session session) {
      return new CatalogSession(session, null);
    }
  }

  private static final class SessionConfiguration {
    private final String indexCacheBackendUri;
    private final String metadataCacheBackendUri;
    private final Long indexCacheSize;
    private final Long metadataCacheSize;

    private SessionConfiguration(
        String indexCacheBackendUri,
        String metadataCacheBackendUri,
        Long indexCacheSize,
        Long metadataCacheSize) {
      this.indexCacheBackendUri = indexCacheBackendUri;
      this.metadataCacheBackendUri = metadataCacheBackendUri;
      this.indexCacheSize = indexCacheSize;
      this.metadataCacheSize = metadataCacheSize;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof SessionConfiguration)) {
        return false;
      }
      SessionConfiguration that = (SessionConfiguration) other;
      return Objects.equals(indexCacheBackendUri, that.indexCacheBackendUri)
          && Objects.equals(metadataCacheBackendUri, that.metadataCacheBackendUri)
          && Objects.equals(indexCacheSize, that.indexCacheSize)
          && Objects.equals(metadataCacheSize, that.metadataCacheSize);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          indexCacheBackendUri, metadataCacheBackendUri, indexCacheSize, metadataCacheSize);
    }
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
    registerKnownNamespaceImpl(namespaceImpl);
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
