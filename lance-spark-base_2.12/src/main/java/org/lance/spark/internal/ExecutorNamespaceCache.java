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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DescribeTableVersionRequest;
import org.lance.namespace.model.DescribeTableVersionResponse;
import org.lance.namespace.model.ListTableVersionsRequest;
import org.lance.namespace.model.ListTableVersionsResponse;
import org.lance.spark.LanceRuntime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Executor-local cache for namespace clients and credential-vending table descriptions.
 *
 * <p>A Spark executor opens one Lance dataset per fragment. Opening through a namespace calls
 * {@link LanceNamespace#describeTable(DescribeTableRequest)}, so reconstructing the namespace for
 * every fragment turns a table with many fragments into a burst of catalog requests. This cache
 * keeps one namespace client per Spark scan in each executor and coalesces identical table
 * descriptions within that scan while preserving refresh before temporary credentials expire. Scan
 * scoping prevents table locations and credentials from leaking into a later query that happens to
 * reuse the same executor JVM.
 */
public final class ExecutorNamespaceCache {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorNamespaceCache.class);

  static final String EXPIRES_AT_MILLIS = "expires_at_millis";
  static final long STATIC_RESPONSE_TTL_MILLIS = 5 * 60 * 1000L;
  static final long UNKNOWN_CREDENTIAL_TTL_MILLIS = 60 * 1000L;
  static final long MAX_EXPIRY_SAFETY_WINDOW_MILLIS = 60 * 1000L;
  static final long MIN_EXPIRY_SAFETY_WINDOW_MILLIS = 1000L;
  static final long MAX_DESCRIBED_TABLES_PER_NAMESPACE = 1000L;
  static final long MAX_CACHED_SCANS = 1000L;
  static final long CACHE_IDLE_EXPIRY_MILLIS = 60 * 60 * 1000L;

  private static final Cache<NamespaceKey, CachedNamespace> NAMESPACES =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_CACHED_SCANS)
          .expireAfterAccess(CACHE_IDLE_EXPIRY_MILLIS, TimeUnit.MILLISECONDS)
          .removalListener(
              (RemovalNotification<NamespaceKey, CachedNamespace> notification) -> {
                CachedNamespace namespace = notification.getValue();
                if (namespace != null) {
                  namespace.evict();
                }
              })
          .build();

  private ExecutorNamespaceCache() {}

  /** Acquires a shared, credential-aware namespace client for one scan in this executor JVM. */
  public static Lease acquire(
      String namespaceImpl, Map<String, String> namespaceProperties, String scanId) {
    NamespaceKey key = new NamespaceKey(namespaceImpl, namespaceProperties, scanId);
    while (true) {
      CachedNamespace cached;
      try {
        cached =
            NAMESPACES.get(
                key,
                () ->
                    new CachedNamespace(
                        LanceRuntime.getOrCreateNamespace(namespaceImpl, key.properties),
                        key.scanId));
      } catch (ExecutionException e) {
        throw propagate(e.getCause(), "Failed to initialize executor namespace");
      } catch (UncheckedExecutionException e) {
        throw propagate(e.getCause(), "Failed to initialize executor namespace");
      } catch (ExecutionError e) {
        throw propagate(e.getCause(), "Failed to initialize executor namespace");
      }
      Lease lease = cached.acquire();
      if (lease != null) {
        return lease;
      }
      NAMESPACES.asMap().remove(key, cached);
    }
  }

  /** Clears cached namespaces. Primarily for tests. */
  static void clear() {
    NAMESPACES.invalidateAll();
    NAMESPACES.cleanUp();
  }

  private static RuntimeException propagate(Throwable cause, String message) {
    if (cause instanceof RuntimeException) {
      return (RuntimeException) cause;
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    return new RuntimeException(message, cause);
  }

  private static final class NamespaceKey {
    private final String impl;
    private final Map<String, String> properties;
    private final String scanId;

    private NamespaceKey(String impl, Map<String, String> properties, String scanId) {
      this.impl = Objects.requireNonNull(impl, "namespaceImpl");
      this.properties =
          properties == null
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new HashMap<>(properties));
      this.scanId = Objects.requireNonNull(scanId, "scanId");
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof NamespaceKey)) {
        return false;
      }
      NamespaceKey that = (NamespaceKey) other;
      return impl.equals(that.impl)
          && properties.equals(that.properties)
          && scanId.equals(that.scanId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(impl, properties, scanId);
    }
  }

  /** A reference-counted namespace lease held for the lifetime of one fragment scanner. */
  public static final class Lease implements AutoCloseable {
    private CachedNamespace owner;

    private Lease(CachedNamespace owner) {
      this.owner = owner;
    }

    public LanceNamespace namespace() {
      synchronized (this) {
        if (owner == null) {
          throw new IllegalStateException("Namespace lease is already closed");
        }
        return owner.namespace;
      }
    }

    @Override
    public void close() {
      CachedNamespace toRelease;
      synchronized (this) {
        toRelease = owner;
        owner = null;
      }
      if (toRelease != null) {
        toRelease.release();
      }
    }
  }

  private static final class CachedNamespace {
    private final LanceNamespace delegate;
    private final CredentialCachingNamespace namespace;
    private int leases;
    private boolean evicted;
    private boolean closed;

    private CachedNamespace(LanceNamespace delegate, String scanId) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.namespace =
          new CredentialCachingNamespace(delegate, System::currentTimeMillis, scanId);
    }

    private synchronized Lease acquire() {
      if (evicted) {
        return null;
      }
      leases++;
      return new Lease(this);
    }

    private synchronized void release() {
      if (leases <= 0) {
        throw new IllegalStateException("Namespace lease released too many times");
      }
      leases--;
      if (evicted && leases == 0) {
        closeDelegate();
      }
    }

    private synchronized void evict() {
      evicted = true;
      if (leases == 0) {
        closeDelegate();
      }
    }

    private void closeDelegate() {
      if (closed) {
        return;
      }
      closed = true;
      if (delegate instanceof AutoCloseable) {
        try {
          ((AutoCloseable) delegate).close();
        } catch (Exception e) {
          // Cache eviction is best effort and must not fail an unrelated Spark task.
          LOG.warn("Failed to close an evicted executor namespace", e);
        }
      }
    }
  }

  /**
   * Read-only namespace wrapper used by executor-side Dataset opens.
   *
   * <p>Lance 9.0 executor reads use {@code namespaceId}, {@code describeTable}, and, for managed
   * versioning, {@code listTableVersions} and {@code describeTableVersion}. Other namespace
   * operations intentionally retain {@link LanceNamespace}'s unsupported default implementations;
   * worker-side reads must not expose catalog mutation APIs through this cache.
   */
  static final class CredentialCachingNamespace implements LanceNamespace {
    private final LanceNamespace delegate;
    private final LongSupplier clock;
    private final String namespaceId;
    private final Cache<DescribeTableRequest, FutureTask<CachedDescription>> descriptionCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_DESCRIBED_TABLES_PER_NAMESPACE)
            .expireAfterAccess(CACHE_IDLE_EXPIRY_MILLIS, TimeUnit.MILLISECONDS)
            .build();
    private final ConcurrentMap<DescribeTableRequest, FutureTask<CachedDescription>> descriptions =
        descriptionCache.asMap();

    CredentialCachingNamespace(LanceNamespace delegate, LongSupplier clock, String scanId) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.clock = Objects.requireNonNull(clock, "clock");
      // Lance uses namespaceId as part of its object-store/provider cache key. Keep the ID stable
      // within one Spark scan, but isolate scans so a later scan cannot reuse a provider whose
      // namespace lease belongs to an earlier scan and may be closed on cache eviction.
      this.namespaceId =
          delegate.namespaceId()
              + ",sparkScan["
              + Objects.requireNonNull(scanId, "scanId")
              + "]";
    }

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
      throw new UnsupportedOperationException("Cached namespace is already initialized");
    }

    @Override
    public String namespaceId() {
      return namespaceId;
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      while (true) {
        FutureTask<CachedDescription> candidate =
            new FutureTask<>(
                () -> {
                  long now = clock.getAsLong();
                  DescribeTableResponse response = delegate.describeTable(request);
                  return new CachedDescription(response, refreshAtMillis(response, now));
                });
        FutureTask<CachedDescription> task = descriptions.putIfAbsent(request, candidate);
        boolean loaded = task == null;
        if (task == null) {
          task = candidate;
          task.run();
        }

        CachedDescription cached;
        try {
          cached = get(task);
        } catch (RuntimeException | Error e) {
          descriptions.remove(request, task);
          throw e;
        }
        if (clock.getAsLong() < cached.refreshAtMillis) {
          return cached.response;
        }
        descriptions.remove(request, task);
        if (loaded) {
          // Return the freshly fetched response even when it is already inside the safety window.
          // The next caller will retry, but this caller must not spin forever if a server keeps
          // vending expired or unusually short-lived credentials.
          return cached.response;
        }
      }
    }

    @Override
    public ListTableVersionsResponse listTableVersions(ListTableVersionsRequest request) {
      return delegate.listTableVersions(request);
    }

    @Override
    public DescribeTableVersionResponse describeTableVersion(DescribeTableVersionRequest request) {
      return delegate.describeTableVersion(request);
    }

    private static CachedDescription get(FutureTask<CachedDescription> task) {
      try {
        return task.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while refreshing namespace credentials", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new RuntimeException("Failed to refresh namespace credentials", cause);
      }
    }

    private static long refreshAtMillis(DescribeTableResponse response, long now) {
      Map<String, String> storageOptions = response.getStorageOptions();
      if (storageOptions == null || storageOptions.isEmpty()) {
        return saturatedAdd(now, STATIC_RESPONSE_TTL_MILLIS);
      }

      String rawExpiry = storageOptions.get(EXPIRES_AT_MILLIS);
      if (rawExpiry == null) {
        // The namespace spec requires temporary credentials to include expires_at_millis. Keep a
        // short fallback TTL for older or non-conforming servers instead of caching indefinitely.
        return saturatedAdd(now, UNKNOWN_CREDENTIAL_TTL_MILLIS);
      }

      try {
        long expiresAt = Long.parseLong(rawExpiry);
        long remaining = expiresAt - now;
        if (remaining <= 0) {
          return now;
        }
        long safetyWindow =
            Math.min(
                MAX_EXPIRY_SAFETY_WINDOW_MILLIS,
                Math.max(MIN_EXPIRY_SAFETY_WINDOW_MILLIS, remaining / 10));
        return Math.max(now, expiresAt - safetyWindow);
      } catch (NumberFormatException ignored) {
        return saturatedAdd(now, UNKNOWN_CREDENTIAL_TTL_MILLIS);
      }
    }

    private static long saturatedAdd(long value, long increment) {
      if (value > Long.MAX_VALUE - increment) {
        return Long.MAX_VALUE;
      }
      return value + increment;
    }
  }

  private static final class CachedDescription {
    private final DescribeTableResponse response;
    private final long refreshAtMillis;

    private CachedDescription(DescribeTableResponse response, long refreshAtMillis) {
      this.response = response;
      this.refreshAtMillis = refreshAtMillis;
    }
  }
}
