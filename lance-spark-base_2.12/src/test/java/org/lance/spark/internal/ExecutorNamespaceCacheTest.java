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

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExecutorNamespaceCacheTest {

  @AfterEach
  public void clearExecutorNamespaceCache() {
    ExecutorNamespaceCache.clear();
  }

  @Test
  public void defersClosingEvictedNamespaceUntilAllFragmentLeasesClose() {
    CloseableNamespace.initializeCalls.set(0);
    CloseableNamespace.closeCalls.set(0);

    ExecutorNamespaceCache.Lease first =
        ExecutorNamespaceCache.acquire(
            CloseableNamespace.class.getName(), Map.of("catalog", "test"), "scan-a");
    ExecutorNamespaceCache.Lease second =
        ExecutorNamespaceCache.acquire(
            CloseableNamespace.class.getName(), Map.of("catalog", "test"), "scan-a");

    assertSame(first.namespace(), second.namespace());
    assertEquals(1, CloseableNamespace.initializeCalls.get());

    ExecutorNamespaceCache.clear();
    assertEquals(0, CloseableNamespace.closeCalls.get());
    first.close();
    assertEquals(0, CloseableNamespace.closeCalls.get());
    second.close();
    assertEquals(1, CloseableNamespace.closeCalls.get());
  }

  @Test
  public void coalescesConcurrentDescribeTableCalls() throws Exception {
    AtomicLong clock = new AtomicLong(100_000L);
    BlockingNamespace delegate = new BlockingNamespace(clock);
    ExecutorNamespaceCache.CredentialCachingNamespace namespace =
        new ExecutorNamespaceCache.CredentialCachingNamespace(delegate, clock::get);
    DescribeTableRequest request = new DescribeTableRequest().addIdItem("table").version(1L);

    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      @SuppressWarnings("unchecked")
      Future<DescribeTableResponse>[] futures = new Future[8];
      for (int i = 0; i < futures.length; i++) {
        futures[i] = executor.submit(() -> namespace.describeTable(request));
      }

      delegate.entered.await(10, TimeUnit.SECONDS);
      delegate.release.countDown();

      DescribeTableResponse first = futures[0].get(10, TimeUnit.SECONDS);
      for (Future<DescribeTableResponse> future : futures) {
        assertSame(first, future.get(10, TimeUnit.SECONDS));
      }
      assertEquals(1, delegate.describeCalls.get());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void refreshesBeforeTemporaryCredentialsExpire() {
    AtomicLong clock = new AtomicLong(100_000L);
    RecordingNamespace delegate = new RecordingNamespace(clock);
    ExecutorNamespaceCache.CredentialCachingNamespace namespace =
        new ExecutorNamespaceCache.CredentialCachingNamespace(delegate, clock::get);
    DescribeTableRequest request = new DescribeTableRequest().addIdItem("table").version(1L);

    DescribeTableResponse first = namespace.describeTable(request);
    clock.set(189_999L);
    assertSame(first, namespace.describeTable(request));

    // Credentials expire at 200_000. With a 10% safety window, refresh starts at 190_000.
    clock.set(190_000L);
    DescribeTableResponse refreshed = namespace.describeTable(request);

    assertEquals(2, delegate.describeCalls.get());
    assertEquals("token-2", refreshed.getStorageOptions().get("token"));
  }

  @Test
  public void retriesAfterDescribeTableFailure() {
    AtomicLong clock = new AtomicLong(100_000L);
    RecordingNamespace delegate = new RecordingNamespace(clock);
    delegate.failNext = true;
    ExecutorNamespaceCache.CredentialCachingNamespace namespace =
        new ExecutorNamespaceCache.CredentialCachingNamespace(delegate, clock::get);
    DescribeTableRequest request = new DescribeTableRequest().addIdItem("table").version(1L);

    assertThrows(IllegalStateException.class, () -> namespace.describeTable(request));
    DescribeTableResponse response = namespace.describeTable(request);

    assertEquals(2, delegate.describeCalls.get());
    assertEquals("token-2", response.getStorageOptions().get("token"));
  }

  @Test
  public void doesNotSpinWhenNamespaceReturnsExpiredCredentials() {
    AtomicInteger calls = new AtomicInteger();
    AtomicLong clock = new AtomicLong(100_000L);
    LanceNamespace delegate =
        new RecordingNamespace(clock) {
          @Override
          public DescribeTableResponse describeTable(DescribeTableRequest request) {
            int call = calls.incrementAndGet();
            return new DescribeTableResponse()
                .location("file:///tmp/table")
                .storageOptions(
                    Map.of(
                        "token",
                        "expired-" + call,
                        ExecutorNamespaceCache.EXPIRES_AT_MILLIS,
                        Long.toString(clock.get() - 1)));
          }
        };
    ExecutorNamespaceCache.CredentialCachingNamespace namespace =
        new ExecutorNamespaceCache.CredentialCachingNamespace(delegate, clock::get);
    DescribeTableRequest request = new DescribeTableRequest().addIdItem("table").version(1L);

    assertEquals("expired-1", namespace.describeTable(request).getStorageOptions().get("token"));
    assertEquals("expired-2", namespace.describeTable(request).getStorageOptions().get("token"));
    assertEquals(2, calls.get());
  }

  private static class RecordingNamespace implements LanceNamespace {
    final AtomicInteger describeCalls = new AtomicInteger();
    final AtomicLong clock;
    volatile boolean failNext;

    RecordingNamespace(AtomicLong clock) {
      this.clock = clock;
    }

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {}

    @Override
    public String namespaceId() {
      return "recording";
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      int call = describeCalls.incrementAndGet();
      if (failNext) {
        failNext = false;
        throw new IllegalStateException("injected failure");
      }
      return new DescribeTableResponse()
          .location("file:///tmp/table")
          .storageOptions(
              Map.of(
                  "token",
                  "token-" + call,
                  ExecutorNamespaceCache.EXPIRES_AT_MILLIS,
                  Long.toString(clock.get() + 100_000L)));
    }
  }

  private static final class BlockingNamespace extends RecordingNamespace {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    BlockingNamespace(AtomicLong clock) {
      super(clock);
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      entered.countDown();
      try {
        if (!release.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("timed out waiting to release describeTable");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return super.describeTable(request);
    }
  }

  public static final class CloseableNamespace implements LanceNamespace, AutoCloseable {
    static final AtomicInteger initializeCalls = new AtomicInteger();
    static final AtomicInteger closeCalls = new AtomicInteger();

    public CloseableNamespace() {}

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
      initializeCalls.incrementAndGet();
    }

    @Override
    public String namespaceId() {
      return "closeable";
    }

    @Override
    public void close() {
      closeCalls.incrementAndGet();
    }
  }
}
