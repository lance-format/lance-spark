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

/**
 * Runtime utilities for Lance Spark connector.
 *
 * <p>This class manages a global Arrow buffer allocator with size configurable via environment
 * variable.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * BufferAllocator allocator = LanceRuntime.allocator();
 * }</pre>
 */
public final class LanceRuntime {

  /** Environment variable for allocator size. */
  public static final String ENV_ALLOCATOR_SIZE = "LANCE_ALLOCATOR_SIZE";

  /** Default allocator size (unlimited). */
  public static final long DEFAULT_ALLOCATOR_SIZE = Long.MAX_VALUE;

  /** Global allocator (lazy initialized based on env var). */
  private static volatile BufferAllocator GLOBAL_ALLOCATOR;

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
