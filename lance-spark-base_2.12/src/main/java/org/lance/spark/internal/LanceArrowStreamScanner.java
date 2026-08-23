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

import org.lance.spark.LanceRuntime;
import org.lance.spark.read.LanceInputPartition;

import org.apache.arrow.c.ArrowArrayStream;

import java.io.IOException;

/**
 * Exports a Lance fragment scan as an Arrow C Data Interface stream ({@link ArrowArrayStream}) for
 * native consumers such as Apache Gluten / Velox.
 *
 * <p>Only the {@link ArrowArrayStream} C-struct address ({@link LanceArrowStream#streamAddress()})
 * crosses the JVM/native boundary, so the consumer's Arrow build and classloader do not need to
 * match lance-spark's. All scan planning — column projection, filter pushdown, limit/offset, row-id
 * / row-address, batch size — is delegated to {@link LanceFragmentScanner}, so this path produces
 * exactly the same rows in the same order as the Spark columnar reader.
 *
 * <p>The Lance native core writes batches into the caller-owned stream on demand as the consumer
 * pulls them, so no Arrow data is materialized on the JVM heap on this path.
 */
public final class LanceArrowStreamScanner {

  private LanceArrowStreamScanner() {}

  /**
   * Plans a fragment scan and exports it as an Arrow C stream.
   *
   * <p>The returned {@link LanceArrowStream} owns the exported stream and the scan backing it. Hand
   * {@link LanceArrowStream#streamAddress()} to the native consumer, let it drain the stream to
   * exhaustion, then {@link LanceArrowStream#close() close} the handle. Closing before the consumer
   * has finished reading is a use-after-free on caller-owned native memory.
   *
   * @param fragmentId the Lance fragment to scan
   * @param inputPartition the planned partition (schema, filter, limit/offset, storage options)
   * @return an open Arrow C stream handle over the fragment scan
   */
  public static LanceArrowStream export(int fragmentId, LanceInputPartition inputPartition) {
    LanceFragmentScanner fragmentScanner = LanceFragmentScanner.create(fragmentId, inputPartition);
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(LanceRuntime.allocator());
    try {
      // The Lance native core populates the caller-owned stream directly from the planned scan, so
      // no Arrow batch is ever materialized on the JVM heap here — the consumer pulls batches over
      // the C Data Interface. The stream's release callback routes back to the native side, so
      // releasing the stream (by the consumer, or by LanceArrowStream#close) tears down the scan.
      fragmentScanner.exportArrowStream(stream.memoryAddress());
    } catch (Throwable t) {
      closeQuietly(stream);
      closeQuietly(fragmentScanner);
      if (t instanceof RuntimeException) {
        throw (RuntimeException) t;
      }
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw new RuntimeException(t);
    }
    return new LanceArrowStream(stream, fragmentScanner);
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception ignore) {
        // Best effort on the construction error path.
      }
    }
  }

  /**
   * Owns an exported {@link ArrowArrayStream} together with the fragment scan behind it.
   *
   * <p>{@link #close()} releases, in order, the exported stream (whose release callback tears down
   * the native scan, freeing its buffers) and then the Lance scanner and dataset handles. These are
   * distinct resources: the stream drives the native scan, while the scanner owns the open dataset.
   */
  public static final class LanceArrowStream implements AutoCloseable {
    private final ArrowArrayStream stream;
    private final LanceFragmentScanner fragmentScanner;

    LanceArrowStream(ArrowArrayStream stream, LanceFragmentScanner fragmentScanner) {
      this.stream = stream;
      this.fragmentScanner = fragmentScanner;
    }

    /** The Arrow C Data Interface stream backing this scan. */
    public ArrowArrayStream stream() {
      return stream;
    }

    /**
     * The C-struct address to hand to a native consumer (e.g. a Velox Arrow-stream source). Valid
     * until {@link #close()}.
     */
    public long streamAddress() {
      return stream.memoryAddress();
    }

    @Override
    public void close() throws IOException {
      Throwable primary = null;
      // Closing the stream runs its release callback, tearing down the native scan and its buffers;
      // then release the scanner and the dataset it holds open.
      primary = closeAndAccumulate(stream, primary);
      primary = closeAndAccumulate(fragmentScanner, primary);
      if (primary != null) {
        if (primary instanceof IOException) {
          throw (IOException) primary;
        }
        if (primary instanceof RuntimeException) {
          throw (RuntimeException) primary;
        }
        if (primary instanceof Error) {
          throw (Error) primary;
        }
        throw new IOException(primary);
      }
    }

    private static Throwable closeAndAccumulate(AutoCloseable closeable, Throwable primary) {
      if (closeable == null) {
        return primary;
      }
      try {
        closeable.close();
        return primary;
      } catch (Throwable t) {
        if (primary != null) {
          primary.addSuppressed(t);
          return primary;
        }
        return t;
      }
    }
  }
}
