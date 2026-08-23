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

import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.BlobUtils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Exports a Lance fragment scan as an Arrow C Data Interface stream ({@link ArrowArrayStream}) for
 * native consumers such as Apache Gluten / Velox.
 *
 * <p>Only the {@link ArrowArrayStream} C-struct address ({@link LanceArrowStream#streamAddress()})
 * crosses the JVM/native boundary, so the consumer's Arrow build and classloader do not need to
 * match lance-spark's. Scan planning — column projection, filter pushdown, limit/offset, batch size
 * — is delegated to {@link LanceFragmentScanner}.
 *
 * <p>{@link #export} rejects a partition with a pushed aggregation (e.g. {@code COUNT(*)}): its
 * Spark output is the aggregate result produced by a dedicated reader ({@link
 * org.lance.spark.read.LanceCountStarPartitionReader}), not the data rows this fragment scan
 * returns. Filter, limit, offset, and top-N ordering, by contrast, are pushed faithfully into the
 * native scan, so they are exportable.
 *
 * <p>The exported stream carries the raw native scan schema, which for a projection of ordinary
 * data columns is exactly the rows and order the Spark columnar reader produces. Shapes that the
 * columnar reader fixes up on the JVM after import — the synthesized {@code _fragid} column, an
 * empty projection (which surfaces the internal {@code _rowid}), metadata columns ({@code _rowid} /
 * {@code _rowaddr} / row-version / {@code _score}), or blob columns — would export a schema that
 * does not match the partition, so {@link #export} rejects them with {@link
 * UnsupportedOperationException} and the caller must fall back to the columnar reader.
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
   * @throws UnsupportedOperationException if the partition needs execution this raw fragment scan
   *     cannot reproduce — a pushed aggregation, or a schema that needs JVM-side post-processing
   *     (see the class documentation)
   */
  public static LanceArrowStream export(int fragmentId, LanceInputPartition inputPartition) {
    checkExportablePartition(inputPartition);
    LanceFragmentScanner fragmentScanner = LanceFragmentScanner.create(fragmentId, inputPartition);
    // Allocate inside the cleanup scope: allocateNew can fail (e.g. a bounded allocator), and the
    // dataset + scanner opened by create() above must still be closed on that path.
    ArrowArrayStream stream = null;
    try {
      stream = ArrowArrayStream.allocateNew(LanceRuntime.allocator());
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

  /**
   * Rejects partitions whose Spark-visible output differs from the raw native fragment scan.
   *
   * <p>A pushed aggregation (e.g. {@code COUNT(*)}) is rejected first: {@link
   * org.lance.spark.read.LanceScan} routes it to a dedicated reader that returns the aggregate
   * result, but {@link LanceFragmentScanner} ignores {@code pushedAggregation} and scans data rows.
   *
   * <p>A schema is then rejected if it names columns {@link LanceFragmentScanner} drops from the
   * native projection while {@link org.lance.spark.read.LanceFragmentColumnarBatchScanner}
   * synthesizes/strips/reorders them after import, so exporting the native stream as-is would
   * surface a schema that does not match the partition. Such partitions are not offloadable through
   * this path; the caller must read them with the columnar / aggregate reader instead.
   */
  private static void checkExportablePartition(LanceInputPartition inputPartition) {
    if (inputPartition.getPushedAggregation().isPresent()) {
      throw new UnsupportedOperationException(
          "Arrow C stream export does not support pushed aggregation (e.g. COUNT(*)): the "
              + "partition's Spark output is the aggregate result, but the native fragment scan "
              + "returns data rows. Fall back to the aggregate reader for this partition.");
    }
    StructType schema = inputPartition.getSchema();
    if (schema.isEmpty()) {
      throw new UnsupportedOperationException(
          "Arrow C stream export requires a non-empty column projection: an empty projection makes "
              + "the native scan surface the internal _rowid column. Fall back to the columnar "
              + "reader for this partition.");
    }
    List<String> unsupported = new ArrayList<>();
    for (StructField field : schema.fields()) {
      String name = field.name();
      if (name.equals(LanceConstant.FRAGMENT_ID)
          || name.equals(LanceConstant.ROW_ID)
          || name.equals(LanceConstant.ROW_ADDRESS)
          || name.equals(LanceConstant.ROW_CREATED_AT_VERSION)
          || name.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
          || name.equals(LanceConstant.SCORE)
          || name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
          || name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX)
          || BlobUtils.isBlobReadColumn(field)) {
        unsupported.add(name);
      }
    }
    if (!unsupported.isEmpty()) {
      throw new UnsupportedOperationException(
          "Arrow C stream export does not support metadata or blob columns that the columnar "
              + "reader synthesizes, strips, or reorders relative to the native scan: "
              + unsupported
              + ". Fall back to the columnar reader for this partition.");
    }
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
   * <p>{@link #close()} runs the stream's release callback (tearing down the native scan), frees
   * the stream struct, and then closes the Lance scanner and dataset handles, in that order. These
   * are distinct resources: the stream drives the native scan, while the scanner owns the open
   * dataset. Running the release callback here is what frees the native provider state when a
   * consumer abandons or only partially consumes the raw C stream; it is skipped when a consumer
   * has already imported or released the stream.
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
      // Run the C release callback (drops the native provider's private_data + record-batch
      // stream), then free the struct buffer, then release the scanner and the dataset it holds.
      primary = releaseStream(primary);
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

    /**
     * Invokes the stream's release callback so an abandoned or partially-consumed raw C stream
     * still drops its native provider state, then leaves {@link #close()} to free the struct.
     *
     * <p>Idempotent against a consumer that already took the stream: {@code ArrowArrayStreamReader}
     * snapshots the callback into its own struct and closes this one on import, so the struct may
     * be freed ({@link ArrowArrayStream#snapshot()} throws) or the callback already moved/released
     * (release address is {@code NULL} per the Arrow C ABI). In both cases there is nothing to
     * release here.
     */
    private Throwable releaseStream(Throwable primary) {
      long releaseCallback;
      try {
        releaseCallback = stream.snapshot().release;
      } catch (RuntimeException alreadyClosed) {
        return primary;
      }
      if (releaseCallback == 0L) {
        return primary;
      }
      try {
        stream.release();
        return primary;
      } catch (Throwable t) {
        return accumulate(primary, t);
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
        return accumulate(primary, t);
      }
    }

    private static Throwable accumulate(Throwable primary, Throwable t) {
      if (primary != null) {
        primary.addSuppressed(t);
        return primary;
      }
      return t;
    }
  }
}
