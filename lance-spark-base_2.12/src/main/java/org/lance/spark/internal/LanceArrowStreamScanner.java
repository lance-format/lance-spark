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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.util.LanceArrowUtils;

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
 * <p>The export is zero-copy: it cannot re-project or reorder columns the way the Spark columnar
 * reader does on the JVM after import. So {@link #export} verifies that the schema the native scan
 * actually produces equals the declared partition schema (field names, in order) and rejects the
 * partition otherwise. This covers columns the columnar reader reconciles after import — the
 * synthesized {@code _fragid}, the {@code _score} a full-text query auto-projects, the {@code
 * _rowaddr} added for blob columns, the {@code _rowid} an empty projection surfaces, and reordered
 * row-version columns — none of which the raw stream can reproduce.
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
   *     cannot reproduce — a pushed aggregation, or a native scan schema that does not match the
   *     declared partition schema (see the class documentation)
   */
  public static LanceArrowStream export(int fragmentId, LanceInputPartition inputPartition) {
    checkNoPushedAggregation(inputPartition);
    LanceFragmentScanner fragmentScanner = LanceFragmentScanner.create(fragmentId, inputPartition);
    // Allocate inside the cleanup scope: the schema check and allocateNew can throw (a mismatch or
    // e.g. a bounded allocator), and the dataset + scanner opened by create() must still be closed.
    ArrowArrayStream stream = null;
    try {
      checkNativeSchemaMatchesPartition(fragmentScanner, inputPartition);
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
   * Rejects a pushed aggregation (e.g. {@code COUNT(*)}) before the scan is planned. {@link
   * org.lance.spark.read.LanceScan} routes such a partition to a dedicated reader that returns the
   * aggregate result, but {@link LanceFragmentScanner} ignores {@code pushedAggregation} and scans
   * data rows — and the aggregate partition's declared schema can equal the native data schema, so
   * the schema check below cannot catch it.
   */
  private static void checkNoPushedAggregation(LanceInputPartition inputPartition) {
    if (inputPartition.getPushedAggregation().isPresent()) {
      throw new UnsupportedOperationException(
          "Arrow C stream export does not support pushed aggregation (e.g. COUNT(*)): the "
              + "partition's Spark output is the aggregate result, but the native fragment scan "
              + "returns data rows. Fall back to the aggregate reader for this partition.");
    }
  }

  /**
   * Rejects a partition whose declared Spark schema differs from the schema the native scan
   * actually produces. The columnar reader reconciles such differences on the JVM after import —
   * synthesizing {@code _fragid}, dropping the {@code _score} a full-text query auto-projects,
   * stripping the {@code _rowaddr} added for blobs, surfacing {@code _rowid} for an empty
   * projection, reordering row-version columns — but this zero-copy export cannot, so any mismatch
   * must fall back to the columnar reader.
   *
   * <p>The comparison covers field names, order, Arrow types, and nullability. The declared Spark
   * schema is converted to Arrow through {@link LanceArrowUtils} — the same adapter the read path
   * uses — so it carries the Arrow type distinctions a Spark {@code DataType} alone cannot express
   * (e.g. {@code LargeUtf8} vs {@code Utf8}, {@code LargeBinary}, {@code Date(MILLISECOND)}, {@code
   * FixedSizeBinary}, {@code Float16}), which {@code fromArrowSchema} recorded in field metadata
   * and {@code toArrowSchema} restores here. Checking the {@code ArrowType} (not just the name)
   * stops a column whose native type differs from the declared one — e.g. a narrower/wider int or a
   * different time-zone timestamp — from being streamed to the native consumer as if it matched.
   */
  private static void checkNativeSchemaMatchesPartition(
      LanceFragmentScanner fragmentScanner, LanceInputPartition inputPartition) {
    List<Field> declared =
        LanceArrowUtils.toArrowSchema(inputPartition.getSchema(), "UTC", true).getFields();
    List<Field> nativeFields = fragmentScanner.schema().getFields();
    if (!fieldsMatch(declared, nativeFields)) {
      throw new UnsupportedOperationException(
          "Arrow C stream export requires the native scan schema to match the partition schema "
              + "(field names, order, Arrow types, and nullability), which this zero-copy export "
              + "cannot re-project, reorder, or cast. Declared "
              + describe(declared)
              + " but the native scan produced "
              + describe(nativeFields)
              + ". Fall back to the columnar reader for this partition.");
    }
  }

  /**
   * Compares two Arrow field lists by name, order, {@code ArrowType} (value equality over width,
   * precision, time zone, etc.), and nullability, recursing into children so nested list/struct
   * element types are checked too. Field-level metadata and dictionary encodings are intentionally
   * ignored: they carry lance-internal markers, not a difference the raw stream cannot reproduce.
   */
  private static boolean fieldsMatch(List<Field> declared, List<Field> actual) {
    if (declared.size() != actual.size()) {
      return false;
    }
    for (int i = 0; i < declared.size(); i++) {
      Field d = declared.get(i);
      Field a = actual.get(i);
      if (!d.getName().equals(a.getName())
          || d.isNullable() != a.isNullable()
          || !d.getType().equals(a.getType())
          || !fieldsMatch(d.getChildren(), a.getChildren())) {
        return false;
      }
    }
    return true;
  }

  private static String describe(List<Field> fields) {
    List<String> parts = new ArrayList<>(fields.size());
    for (Field field : fields) {
      String nullability = field.isNullable() ? " null" : " notnull";
      parts.add(field.getName() + " " + field.getType() + nullability);
    }
    return parts.toString();
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
