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
package org.lance.spark.write;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.Utils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import scala.Option;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for backfill writers that buffer rows per fragment and then process each
 * fragment's data via a Lance fragment operation (merge or update).
 *
 * <p>Subclasses implement {@link #processFragment} to perform the specific operation and {@link
 * #buildCommitMessage} to construct the appropriate commit message.
 *
 * <p>By default a write task buffers the rows of every fragment it receives and flushes them one by
 * one on the task thread when it commits. {@link #FLUSH_EAGERLY_ENABLED_CONF} and {@link
 * #FLUSH_ASYNC_ENABLED_CONF} bound the buffered memory and overlap the flushes.
 */
public abstract class AbstractBackfillWriter implements DataWriter<InternalRow> {
  /**
   * Whether the rows of each write task are sorted by {@code _fragid} and every fragment is flushed
   * (merged or updated, then released) as soon as the next one starts, instead of buffering every
   * fragment of the task until it commits.
   */
  public static final String FLUSH_EAGERLY_ENABLED_CONF =
      "spark.sql.lance.backfill.flush_eagerly.enabled";

  /** Whether fragments are flushed on a thread pool of the writer instead of the task thread. */
  public static final String FLUSH_ASYNC_ENABLED_CONF =
      "spark.sql.lance.backfill.flush_async.enabled";

  /**
   * Number of threads, and thus of fragments flushed at the same time, of a writer. Only effective
   * when {@link #FLUSH_ASYNC_ENABLED_CONF} is true.
   */
  public static final String FLUSH_ASYNC_POOL_SIZE_CONF =
      "spark.sql.lance.backfill.flush_async.pool_size";

  public static final int DEFAULT_FLUSH_ASYNC_POOL_SIZE = 4;

  private final LanceSparkWriteOptions writeOptions;
  private final StructType schema;
  private final int fragmentIdField;
  private final StructType writerSchema;
  private final Map<Integer, FragmentBuffer> buffers = new HashMap<>();

  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  private static final AtomicInteger FLUSH_THREAD_COUNTER = new AtomicInteger();

  /**
   * Whether the input rows are ordered by fragment id, see {@link #FLUSH_EAGERLY_ENABLED_CONF}. If
   * so, each fragment is flushed as soon as the next one starts instead of on commit.
   */
  private final boolean dataFragIdOrdered;

  /**
   * Whether fragments are flushed on {@link #flushExecutor}, see {@link #FLUSH_ASYNC_ENABLED_CONF}.
   */
  private final boolean flushAsync;

  /** Flushing asynchronously only: the maximum number of fragments flushed at the same time. */
  private final int flushAsyncPoolSize;

  /** Data ordered by fragment id only: the fragment being buffered and the fragments flushed. */
  private Integer currentFragmentId;

  private final Set<Integer> flushedFragmentIds = new HashSet<>();

  /** Flushing asynchronously only: the flushes not awaited yet, oldest first. */
  private final Deque<Future<?>> inflightFlushes = new ArrayDeque<>();

  private ExecutorService flushExecutor;

  /**
   * Flushing eagerly or asynchronously only: the dataset opened by the first flush, closed on
   * commit or close.
   */
  private Dataset dataset;

  private static class FragmentBuffer {
    final VectorSchemaRoot data;
    final org.lance.spark.arrow.LanceArrowWriter writer;

    FragmentBuffer(VectorSchemaRoot data, org.lance.spark.arrow.LanceArrowWriter writer) {
      this.data = data;
      this.writer = writer;
    }
  }

  protected AbstractBackfillWriter(
      LanceSparkWriteOptions writeOptions,
      StructType schema,
      List<String> targetColumns,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean dataFragIdOrdered,
      boolean flushAsync,
      int flushAsyncPoolSize) {
    if (flushAsync && flushAsyncPoolSize <= 0) {
      throw new IllegalArgumentException(
          "flushAsyncPoolSize must be positive, but was " + flushAsyncPoolSize);
    }
    this.dataFragIdOrdered = dataFragIdOrdered;
    this.flushAsync = flushAsync;
    this.flushAsyncPoolSize = flushAsyncPoolSize;
    this.writeOptions = writeOptions;
    this.schema = schema;
    this.fragmentIdField = schema.fieldIndex(LanceDataset.FRAGMENT_ID_COLUMN.name());
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;

    StructType ws = new StructType();
    for (org.apache.spark.sql.types.StructField f : schema.fields()) {
      if (targetColumns.contains(f.name())
          || f.name().equals(LanceDataset.ROW_ADDRESS_COLUMN.name())) {
        ws = ws.add(f);
      }
    }
    this.writerSchema = ws;
  }

  /** Reads a setting from the active session, {@code defaultValue} if unset or no session. */
  private static String sessionConf(String key, String defaultValue) {
    Option<SparkSession> session = SparkSession.getActiveSession();
    return session.isDefined() ? session.get().conf().get(key, defaultValue) : defaultValue;
  }

  static boolean isFlushEagerlyEnabled() {
    return Boolean.parseBoolean(sessionConf(FLUSH_EAGERLY_ENABLED_CONF, "false"));
  }

  static boolean isFlushAsyncEnabled() {
    return Boolean.parseBoolean(sessionConf(FLUSH_ASYNC_ENABLED_CONF, "false"));
  }

  /** Reads {@link #FLUSH_ASYNC_POOL_SIZE_CONF}, which must be positive. */
  static int flushAsyncPoolSize() {
    int value =
        Integer.parseInt(
            sessionConf(FLUSH_ASYNC_POOL_SIZE_CONF, String.valueOf(DEFAULT_FLUSH_ASYNC_POOL_SIZE))
                .trim());
    if (value <= 0) {
      throw new IllegalArgumentException(
          FLUSH_ASYNC_POOL_SIZE_CONF + " must be positive, but was " + value);
    }
    return value;
  }

  /** The ordering a backfill write requires within each task. */
  static SortOrder[] requiredOrdering(boolean flushEagerly) {
    if (flushEagerly) {
      return new SortOrder[] {
        Expressions.sort(Expressions.column(LanceConstant.FRAGMENT_ID), SortDirection.ASCENDING)
      };
    }
    return new SortOrder[0];
  }

  @Override
  public void write(InternalRow record) throws IOException {
    int fragId = record.getInt(fragmentIdField);
    if (dataFragIdOrdered) {
      advanceFragment(fragId);
    }

    FragmentBuffer buffer =
        buffers.computeIfAbsent(
            fragId,
            id -> {
              BufferAllocator allocator = LanceRuntime.allocator();
              VectorSchemaRoot data =
                  VectorSchemaRoot.create(
                      LanceArrowUtils.toArrowSchema(writerSchema, "UTC", false), allocator);
              org.lance.spark.arrow.LanceArrowWriter writer =
                  org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(data, writerSchema);
              return new FragmentBuffer(data, writer);
            });

    for (int i = 0; i < writerSchema.fields().length; i++) {
      buffer.writer.field(i).write(record, schema.fieldIndex(writerSchema.fields()[i].name()));
    }
  }

  /** Data ordered by fragment id: flushes the buffered fragment once the input moves on. */
  private void advanceFragment(int fragId) {
    if (currentFragmentId != null && currentFragmentId == fragId) {
      return;
    }
    if (flushedFragmentIds.contains(fragId)) {
      throw new IllegalStateException(
          "Rows of fragment "
              + fragId
              + " arrived after the fragment was flushed; the input of each task must be sorted by "
              + LanceDataset.FRAGMENT_ID_COLUMN.name());
    }
    if (currentFragmentId != null) {
      FragmentBuffer buffer = buffers.remove(currentFragmentId);
      if (buffer != null) {
        flush(currentFragmentId, buffer);
      }
      flushedFragmentIds.add(currentFragmentId);
    }
    currentFragmentId = fragId;
  }

  /**
   * Flushes the fragment and releases its buffer. When flushing asynchronously, the flush runs on
   * {@link #flushExecutor}, after waiting for a free slot if {@link #flushAsyncPoolSize} fragments
   * are already being flushed.
   */
  private void flush(int fragmentId, FragmentBuffer buffer) {
    Dataset ds;
    try {
      if (flushAsync) {
        awaitFlushes(flushAsyncPoolSize - 1);
      }
      ds = openDataset();
    } catch (RuntimeException e) {
      buffer.data.close();
      throw e;
    }
    if (flushAsync) {
      inflightFlushes.add(flushExecutor().submit(() -> flushFragment(ds, fragmentId, buffer)));
    } else {
      flushFragment(ds, fragmentId, buffer);
    }
  }

  /** Waits for the oldest flushes until at most {@code remaining} are in flight. */
  private void awaitFlushes(int remaining) {
    while (inflightFlushes.size() > remaining) {
      try {
        inflightFlushes.peek().get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for a fragment flush.", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new RuntimeException(cause);
      }
      inflightFlushes.poll();
    }
  }

  private ExecutorService flushExecutor() {
    if (flushExecutor == null) {
      flushExecutor =
          Executors.newFixedThreadPool(
              flushAsyncPoolSize,
              runnable -> {
                Thread thread =
                    new Thread(
                        runnable, "lance-backfill-flush-" + FLUSH_THREAD_COUNTER.getAndIncrement());
                thread.setDaemon(true);
                return thread;
              });
    }
    return flushExecutor;
  }

  private Dataset openDataset() {
    if (dataset == null) {
      dataset =
          Utils.openDatasetBuilder(writeOptions)
              .initialStorageOptions(initialStorageOptions)
              .build();
    }
    return dataset;
  }

  private void flushFragment(Dataset dataset, int fragmentId, FragmentBuffer buffer) {
    try {
      buffer.writer.finish();
      BufferAllocator allocator = LanceRuntime.allocator();

      try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
          ArrowReader reader = new SingleBatchArrowReader(allocator, buffer.data)) {
        Data.exportArrayStream(allocator, reader, stream);

        Fragment fragment = new Fragment(dataset, fragmentId);
        processFragment(fragment, stream);
      } catch (Exception e) {
        throw new RuntimeException("Cannot read arrow stream.", e);
      }
    } finally {
      buffer.data.close();
    }
  }

  /**
   * Process a single fragment's buffered data. Subclasses call the appropriate Lance fragment
   * operation (e.g. mergeColumns or updateColumns) and store the results.
   *
   * <p>When flushing asynchronously, this runs on a flush thread and up to {@link
   * #flushAsyncPoolSize} calls may run concurrently, so storing the results must be thread-safe.
   */
  protected abstract void processFragment(Fragment fragment, ArrowArrayStream stream);

  /** Build the commit message from accumulated results after all fragments have been processed. */
  protected abstract WriterCommitMessage buildCommitMessage();

  @Override
  public WriterCommitMessage commit() {
    if (dataFragIdOrdered || flushAsync) {
      try {
        Iterator<Map.Entry<Integer, FragmentBuffer>> it = buffers.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Integer, FragmentBuffer> entry = it.next();
          it.remove();
          flush(entry.getKey(), entry.getValue());
        }
        awaitFlushes(0);
      } finally {
        closeFlushes();
      }
      return buildCommitMessage();
    }

    try (Dataset dataset =
        Utils.openDatasetBuilder(writeOptions)
            .initialStorageOptions(initialStorageOptions)
            .build()) {
      for (Map.Entry<Integer, FragmentBuffer> entry : buffers.entrySet()) {
        flushFragment(dataset, entry.getKey(), entry.getValue());
      }
    }

    return buildCommitMessage();
  }

  @Override
  public void abort() {}

  @Override
  public void close() throws IOException {
    closeFlushes();
    for (FragmentBuffer buffer : buffers.values()) {
      buffer.data.close();
    }
    buffers.clear();
  }

  /**
   * Waits for the flushes still running, ignoring their failures, then releases the flush executor
   * and the dataset they use.
   */
  private void closeFlushes() {
    while (!inflightFlushes.isEmpty()) {
      try {
        inflightFlushes.peek().get();
      } catch (InterruptedException e) {
        // Closing the dataset below still waits for the running flushes.
        Thread.currentThread().interrupt();
        break;
      } catch (ExecutionException e) {
        // The write already fails, or the failure was thrown by awaitFlushes.
      }
      inflightFlushes.poll();
    }
    inflightFlushes.clear();
    if (flushExecutor != null) {
      flushExecutor.shutdown();
      flushExecutor = null;
    }
    if (dataset != null) {
      dataset.close();
      dataset = null;
    }
  }
}
