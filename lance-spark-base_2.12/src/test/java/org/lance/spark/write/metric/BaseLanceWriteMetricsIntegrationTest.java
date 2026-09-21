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
package org.lance.spark.write.metric;

import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end check that write metrics reach {@code TaskMetrics.outputMetrics}, which is what the
 * history server REST API and the benchmark harness read as stage outputBytes / outputRecords.
 */
public abstract class BaseLanceWriteMetricsIntegrationTest {
  private SparkSession spark;
  private OutputMetricsListener listener;

  @TempDir Path tempDir;

  private static final String TABLE = "lance.default.write_metrics_test";

  /** Sums the task-level output metrics of every completed task. */
  static class OutputMetricsListener extends SparkListener {
    final AtomicLong bytesWritten = new AtomicLong();
    final AtomicLong recordsWritten = new AtomicLong();

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.taskMetrics() != null) {
        bytesWritten.addAndGet(taskEnd.taskMetrics().outputMetrics().bytesWritten());
        recordsWritten.addAndGet(taskEnd.taskMetrics().outputMetrics().recordsWritten());
      }
    }
  }

  /** Captures the SQL-tab SQLMetric named bytesWritten from the executed plan. */
  static class SqlMetricListener implements QueryExecutionListener {
    final AtomicLong sqlBytesWritten = new AtomicLong(-1);
    final AtomicLong sqlRecordsWritten = new AtomicLong(-1);

    private void walk(SparkPlan plan) {
      scala.collection.Iterator<String> keys = plan.metrics().keysIterator();
      while (keys.hasNext()) {
        String key = keys.next();
        if (key.equals("bytesWritten")) {
          sqlBytesWritten.set(plan.metrics().apply(key).value());
        }
        if (key.equals("recordsWritten")) {
          sqlRecordsWritten.set(plan.metrics().apply(key).value());
        }
      }
      for (SparkPlan child : JavaConverters.seqAsJavaList(plan.children())) {
        walk(child);
      }
    }

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
      walk(qe.executedPlan());
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {}
  }

  private SqlMetricListener sqlListener;

  @BeforeEach
  public void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-write-metrics-test")
            .master("local")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.root", tempDir.toString())
            .getOrCreate();
    listener = new OutputMetricsListener();
    spark.sparkContext().addSparkListener(listener);
    sqlListener = new SqlMetricListener();
    spark.listenerManager().register(sqlListener);
    spark.sql(String.format("CREATE TABLE %s (id INT, text STRING) USING LANCE;", TABLE));
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testInsertPopulatesOutputMetrics() throws Exception {
    int rows = 50;
    spark.sql(
        String.format(
            "INSERT INTO %s (id, text) VALUES %s ;",
            TABLE,
            IntStream.range(0, rows)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
    spark.sparkContext().listenerBus().waitUntilEmpty(10000);

    assertEquals(rows, listener.recordsWritten.get(), "outputMetrics.recordsWritten");
    // No fragment completes before commit(), so nonzero proves the post-commit publish ran.
    assertTrue(
        listener.bytesWritten.get() > 0,
        "outputMetrics.bytesWritten should be > 0, was " + listener.bytesWritten.get());
  }

  /** The SQL tab carries recordsWritten only; bytesWritten reaches users via outputMetrics. */
  @Test
  void testBytesWrittenIsNotASqlMetric() throws Exception {
    int rows = 50;
    spark.sql(
        String.format(
            "INSERT INTO %s (id, text) VALUES %s ;",
            TABLE,
            IntStream.range(0, rows)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
    spark.sparkContext().listenerBus().waitUntilEmpty(10000);

    assertEquals(rows, sqlListener.sqlRecordsWritten.get(), "SQL recordsWritten");
    assertEquals(
        -1,
        sqlListener.sqlBytesWritten.get(),
        "bytesWritten must not be advertised as a SQL metric; it would always read 0");
    assertTrue(
        listener.bytesWritten.get() > 0,
        "outputMetrics.bytesWritten should be > 0, was " + listener.bytesWritten.get());
  }

  /**
   * A failed attempt must not leave its partial row count in the stage totals. Spark routes the
   * reserved names into output metrics from inside the write loop, and {@code AppStatusListener}
   * folds a task's metrics into the stage whatever its end reason, so without the clear in {@code
   * abort()} the retry's count lands on top of the failed attempt's.
   */
  @Test
  void testFailedAttemptDoesNotInflateOutputMetrics() throws Exception {
    spark.stop();
    // local[1,2]: one thread, two attempts, so the first failure is retried rather than fatal.
    spark =
        SparkSession.builder()
            .appName("lance-write-metrics-retry-test")
            .master("local[1,2]")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.root", tempDir.toString())
            .getOrCreate();
    OutputMetricsListener retryListener = new OutputMetricsListener();
    spark.sparkContext().addSparkListener(retryListener);

    String table = "lance.default.write_metrics_retry_test";
    spark.sql(String.format("CREATE TABLE %s (id INT, text STRING) USING LANCE;", table));

    int rows = 500;
    // Fails deep enough into the write that the in-loop metric update has already run.
    spark
        .udf()
        .register(
            "fail_first_attempt",
            (UDF1<Long, Long>)
                id -> {
                  if (TaskContext.get().attemptNumber() == 0 && id == 450L) {
                    throw new RuntimeException("injected failure on attempt 0");
                  }
                  return id;
                },
            DataTypes.LongType);

    spark.sql(
        String.format(
            "INSERT INTO %s SELECT CAST(fail_first_attempt(id) AS INT), CAST(id AS STRING) "
                + "FROM range(%d);",
            table, rows));
    spark.sparkContext().listenerBus().waitUntilEmpty(10000);

    assertEquals(
        rows,
        spark.sql(String.format("SELECT * FROM %s;", table)).count(),
        "the retry should have written every row exactly once");
    assertEquals(
        rows,
        retryListener.recordsWritten.get(),
        "stage outputRecords must count the committed rows only, not the failed attempt's");
  }
}
