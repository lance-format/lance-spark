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

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end check that write metrics reach {@code TaskMetrics.outputMetrics}, which is what the
 * history server REST API (and lance's own benchmark harness) reads as stage outputBytes /
 * outputRecords. Before write metrics existed both were always 0.
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

    // Exact: every row handed to the writer is counted once, and the value is absolute, so the
    // repeated currentMetricsValues() polls plus the post-commit publish cannot inflate it.
    assertEquals(rows, listener.recordsWritten.get(), "outputMetrics.recordsWritten");
    // The only fragment here is completed inside commit(), after Spark's last
    // currentMetricsValues() call, so a nonzero value proves the post-commit publish works.
    assertTrue(
        listener.bytesWritten.get() > 0,
        "outputMetrics.bytesWritten should be > 0, was " + listener.bytesWritten.get());
  }
}
