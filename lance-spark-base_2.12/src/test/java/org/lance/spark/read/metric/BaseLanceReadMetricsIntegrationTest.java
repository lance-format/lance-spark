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
package org.lance.spark.read.metric;

import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseLanceReadMetricsIntegrationTest {
  private static SparkSession spark;
  private static Dataset<Row> data;
  private static MetricsCapturingListener metricsListener;

  // Spark registers custom metric accumulators using description() as the name,
  // so we build a reverse lookup: description -> metric name.
  private static final Map<String, String> DESC_TO_NAME = new HashMap<>();

  static {
    for (CustomMetric m : LanceCustomMetrics.allMetrics()) {
      DESC_TO_NAME.put(m.description(), m.name());
    }
  }

  /**
   * SparkListener that captures custom metric accumulator values from task-end events. Keyed by
   * metric name (e.g. "numFragmentsScanned"), translated from the description that Spark uses as
   * the accumulator name.
   */
  static class MetricsCapturingListener extends SparkListener {
    private final Map<String, Long> metricValues = new ConcurrentHashMap<>();

    void reset() {
      metricValues.clear();
    }

    Map<String, Long> getMetricValues() {
      return metricValues;
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.taskInfo() != null) {
        JavaConverters.seqAsJavaList(taskEnd.taskInfo().accumulables())
            .forEach(
                info -> {
                  if (info.name().isDefined() && info.update().isDefined()) {
                    String desc = info.name().get();
                    String metricName = DESC_TO_NAME.get(desc);
                    if (metricName != null) {
                      Object value = info.update().get();
                      if (value instanceof Long) {
                        metricValues.merge(metricName, (Long) value, Long::sum);
                      }
                    }
                  }
                });
      }
    }
  }

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-metrics-test")
            .master("local")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .getOrCreate();
    metricsListener = new MetricsCapturingListener();
    spark.sparkContext().addSparkListener(metricsListener);
    data =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(
                    TestUtils.TestTable1Config.dbPath, TestUtils.TestTable1Config.datasetName))
            .load();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testSupportedCustomMetricsCount() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    assertEquals(6, metrics.length);
  }

  @Test
  void testReadQueryCompletes() {
    // Execute a read query and collect results to verify metrics don't break normal reading
    Dataset<Row> result = data.select("x", "y");
    long count = result.count();
    assertTrue(count > 0, "Should read at least one row");
  }

  @Test
  void testCountStarWithFilterCompletes() {
    // COUNT(*) with filter goes through LanceCountStarPartitionReader
    data.createOrReplaceTempView("metrics_test");
    Dataset<Row> result = spark.sql("SELECT COUNT(*) FROM metrics_test WHERE x > 0");
    Row row = result.collectAsList().get(0);
    long count = row.getLong(0);
    assertTrue(count > 0, "Filtered count should be > 0");
  }

  @Test
  void testSelectAllColumnsCompletes() {
    // Full row read exercises the columnar partition reader metrics path
    java.util.List<Row> rows = data.collectAsList();
    assertTrue(rows.size() > 0, "Should collect at least one row");
  }

  @Test
  void testReadMetricsValues() throws Exception {
    metricsListener.reset();
    // Execute a full read query to trigger columnar partition reader metrics
    data.select("x", "y").collect();
    // Allow listener to process events
    spark.sparkContext().listenerBus().waitUntilEmpty(5000);

    Map<String, Long> metrics = metricsListener.getMetricValues();

    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_FRAGMENTS_SCANNED, 0L) > 0,
        "numFragmentsScanned should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BATCHES_LOADED, 0L) >= 1,
        "numBatchesLoaded should be >= 1");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_ROWS_SCANNED, 0L) > 0,
        "numRowsScanned should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.DATASET_OPEN_TIME_NS, 0L) > 0,
        "datasetOpenTimeNs should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.SCANNER_CREATE_TIME_NS, 0L) > 0,
        "scannerCreateTimeNs should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.BATCH_LOAD_TIME_NS, 0L) > 0,
        "batchLoadTimeNs should be > 0");
  }

  @Test
  void testCountStarMetricsValues() throws Exception {
    metricsListener.reset();
    data.createOrReplaceTempView("metrics_count_test");
    spark.sql("SELECT COUNT(*) FROM metrics_count_test WHERE x > 0").collect();
    spark.sparkContext().listenerBus().waitUntilEmpty(5000);

    Map<String, Long> metrics = metricsListener.getMetricValues();

    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_FRAGMENTS_SCANNED, 0L) > 0,
        "numFragmentsScanned should be > 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BATCHES_LOADED, 0L) >= 1,
        "numBatchesLoaded should be >= 1 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_ROWS_SCANNED, 0L) > 0,
        "numRowsScanned should be > 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.DATASET_OPEN_TIME_NS, 0L) > 0,
        "datasetOpenTimeNs should be > 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.SCANNER_CREATE_TIME_NS, 0L) > 0,
        "scannerCreateTimeNs should be > 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.BATCH_LOAD_TIME_NS, 0L) > 0,
        "batchLoadTimeNs should be > 0 for COUNT(*)");
  }
}
