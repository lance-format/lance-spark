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

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseLanceReadMetricsIntegrationTest {
  private SparkSession spark;
  private MetricsCapturingListener metricsListener;

  @TempDir Path tempDir;
  private String fullTable = "lance.default.metrics_test";

  // Spark registers custom metric accumulators using description() as the name,
  // so we build a reverse lookup: description -> metric name.
  private static final Map<String, String> DESC_TO_NAME = new HashMap<>();

  static {
    for (CustomMetric m : LanceCustomMetrics.allMetrics()) {
      DESC_TO_NAME.put(m.description(), m.name());
    }
  }

  /**
   * SparkListener that captures custom metric accumulator values FROM task-end events. Keyed by
   * metric name (e.g. "numFragmentsScanned"), translated FROM the description that Spark uses as
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

  @BeforeEach
  public void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-metrics-test")
            .master("local")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.root", tempDir.toString())
            .getOrCreate();
    metricsListener = new MetricsCapturingListener();
    spark.sparkContext().addSparkListener(metricsListener);

    prepareDataset();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private void prepareDataset() {
    spark.sql(String.format("CREATE TABLE %s (id INT, text STRING) USING LANCE;", fullTable));
    spark.sql(
        String.format(
            "INSERT INTO %s (id, text) VALUES %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  @Test
  void testSupportedCustomMetricsCount() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    assertEquals(12, metrics.length);
  }

  @Test
  void testReadQueryCompletes() {
    // Execute a read query and collect results to verify metrics don't break normal reading
    Dataset<Row> result = spark.sql(String.format("SELECT id FROM %s", fullTable));
    long COUNT = result.count();
    assertTrue(COUNT > 0, "Should read at least one row");
  }

  @Test
  void testCountStarWithFilterCompletes() {
    Dataset<Row> result =
        spark.sql(String.format("SELECT COUNT(*) FROM %s WHERE id > 0", fullTable));
    Row row = result.collectAsList().get(0);
    long COUNT = row.getLong(0);
    assertTrue(COUNT > 0, "Filtered COUNT should be > 0");
  }

  @Test
  void testSelectAllColumnsCompletes() {
    // Full row read exercises the columnar partition reader metrics path
    java.util.List<Row> rows =
        spark.sql(String.format("SELECT COUNT(*) FROM %s", fullTable)).collectAsList();
    assertTrue(rows.size() > 0, "Should collect at least one row");
  }

  @Test
  void testReadMetricsValues() throws Exception {
    metricsListener.reset();
    // Execute a full read query to trigger columnar partition reader metrics
    spark.sql(String.format("SELECT * FROM %s", fullTable)).collectAsList();
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

    assertTrue(metrics.getOrDefault(LanceCustomMetrics.NUM_IOPS, 0L) > 0, "numIops should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_REQUESTS, 0L) > 0, "numRequests should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BYTES_READ, 0L) > 0,
        "numBytesRead should be > 0");

    // No indexes to read
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_INDICES_LOADED, 0L) == 0,
        "numIndicesLoaded should be == 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_PARTS_LOADED, 0L) == 0,
        "numPartsLoaded should be == 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_INDEX_COMPARISONS, 0L) == 0,
        "numIndexComparisons should be == 0");
  }

  @Test
  void testReadMetricsForIndices() throws Exception {
    metricsListener.reset();

    // Create btree indices
    spark
        .sql(String.format("ALTER TABLE %s CREATE INDEX idx_btree_id USING BTREE(id)", fullTable))
        .collectAsList();

    spark.sql(String.format("SELECT * FROM %s WHERE id > 1", fullTable)).collectAsList();

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

    assertTrue(metrics.getOrDefault(LanceCustomMetrics.NUM_IOPS, 0L) > 0, "numIops should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_REQUESTS, 0L) > 0, "numRequests should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BYTES_READ, 0L) > 0,
        "numBytesRead should be > 0");

    // Some indexes to read
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_INDICES_LOADED, 0L) > 0,
        "numIndicesLoaded should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_PARTS_LOADED, 0L) > 0,
        "numPartsLoaded should be > 0");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_INDEX_COMPARISONS, 0L) > 0,
        "numIndexComparisons should be > 0");
  }

  @Test
  void testCountStarMetricsValues() throws Exception {
    metricsListener.reset();
    spark.sql(String.format("SELECT COUNT(*) FROM %s WHERE id > 0", fullTable)).collect();
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

    // Test new metrics
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_IOPS, 0L) > 0,
        "numIops should be >= 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_REQUESTS, 0L) > 0,
        "numRequests should be >= 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BYTES_READ, 0L) > 0,
        "numBytesRead should be >= 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_INDICES_LOADED, 0L) >= 0,
        "numIndicesLoaded should be >= 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_PARTS_LOADED, 0L) >= 0,
        "numPartsLoaded should be >= 0 for COUNT(*)");
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_INDEX_COMPARISONS, 0L) >= 0,
        "numIndexComparisons should be >= 0 for COUNT(*)");
  }
}
