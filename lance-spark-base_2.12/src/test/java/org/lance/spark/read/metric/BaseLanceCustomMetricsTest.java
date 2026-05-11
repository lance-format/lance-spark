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

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseLanceCustomMetricsTest {

  @Test
  void testAllMetricsReturnsSixMetrics() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    assertEquals(6, metrics.length);
  }

  @Test
  void testMetricNamesAreUnique() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    Set<String> names = new HashSet<>();
    for (CustomMetric m : metrics) {
      assertTrue(names.add(m.name()), "Duplicate metric name: " + m.name());
    }
  }

  @Test
  void testMetricNamesMatchConstants() {
    Set<String> expected =
        new HashSet<>(
            Arrays.asList(
                LanceCustomMetrics.NUM_FRAGMENTS_SCANNED,
                LanceCustomMetrics.NUM_BATCHES_LOADED,
                LanceCustomMetrics.NUM_ROWS_SCANNED,
                LanceCustomMetrics.DATASET_OPEN_TIME_NS,
                LanceCustomMetrics.SCANNER_CREATE_TIME_NS,
                LanceCustomMetrics.BATCH_LOAD_TIME_NS));
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    Set<String> actual = new HashSet<>();
    for (CustomMetric m : metrics) {
      actual.add(m.name());
    }
    assertEquals(expected, actual);
  }

  @Test
  void testAllMetricsHaveDescriptions() {
    for (CustomMetric m : LanceCustomMetrics.allMetrics()) {
      assertNotNull(m.description(), "Missing description for " + m.name());
      assertTrue(m.description().length() > 0, "Empty description for " + m.name());
    }
  }

  @Test
  void testMetricDescriptionsAreUnique() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    Set<String> descriptions = new HashSet<>();
    for (CustomMetric m : metrics) {
      assertTrue(
          descriptions.add(m.description()), "Duplicate metric description: " + m.description());
    }
  }

  @Test
  void testCountAggregationReturnsRawLong() {
    LanceCustomMetrics.NumFragmentsScannedMetric metric =
        new LanceCustomMetrics.NumFragmentsScannedMetric();
    String result = metric.aggregateTaskMetrics(new long[] {3, 5, 7});
    assertEquals("15", result);
  }

  @Test
  void testCountAggregationEmpty() {
    LanceCustomMetrics.NumFragmentsScannedMetric metric =
        new LanceCustomMetrics.NumFragmentsScannedMetric();
    String result = metric.aggregateTaskMetrics(new long[] {});
    assertEquals("0", result);
  }

  @Test
  void testTimeAggregationFormatsDuration() {
    LanceCustomMetrics.BatchLoadTimeNsMetric metric =
        new LanceCustomMetrics.BatchLoadTimeNsMetric();
    assertEquals("1.5 s", metric.aggregateTaskMetrics(new long[] {500_000_000L, 1_000_000_000L}));
    assertEquals("250 ms", metric.aggregateTaskMetrics(new long[] {100_000_000L, 150_000_000L}));
  }

  @Test
  void testFormatDurationNsThresholds() {
    assertEquals("0 ns", CustomNsTimeMetric.formatDurationNs(0));
    assertEquals("0 ns", CustomNsTimeMetric.formatDurationNs(-5));
    assertEquals("999 ns", CustomNsTimeMetric.formatDurationNs(999));
    assertEquals("1 us", CustomNsTimeMetric.formatDurationNs(1_000));
    assertEquals("47 us", CustomNsTimeMetric.formatDurationNs(47_500));
    assertEquals("999 us", CustomNsTimeMetric.formatDurationNs(999_999));
    assertEquals("1 ms", CustomNsTimeMetric.formatDurationNs(1_000_000));
    assertEquals("350 ms", CustomNsTimeMetric.formatDurationNs(350_000_000));
    assertEquals("999 ms", CustomNsTimeMetric.formatDurationNs(999_999_999));
    assertEquals("1.0 s", CustomNsTimeMetric.formatDurationNs(1_000_000_000L));
    assertEquals("1.2 s", CustomNsTimeMetric.formatDurationNs(1_234_567_890L));
    assertEquals("47.3 s", CustomNsTimeMetric.formatDurationNs(47_382_910_283L));
  }

  @Test
  void testNoArgConstructors() throws Exception {
    // Spark instantiates metric classes via reflection — verify no-arg constructors work
    Class<?>[] metricClasses =
        new Class<?>[] {
          LanceCustomMetrics.NumFragmentsScannedMetric.class,
          LanceCustomMetrics.NumBatchesLoadedMetric.class,
          LanceCustomMetrics.NumRowsScannedMetric.class,
          LanceCustomMetrics.DatasetOpenTimeNsMetric.class,
          LanceCustomMetrics.ScannerCreateTimeNsMetric.class,
          LanceCustomMetrics.BatchLoadTimeNsMetric.class,
        };
    for (Class<?> clazz : metricClasses) {
      Object instance = clazz.getDeclaredConstructor().newInstance();
      assertNotNull(instance, "Failed to instantiate " + clazz.getSimpleName());
    }
  }

  @Test
  void testTrackerInitialValues() {
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    CustomTaskMetric[] values = tracker.currentMetricsValues();
    assertEquals(6, values.length);
    for (CustomTaskMetric m : values) {
      assertEquals(0L, m.value(), "Initial value should be 0 for " + m.name());
    }
  }

  @Test
  void testTrackerAccumulation() {
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    tracker.addNumFragmentsScanned(1);
    tracker.addNumFragmentsScanned(1);
    tracker.addNumBatchesLoaded(5);
    tracker.addNumRowsScanned(1024);
    tracker.addDatasetOpenTimeNs(100_000);
    tracker.addScannerCreateTimeNs(50_000);
    tracker.addBatchLoadTimeNs(200_000);

    assertEquals(2, tracker.getNumFragmentsScanned());
    assertEquals(5, tracker.getNumBatchesLoaded());
    assertEquals(1024, tracker.getNumRowsScanned());
    assertEquals(100_000, tracker.getDatasetOpenTimeNs());
    assertEquals(50_000, tracker.getScannerCreateTimeNs());
    assertEquals(200_000, tracker.getBatchLoadTimeNs());
  }

  @Test
  void testTrackerSnapshotReflectsLiveFields() {
    // The tracker caches a single CustomTaskMetric[] at construction; each value() call
    // must read the current field state, not a captured snapshot.
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    CustomTaskMetric[] before = tracker.currentMetricsValues();
    tracker.addNumFragmentsScanned(7);
    CustomTaskMetric[] after = tracker.currentMetricsValues();
    assertSame(before, after, "currentMetricsValues() should return the same cached array");
    long observed = -1;
    for (CustomTaskMetric m : after) {
      if (m.name().equals(LanceCustomMetrics.NUM_FRAGMENTS_SCANNED)) {
        observed = m.value();
      }
    }
    assertEquals(7L, observed, "value() must read live field, not capture-time value");
  }

  @Test
  void testTrackerMetricNamesMatchDefinitions() {
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    CustomTaskMetric[] taskMetrics = tracker.currentMetricsValues();
    CustomMetric[] definitions = LanceCustomMetrics.allMetrics();

    Set<String> taskNames = new HashSet<>();
    for (CustomTaskMetric m : taskMetrics) {
      taskNames.add(m.name());
    }
    Set<String> defNames = new HashSet<>();
    for (CustomMetric m : definitions) {
      defNames.add(m.name());
    }
    assertEquals(
        defNames,
        taskNames,
        "Task metric names must match definition names for Spark to aggregate");
  }
}
