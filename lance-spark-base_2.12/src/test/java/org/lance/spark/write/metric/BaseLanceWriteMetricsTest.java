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

import org.lance.FragmentMetadata;
import org.lance.fragment.DataFile;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseLanceWriteMetricsTest {

  /** A rename here would silently drop stage-level outputBytes/outputRecords. */
  @Test
  void testReservedSparkMetricNames() {
    assertEquals("bytesWritten", LanceWriteMetrics.BYTES_WRITTEN);
    assertEquals("recordsWritten", LanceWriteMetrics.RECORDS_WRITTEN);
  }

  @Test
  void testAllMetricsHaveUniqueNamesAndDescriptions() {
    CustomMetric[] metrics = LanceWriteMetrics.allMetrics();
    assertEquals(1, metrics.length);
    HashMap<String, CustomMetric> byName = new HashMap<>();
    for (CustomMetric metric : metrics) {
      assertNotNull(metric.description(), "Missing description for " + metric.name());
      assertTrue(metric.description().length() > 0, "Empty description for " + metric.name());
      assertTrue(byName.put(metric.name(), metric) == null, "Duplicate name: " + metric.name());
    }
    assertTrue(byName.containsKey(LanceWriteMetrics.RECORDS_WRITTEN));
  }

  /** Advertising it would always read 0. See {@link LanceWriteMetrics}. */
  @Test
  void testBytesWrittenIsNotAdvertisedAsSqlMetric() {
    for (CustomMetric metric : LanceWriteMetrics.allMetrics()) {
      assertTrue(
          !LanceWriteMetrics.BYTES_WRITTEN.equals(metric.name()),
          "bytesWritten must not be advertised as a SQL custom metric");
    }
  }

  @Test
  void testAllMetricsReturnsDefensiveCopy() {
    assertTrue(LanceWriteMetrics.allMetrics() != LanceWriteMetrics.allMetrics());
  }

  @Test
  void testMetricsAggregateAsSum() {
    for (CustomMetric metric : LanceWriteMetrics.allMetrics()) {
      assertEquals("3", metric.aggregateTaskMetrics(new long[] {1L, 2L}));
    }
  }

  @Test
  void testTrackerAccumulatesRowsAndBytes() {
    LanceWriteMetricsTracker tracker = new LanceWriteMetricsTracker();
    assertEquals(0, tracker.getBytesWritten());
    assertEquals(0, tracker.getRecordsWritten());

    tracker.incrementRecordsWritten();
    tracker.incrementRecordsWritten();
    tracker.addFragments(Collections.singletonList(fragment(100L, 200L)));
    tracker.addFragments(Collections.singletonList(fragment(50L)));

    assertEquals(2, tracker.getRecordsWritten());
    assertEquals(350, tracker.getBytesWritten());
  }

  /** Fragments whose file sizes are unknown must not be counted as zero-sized failures. */
  @Test
  void testTrackerSkipsNullFileSizes() {
    LanceWriteMetricsTracker tracker = new LanceWriteMetricsTracker();
    tracker.addFragments(Collections.singletonList(fragment(null, 42L)));
    assertEquals(42, tracker.getBytesWritten());
  }

  /** Values are absolute task totals, so repeated polls with no work in between are stable. */
  @Test
  void testCurrentMetricsValuesAreAbsoluteAndStable() {
    LanceWriteMetricsTracker tracker = new LanceWriteMetricsTracker();
    tracker.incrementRecordsWritten();
    tracker.addFragments(Collections.singletonList(fragment(64L)));

    CustomTaskMetric[] first = tracker.currentMetricsValues();
    CustomTaskMetric[] second = tracker.currentMetricsValues();
    assertSame(first, second, "metric instances should be allocated once per tracker");

    assertEquals(64, valueOf(first, LanceWriteMetrics.BYTES_WRITTEN));
    assertEquals(1, valueOf(first, LanceWriteMetrics.RECORDS_WRITTEN));
    assertEquals(64, valueOf(second, LanceWriteMetrics.BYTES_WRITTEN));
    assertEquals(1, valueOf(second, LanceWriteMetrics.RECORDS_WRITTEN));
  }

  /** publishOutputMetrics() outside a Spark task must be a no-op rather than an NPE. */
  @Test
  void testPublishOutputMetricsWithoutTaskContext() {
    LanceWriteMetricsTracker tracker = new LanceWriteMetricsTracker();
    tracker.incrementRecordsWritten();
    tracker.publishOutputMetrics();
    assertEquals(1, tracker.getRecordsWritten());
  }

  private static long valueOf(CustomTaskMetric[] metrics, String name) {
    for (CustomTaskMetric metric : metrics) {
      if (name.equals(metric.name())) {
        return metric.value();
      }
    }
    throw new IllegalArgumentException("No such metric: " + name);
  }

  private static FragmentMetadata fragment(Long... fileSizes) {
    List<DataFile> files = new ArrayList<>();
    for (Long size : fileSizes) {
      files.add(
          new DataFile(
              "data.lance",
              new int[] {0},
              new int[] {0},
              /* fileMajorVersion= */ 2,
              /* fileMinorVersion= */ 0,
              size,
              /* baseId= */ null));
    }
    return new FragmentMetadata(1, files, 10L, null, null, null, null);
  }
}
