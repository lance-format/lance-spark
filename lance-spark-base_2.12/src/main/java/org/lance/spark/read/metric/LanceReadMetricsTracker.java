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

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Accumulates read-path metrics on the executor side. Thread-confined (one instance per
 * PartitionReader, single-threaded access). Returns snapshot values via {@link
 * #currentMetricsValues()} — Spark calls this once per {@code next()} invocation on the
 * PartitionReader.
 *
 * <p>The {@link CustomTaskMetric} array and per-metric instances are allocated once per tracker
 * (not per call): each {@code value()} invocation reads the current long field directly, so {@code
 * currentMetricsValues()} is allocation-free on the hot path.
 */
public class LanceReadMetricsTracker {
  private long numFragmentsScanned;
  private long numBatchesLoaded;
  private long numRowsScanned;
  private long datasetOpenTimeNs;
  private long scannerCreateTimeNs;
  private long batchLoadTimeNs;

  private final CustomTaskMetric[] taskMetrics =
      new CustomTaskMetric[] {
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.NUM_FRAGMENTS_SCANNED;
          }

          @Override
          public long value() {
            return numFragmentsScanned;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.NUM_BATCHES_LOADED;
          }

          @Override
          public long value() {
            return numBatchesLoaded;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.NUM_ROWS_SCANNED;
          }

          @Override
          public long value() {
            return numRowsScanned;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.DATASET_OPEN_TIME_NS;
          }

          @Override
          public long value() {
            return datasetOpenTimeNs;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.SCANNER_CREATE_TIME_NS;
          }

          @Override
          public long value() {
            return scannerCreateTimeNs;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.BATCH_LOAD_TIME_NS;
          }

          @Override
          public long value() {
            return batchLoadTimeNs;
          }
        },
      };

  public void addNumFragmentsScanned(long n) {
    numFragmentsScanned += n;
  }

  public void addNumBatchesLoaded(long n) {
    numBatchesLoaded += n;
  }

  public void addNumRowsScanned(long n) {
    numRowsScanned += n;
  }

  public void addDatasetOpenTimeNs(long ns) {
    datasetOpenTimeNs += ns;
  }

  public void addScannerCreateTimeNs(long ns) {
    scannerCreateTimeNs += ns;
  }

  public void addBatchLoadTimeNs(long ns) {
    batchLoadTimeNs += ns;
  }

  /** Returns current snapshot of all metrics. Called by PartitionReader.currentMetricsValues(). */
  public CustomTaskMetric[] currentMetricsValues() {
    return taskMetrics;
  }

  // Accessors for testing
  public long getNumFragmentsScanned() {
    return numFragmentsScanned;
  }

  public long getNumBatchesLoaded() {
    return numBatchesLoaded;
  }

  public long getNumRowsScanned() {
    return numRowsScanned;
  }

  public long getDatasetOpenTimeNs() {
    return datasetOpenTimeNs;
  }

  public long getScannerCreateTimeNs() {
    return scannerCreateTimeNs;
  }

  public long getBatchLoadTimeNs() {
    return batchLoadTimeNs;
  }
}
