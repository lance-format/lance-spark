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
import org.apache.spark.sql.connector.metric.CustomSumMetric;

/**
 * Custom metrics for the Lance read path, displayed on the Spark UI Scan node.
 *
 * <p>Naming conventions:
 *
 * <ul>
 *   <li>Counters use {@code num*} prefix (e.g. {@code numFragmentsScanned}).
 *   <li>Durations use {@code *TimeNs} suffix and extend {@link CustomNsTimeMetric} so the UI shows
 *       formatted values like "1.2 s" instead of raw nanoseconds.
 *   <li>Sizes use {@code *Bytes} suffix (reserved for future use).
 * </ul>
 *
 * <p>Future metrics gated on upstream {@code lance-jni} surface (not implementable in pure Java
 * today): {@code numFragmentsPruned}, {@code bytesRead}, {@code numIndexLookups}, {@code
 * ioWaitTimeNs}, {@code decodeTimeNs}.
 */
public final class LanceCustomMetrics {
  public static final String NUM_FRAGMENTS_SCANNED = "numFragmentsScanned";
  public static final String NUM_BATCHES_LOADED = "numBatchesLoaded";
  public static final String NUM_ROWS_SCANNED = "numRowsScanned";
  public static final String NUM_IOPS = "numIops";
  public static final String NUM_REQUESTS = "numRequests";
  public static final String NUM_BYTES_READ = "numBytesRead";
  public static final String NUM_INDICES_LOADED = "numIndicesLoaded";
  public static final String NUM_PARTS_LOADED = "numPartsLoaded";
  public static final String NUM_INDEX_COMPARISONS = "numIndexComparisons";

  public static final String DATASET_OPEN_TIME_NS = "datasetOpenTimeNs";
  public static final String SCANNER_CREATE_TIME_NS = "scannerCreateTimeNs";
  public static final String BATCH_LOAD_TIME_NS = "batchLoadTimeNs";

  private LanceCustomMetrics() {}

  // Each inner class MUST have a public no-arg constructor (Spark instantiates via reflection).

  public static class NumFragmentsScannedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_FRAGMENTS_SCANNED;
    }

    @Override
    public String description() {
      return "number of Lance fragments scanned";
    }
  }

  public static class NumBatchesLoadedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_BATCHES_LOADED;
    }

    @Override
    public String description() {
      return "number of Arrow batches loaded";
    }
  }

  public static class NumRowsScannedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_ROWS_SCANNED;
    }

    @Override
    public String description() {
      return "number of rows read from storage (before filter evaluation)";
    }
  }

  public static class NumIopsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_IOPS;
    }

    @Override
    public String description() {
      return "number of I/O operations performed";
    }
  }

  public static class NumRequestsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_REQUESTS;
    }

    @Override
    public String description() {
      return "number of requests made to the storage layer";
    }
  }

  public static class NumBytesReadMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_BYTES_READ;
    }

    @Override
    public String description() {
      return "number of bytes read";
    }
  }

  public static class NumIndicesLoadedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_INDICES_LOADED;
    }

    @Override
    public String description() {
      return "number of top-level indices loaded";
    }
  }

  public static class NumPartsLoadedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_PARTS_LOADED;
    }

    @Override
    public String description() {
      return "number of index partitions loaded";
    }
  }

  public static class NumIndexComparisonsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_INDEX_COMPARISONS;
    }

    @Override
    public String description() {
      return "number of index comparisons";
    }
  }

  public static class DatasetOpenTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return DATASET_OPEN_TIME_NS;
    }

    @Override
    public String description() {
      return "time to open Lance dataset";
    }
  }

  public static class ScannerCreateTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return SCANNER_CREATE_TIME_NS;
    }

    @Override
    public String description() {
      return "time to create fragment scanner";
    }
  }

  public static class BatchLoadTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return BATCH_LOAD_TIME_NS;
    }

    @Override
    public String description() {
      return "time to load Arrow batch (JNI + IPC deserialization)";
    }
  }

  private static final CustomMetric[] ALL_METRICS = {
    new NumFragmentsScannedMetric(),
    new NumBatchesLoadedMetric(),
    new NumRowsScannedMetric(),
    new NumIopsMetric(),
    new NumRequestsMetric(),
    new NumBytesReadMetric(),
    new NumIndicesLoadedMetric(),
    new NumPartsLoadedMetric(),
    new NumIndexComparisonsMetric(),
    new DatasetOpenTimeNsMetric(),
    new ScannerCreateTimeNsMetric(),
    new BatchLoadTimeNsMetric(),
  };

  /** Returns all supported custom metrics, used by LanceScan.supportedCustomMetrics(). */
  public static CustomMetric[] allMetrics() {
    return ALL_METRICS.clone();
  }
}
