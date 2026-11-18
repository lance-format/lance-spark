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
package org.lance.spark.read;

import org.lance.spark.internal.ExecutorNamespace;
import org.lance.spark.internal.LanceFragmentColumnarBatchScanner;
import org.lance.spark.read.metric.LanceReadMetricsTracker;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class LanceColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceInputPartition inputPartition;
  private int fragmentIndex;
  private LanceFragmentColumnarBatchScanner fragmentReader;
  private ColumnarBatch currentBatch;
  private final LanceReadMetricsTracker metricsTracker = new LanceReadMetricsTracker();
  private boolean currentScanStatsAdded = false;
  private ExecutorNamespace executorNamespace;

  public LanceColumnarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.fragmentIndex = 0;
  }

  @Override
  public boolean next() throws IOException {
    try {
      return nextInternal();
    } catch (Throwable t) {
      throw asIOException(closeResources(t));
    }
  }

  private boolean nextInternal() throws IOException {
    if (loadNextBatchFromCurrentReader()) {
      return true;
    }
    while (fragmentIndex < inputPartition.getLanceSplit().getFragments().size()) {
      // Null-first so if create(...) below throws, the subsequent close() sees fragmentReader ==
      // null and short-circuits, rather than re-closing the already-closed previous scanner and
      // raising `ArrowArrayStream is already closed`.
      if (fragmentReader != null) {
        LanceFragmentColumnarBatchScanner toClose = fragmentReader;
        fragmentReader = null;
        toClose.close();
      }
      initializeExecutorNamespace();
      fragmentReader =
          LanceFragmentColumnarBatchScanner.create(
              inputPartition.getLanceSplit().getFragments().get(fragmentIndex), inputPartition);
      fragmentIndex++;

      currentScanStatsAdded = false;
      metricsTracker.addNumFragmentsScanned(1);
      metricsTracker.addDatasetOpenTimeNs(fragmentReader.getDatasetOpenTimeNs());
      metricsTracker.addScannerCreateTimeNs(fragmentReader.getScannerCreateTimeNs());

      if (loadNextBatchFromCurrentReader()) {
        return true;
      }
    }
    return false;
  }

  private void initializeExecutorNamespace() {
    if (executorNamespace == null) {
      executorNamespace = ExecutorNamespace.acquire(inputPartition);
    }
  }

  private boolean loadNextBatchFromCurrentReader() throws IOException {
    if (fragmentReader == null) {
      return false;
    }
    if (fragmentReader.loadNextBatch()) {
      currentBatch = fragmentReader.getCurrentBatch();
      metricsTracker.addNumBatchesLoaded(1);
      metricsTracker.addNumRowsScanned(currentBatch.numRows());
      metricsTracker.addBatchLoadTimeNs(fragmentReader.getLastBatchLoadTimeNs());
      return true;
    }

    // Lance scan stats are populated when the scan stream is fully consumed
    if (!currentScanStatsAdded) {
      metricsTracker.addScanStats(fragmentReader.getScanStats());
      currentScanStatsAdded = true;
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return metricsTracker.currentMetricsValues();
  }

  @Override
  public void close() throws IOException {
    Throwable failure = null;
    if (fragmentReader != null && !currentScanStatsAdded) {
      try {
        metricsTracker.addScanStats(fragmentReader.getScanStats());
        currentScanStatsAdded = true;
      } catch (Throwable t) {
        failure = t;
      }
    }
    failure = closeResources(failure);
    if (failure != null) {
      throw asIOException(failure);
    }
  }

  private Throwable closeResources(Throwable primary) {
    // Null-first so close() is idempotent. A repeat call must not raise
    // "ArrowArrayStream is already closed" from a second release().
    LanceFragmentColumnarBatchScanner scannerToClose = fragmentReader;
    fragmentReader = null;
    ExecutorNamespace namespaceToClose = executorNamespace;
    executorNamespace = null;

    primary = closeResource(scannerToClose, primary);
    primary = closeResource(namespaceToClose, primary);
    return primary;
  }

  private static Throwable closeResource(AutoCloseable resource, Throwable primary) {
    if (resource == null) {
      return primary;
    }
    try {
      resource.close();
    } catch (Throwable closeError) {
      if (primary == null) {
        return closeError;
      }
      primary.addSuppressed(closeError);
    }
    return primary;
  }

  private static IOException asIOException(Throwable failure) {
    if (failure instanceof IOException) {
      return (IOException) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    return new IOException(failure);
  }
}
