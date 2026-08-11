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

  public LanceColumnarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.fragmentIndex = 0;
  }

  @Override
  public boolean next() throws IOException {
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
    if (fragmentReader == null) {
      return;
    }
    if (!currentScanStatsAdded) {
      metricsTracker.addScanStats(fragmentReader.getScanStats());
      currentScanStatsAdded = true;
    }
    // Null-first so close() is idempotent (PartitionReader extends Closeable, whose contract
    // requires it): a repeat call short-circuits rather than raising
    // `ArrowArrayStream is already closed` from a second ArrowArrayStream.release().
    LanceFragmentColumnarBatchScanner toClose = fragmentReader;
    fragmentReader = null;
    try {
      toClose.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
