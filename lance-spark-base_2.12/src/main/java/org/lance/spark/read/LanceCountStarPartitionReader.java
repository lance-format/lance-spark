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

import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.metric.LanceReadMetricsTracker;
import org.lance.spark.utils.Utils;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

/**
 * Partition reader for pushed down aggregates. This reader computes the aggregate result directly
 * on the Lance dataset.
 */
public class LanceCountStarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceInputPartition inputPartition;
  private final BufferAllocator allocator;
  private boolean finished = false;
  private ColumnarBatch currentBatch;
  private final LanceReadMetricsTracker metricsTracker = new LanceReadMetricsTracker();

  public LanceCountStarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.allocator = LanceRuntime.allocator();
  }

  @Override
  public boolean next() throws IOException {
    if (!finished) {
      finished = true;
      return true;
    } else {
      return false;
    }
  }

  private long computeCount() {
    // Used whenever the metadata-based count is unavailable: a pushed filter, an active full-text
    // query, or an unreadable manifest summary. A full-text query alone is enough, so
    // inputPartition.getWhereCondition() may be empty here.
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    long totalCount = 0;

    // Keep the count scan on the same executor credential-refresh path as ordinary fragment scans.
    // When disabled, leave the namespace unset so executors open directly with the driver-vended
    // storage options (important for Kerberized HMS catalogs).
    if (inputPartition.getNamespaceImpl() != null && readOptions.isExecutorCredentialRefresh()) {
      if (LanceRuntime.useNamespaceOnWorkers(inputPartition.getNamespaceImpl())) {
        readOptions.setNamespace(
            LanceRuntime.getOrCreateNamespace(
                inputPartition.getNamespaceImpl(), inputPartition.getNamespaceProperties()));
      } else {
        readOptions.setNamespace(null);
      }
    }

    long dsOpenStart = System.nanoTime();
    try (Dataset dataset =
        Utils.openDatasetBuilder(readOptions)
            .initialStorageOptions(inputPartition.getInitialStorageOptions())
            .build()) {
      metricsTracker.addDatasetOpenTimeNs(System.nanoTime() - dsOpenStart);

      List<Integer> fragmentIds = inputPartition.getLanceSplit().getFragments();
      if (fragmentIds.isEmpty()) {
        return 0;
      }
      metricsTracker.addNumFragmentsScanned(fragmentIds.size());

      ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
      scanOptionsBuilder.useScalarIndex(readOptions.isUseScalarIndex());
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptionsBuilder.filter(inputPartition.getWhereCondition().get());
      }
      // A full-text query restricts rows just like a filter does, so it must be applied here or the
      // count would cover rows the query excludes. The empty column list below makes Lance treat
      // this as an explicit projection, which would otherwise auto-append `_score` (and log a
      // deprecation warning) for every task; the count only needs row counts, so opt out. Do NOT
      // copy that opt-out to the row-scan path, which relies on the autoprojection to deliver the
      // `_score` metadata column.
      if (readOptions.getFullTextQuery() != null) {
        scanOptionsBuilder.fullTextQuery(readOptions.getFullTextQuery());
        scanOptionsBuilder.disableScoringAutoprojection(true);
      }
      scanOptionsBuilder.withRowId(true);
      scanOptionsBuilder.columns(Lists.newArrayList());
      scanOptionsBuilder.fragmentIds(fragmentIds);

      // Collect scan stats
      scanOptionsBuilder.collectStats(true);

      long scanCreateStart = System.nanoTime();
      try (LanceScanner scanner = dataset.newScan(scanOptionsBuilder.build())) {
        metricsTracker.addScannerCreateTimeNs(System.nanoTime() - scanCreateStart);
        try (ArrowReader reader = scanner.scanBatches()) {
          while (true) {
            long batchStart = System.nanoTime();
            boolean hasNext = reader.loadNextBatch();
            long batchTimeNs = System.nanoTime() - batchStart;
            if (!hasNext) {
              break;
            }
            metricsTracker.addBatchLoadTimeNs(batchTimeNs);
            long rowCount = reader.getVectorSchemaRoot().getRowCount();
            totalCount += rowCount;
            metricsTracker.addNumBatchesLoaded(1);
            metricsTracker.addNumRowsScanned(rowCount);
          }
        }
        metricsTracker.addScanStats(scanner.getStats());
      } catch (Exception e) {
        throw new RuntimeException("Failed to scan fragment " + fragmentIds, e);
      }
    }

    return totalCount;
  }

  private ColumnarBatch createCountResultBatch(long count, StructType resultSchema) {
    VectorSchemaRoot root =
        VectorSchemaRoot.create(
            LanceArrowUtils.toArrowSchema(resultSchema, "UTC", false), allocator);
    try {
      root.allocateNew();
      BigIntVector countVector = (BigIntVector) root.getVector("count");
      countVector.setSafe(0, count);
      root.setRowCount(1);

      LanceArrowColumnVector[] columns =
          root.getFieldVectors().stream()
              .map(LanceArrowColumnVector::new)
              .toArray(LanceArrowColumnVector[]::new);

      return new ColumnarBatch(columns, 1);
    } catch (Exception e) {
      root.close();
      throw e;
    }
  }

  @Override
  public ColumnarBatch get() {
    if (currentBatch == null) {
      long rowCount = computeCount();
      StructType countSchema =
          new StructType().add("count", org.apache.spark.sql.types.DataTypes.LongType);
      currentBatch = createCountResultBatch(rowCount, countSchema);
    }
    return currentBatch;
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return metricsTracker.currentMetricsValues();
  }

  @Override
  public void close() throws IOException {
    if (currentBatch != null) {
      currentBatch.close();
    }
  }
}
