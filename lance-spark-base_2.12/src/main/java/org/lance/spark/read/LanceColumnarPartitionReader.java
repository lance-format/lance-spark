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
import org.lance.ReadOptions;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.namespace.LanceNamespaceStorageOptionsProvider;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.LanceFragmentColumnarBatchScanner;
import org.lance.spark.internal.LanceFragmentScanner;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LanceColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceInputPartition inputPartition;
  private final boolean useFilteredReadPlan;

  // Fragment-based path fields
  private int fragmentIndex;
  private LanceFragmentColumnarBatchScanner fragmentReader;

  // Filtered read plan path fields
  private Dataset dataset;
  private LanceScanner scanner;
  private ArrowReader planArrowReader;
  private boolean initialized;

  private ColumnarBatch currentBatch;

  public LanceColumnarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.useFilteredReadPlan = inputPartition.getLanceSplit().getFilteredReadPlan().isPresent();
    this.fragmentIndex = 0;
    this.initialized = false;
  }

  @Override
  public boolean next() throws IOException {
    if (useFilteredReadPlan) {
      return nextFilteredReadPlan();
    } else {
      return nextFragmentBased();
    }
  }

  private boolean nextFilteredReadPlan() throws IOException {
    if (!initialized) {
      initializeFilteredReadPlan();
      initialized = true;
    }
    if (planArrowReader.loadNextBatch()) {
      VectorSchemaRoot root = planArrowReader.getVectorSchemaRoot();
      List<ColumnVector> fieldVectors =
          root.getFieldVectors().stream()
              .map(LanceArrowColumnVector::new)
              .collect(Collectors.toList());

      LanceFragmentColumnarBatchScanner.addBlobVirtualColumns(fieldVectors, root, inputPartition);

      currentBatch =
          new ColumnarBatch(fieldVectors.toArray(new ColumnVector[] {}), root.getRowCount());
      return true;
    }
    return false;
  }

  private void initializeFilteredReadPlan() {
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();

      Map<String, String> merged =
          LanceRuntime.mergeStorageOptions(
              readOptions.getStorageOptions(), inputPartition.getInitialStorageOptions());
      LanceNamespaceStorageOptionsProvider provider =
          LanceRuntime.getOrCreateStorageOptionsProvider(
              inputPartition.getNamespaceImpl(),
              inputPartition.getNamespaceProperties(),
              readOptions.getTableId());

      ReadOptions.Builder readOptionsBuilder = new ReadOptions.Builder().setStorageOptions(merged);
      if (provider != null) {
        readOptionsBuilder.setStorageOptionsProvider(provider);
      }

      dataset =
          Dataset.open()
              .allocator(LanceRuntime.allocator())
              .uri(readOptions.getDatasetUri())
              .readOptions(readOptionsBuilder.build())
              .build();

      ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
      scanOptionsBuilder.columns(LanceFragmentScanner.getColumnNames(inputPartition.getSchema()));
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptionsBuilder.filter(inputPartition.getWhereCondition().get());
      }
      scanOptionsBuilder.batchSize(readOptions.getBatchSize());
      scanOptionsBuilder.withRowId(LanceFragmentScanner.getWithRowId(inputPartition.getSchema()));
      scanOptionsBuilder.withRowAddress(
          LanceFragmentScanner.getWithRowAddress(inputPartition.getSchema()));
      if (readOptions.getNearest() != null) {
        scanOptionsBuilder.nearest(readOptions.getNearest());
      }
      if (inputPartition.getLimit().isPresent()) {
        scanOptionsBuilder.limit(inputPartition.getLimit().get());
      }
      if (inputPartition.getOffset().isPresent()) {
        scanOptionsBuilder.offset(inputPartition.getOffset().get());
      }
      if (inputPartition.getTopNSortOrders().isPresent()) {
        scanOptionsBuilder.setColumnOrderings(inputPartition.getTopNSortOrders().get());
      }

      scanner = dataset.newScan(scanOptionsBuilder.build());
      planArrowReader =
          scanner.executeFilteredReadPlan(
              inputPartition.getLanceSplit().getFilteredReadPlan().get());
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize filtered read plan", e);
    }
  }

  private boolean nextFragmentBased() throws IOException {
    if (loadNextBatchFromCurrentReader()) {
      return true;
    }
    while (fragmentIndex < inputPartition.getLanceSplit().getFragments().size()) {
      if (fragmentReader != null) {
        fragmentReader.close();
      }
      fragmentReader =
          LanceFragmentColumnarBatchScanner.create(
              inputPartition.getLanceSplit().getFragments().get(fragmentIndex), inputPartition);
      fragmentIndex++;
      if (loadNextBatchFromCurrentReader()) {
        return true;
      }
    }
    return false;
  }

  private boolean loadNextBatchFromCurrentReader() throws IOException {
    if (fragmentReader != null && fragmentReader.loadNextBatch()) {
      currentBatch = fragmentReader.getCurrentBatch();
      return true;
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    if (fragmentReader != null) {
      try {
        fragmentReader.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    if (planArrowReader != null) {
      planArrowReader.close();
    }
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    if (dataset != null) {
      dataset.close();
    }
  }
}
