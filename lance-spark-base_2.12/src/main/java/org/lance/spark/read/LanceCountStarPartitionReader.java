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
import org.lance.spark.vectorized.LanceArrowColumnVector;

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
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
    // This reader is only used when there are filters (metadata-based count uses LocalScan)
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    long totalCount = 0;

    try (Dataset dataset = openDataset(readOptions)) {
      List<Integer> fragmentIds = inputPartition.getLanceSplit().getFragments();
      if (fragmentIds.isEmpty()) {
        return 0;
      }

      ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptionsBuilder.filter(inputPartition.getWhereCondition().get());
      }
      scanOptionsBuilder.withRowId(true);
      scanOptionsBuilder.columns(Lists.newArrayList());
      scanOptionsBuilder.fragmentIds(fragmentIds);
      try (LanceScanner scanner = dataset.newScan(scanOptionsBuilder.build())) {
        try (ArrowReader reader = scanner.scanBatches()) {
          while (reader.loadNextBatch()) {
            totalCount += reader.getVectorSchemaRoot().getRowCount();
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to scan fragment " + fragmentIds, e);
      }
    }

    return totalCount;
  }

  private Dataset openDataset(LanceSparkReadOptions readOptions) {
    if (readOptions.hasNamespace()) {
      return Dataset.open()
          .allocator(allocator)
          .namespace(readOptions.getNamespace())
          .tableId(readOptions.getTableId())
          .readOptions(readOptions.toReadOptions())
          .build();
    } else {
      return Dataset.open()
          .allocator(allocator)
          .uri(readOptions.getDatasetUri())
          .readOptions(readOptions.toReadOptions())
          .build();
    }
  }

  private ColumnarBatch createCountResultBatch(long count, StructType resultSchema) {
    VectorSchemaRoot root =
        VectorSchemaRoot.create(
            LanceArrowUtils.toArrowSchema(resultSchema, "UTC", false, false), allocator);

    root.allocateNew();
    BigIntVector countVector = (BigIntVector) root.getVector("count");
    countVector.setSafe(0, count);
    root.setRowCount(1);

    LanceArrowColumnVector[] columns =
        root.getFieldVectors().stream()
            .map(LanceArrowColumnVector::new)
            .toArray(LanceArrowColumnVector[]::new);

    return new ColumnarBatch(columns, 1);
  }

  @Override
  public ColumnarBatch get() {
    long rowCount = computeCount();
    StructType countSchema =
        new StructType().add("count", org.apache.spark.sql.types.DataTypes.LongType);
    return createCountResultBatch(rowCount, countSchema);
  }

  @Override
  public void close() throws IOException {
    if (currentBatch != null) {
      currentBatch.close();
    }
  }
}
