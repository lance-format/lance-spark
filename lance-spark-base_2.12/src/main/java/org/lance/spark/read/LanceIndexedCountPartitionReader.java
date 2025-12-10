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
import org.lance.spark.SparkOptions;
import org.lance.spark.internal.LanceDatasetAdapter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.LanceArrowColumnVector;

import java.io.IOException;
import java.util.Optional;

/**
 * Partition reader that counts rows using direct index queries. All indexed fragments are processed
 * in a single reader using the scalar index.
 */
public class LanceIndexedCountPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceSplitCountScan.LanceIndexedCountPartition partition;
  private final BufferAllocator allocator;
  private boolean finished = false;
  private ColumnarBatch currentBatch;

  public LanceIndexedCountPartitionReader(
      LanceSplitCountScan.LanceIndexedCountPartition partition) {
    this.partition = partition;
    this.allocator = LanceDatasetAdapter.allocator;
  }

  @Override
  public boolean next() throws IOException {
    if (!finished) {
      finished = true;
      return true;
    }
    return false;
  }

  private long computeIndexedCount() {
    String uri = partition.getConfig().getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(partition.getConfig());

    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      String filter = partition.getWhereCondition().orElse("");
      Optional<java.util.List<Integer>> fragmentIds = Optional.of(partition.getFragmentIds());

      return LanceDatasetAdapter.countIndexedRows(
          dataset, partition.getIndexName(), filter, fragmentIds);
    }
  }

  private ColumnarBatch createCountResultBatch(long count) {
    StructType countSchema = new StructType().add("count", DataTypes.LongType);
    VectorSchemaRoot root =
        VectorSchemaRoot.create(
            LanceArrowUtils.toArrowSchema(countSchema, "UTC", false, false), allocator);

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
    long count = computeIndexedCount();
    currentBatch = createCountResultBatch(count);
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    if (currentBatch != null) {
      currentBatch.close();
    }
  }
}
