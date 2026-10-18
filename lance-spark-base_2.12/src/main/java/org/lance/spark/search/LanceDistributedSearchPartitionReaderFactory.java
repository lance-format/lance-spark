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
package org.lance.spark.search;

import org.lance.spark.LanceRuntime;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class LanceDistributedSearchPartitionReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = -613298471029384756L;

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return new LanceSearchRowPartitionReader(createColumnarReader(partition));
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    LanceRuntime.enableOpenTelemetry();
    return new LanceDistributedSearchColumnarPartitionReader(asDistributedPartition(partition));
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }

  private LanceDistributedSearchInputPartition asDistributedPartition(InputPartition partition) {
    if (!(partition instanceof LanceDistributedSearchInputPartition)) {
      throw new IllegalArgumentException(
          "Unknown InputPartition type. Expecting LanceDistributedSearchInputPartition");
    }
    return (LanceDistributedSearchInputPartition) partition;
  }
}
