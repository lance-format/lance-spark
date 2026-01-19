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

import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class LanceReaderFactory implements PartitionReaderFactory {
  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    Preconditions.checkArgument(
        partition instanceof LanceInputPartition,
        "Unknown InputPartition type. Expecting LanceInputPartition");
    return LanceRowPartitionReader.create((LanceInputPartition) partition);
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    Preconditions.checkArgument(
        partition instanceof LanceInputPartition,
        "Unknown InputPartition type. Expecting LanceInputPartition");

    LanceInputPartition lancePartition = (LanceInputPartition) partition;
    if (lancePartition.getPushedAggregation().isPresent()) {
      AggregateFunc[] aggFunc = lancePartition.getPushedAggregation().get().aggregateExpressions();
      if (aggFunc.length == 1 && aggFunc[0] instanceof CountStar) {
        return new LanceCountStarPartitionReader(lancePartition);
      }
    }

    return new LanceColumnarPartitionReader(lancePartition);
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }
}
