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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class LanceHybridSearchPartitionReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = 6821379812739812739L;

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return new LanceHybridSearchRowPartitionReader(asHybridPartition(partition));
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }

  private LanceHybridSearchInputPartition asHybridPartition(InputPartition partition) {
    if (!(partition instanceof LanceHybridSearchInputPartition)) {
      throw new IllegalArgumentException(
          "Unknown InputPartition type. Expecting LanceHybridSearchInputPartition");
    }
    return (LanceHybridSearchInputPartition) partition;
  }
}
