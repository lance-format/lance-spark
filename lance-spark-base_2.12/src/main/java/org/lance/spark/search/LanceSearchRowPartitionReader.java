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
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.Iterator;

public class LanceSearchRowPartitionReader implements PartitionReader<InternalRow> {
  private final LanceSearchColumnarPartitionReader reader;
  private Iterator<InternalRow> currentRows;
  private InternalRow currentRecord;

  public LanceSearchRowPartitionReader(LanceSearchColumnarPartitionReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean next() throws IOException {
    if (currentRows != null && currentRows.hasNext()) {
      currentRecord = currentRows.next();
      return true;
    }
    if (reader.next()) {
      ColumnarBatch currentBatch = reader.get();
      currentRows = currentBatch.rowIterator();
      if (currentRows != null && currentRows.hasNext()) {
        currentRecord = currentRows.next();
        return true;
      }
    }
    return false;
  }

  @Override
  public InternalRow get() {
    return currentRecord;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return reader.currentMetricsValues();
  }
}
