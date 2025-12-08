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
package org.lance.spark.write;

import org.lance.spark.LanceConfig;
import org.lance.spark.LanceConstant;

import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/** Spark write builder. */
public class AddColumnsBackfillWrite implements Write, RequiresDistributionAndOrdering {
  private final LanceConfig config;
  private final StructType schema;
  private final List<String> newColumns;

  AddColumnsBackfillWrite(StructType schema, LanceConfig config, List<String> newColumns) {
    this.schema = schema;
    this.config = config;
    this.newColumns = newColumns;
  }

  @Override
  public BatchWrite toBatch() {
    return new AddColumnsBackfillBatchWrite(schema, config, newColumns);
  }

  @Override
  public StreamingWrite toStreaming() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    return Distributions.clustered(new NamedReference[] {segmentId});
  }

  @Override
  public SortOrder[] requiredOrdering() {
    NamedReference segmentId = Expressions.column(LanceConstant.ROW_ADDRESS);
    SortValue sortValue =
        new SortValue(segmentId, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    return new SortValue[] {sortValue};
  }

  /** Task commit. */
  public static class AddColumnsWriteBuilder implements WriteBuilder {
    private final LanceConfig config;
    private final StructType schema;
    private final List<String> newColumns;

    public AddColumnsWriteBuilder(StructType schema, LanceConfig config, List<String> newColumns) {
      this.schema = schema;
      this.config = config;
      this.newColumns = newColumns;
    }

    @Override
    public Write build() {
      return new AddColumnsBackfillWrite(schema, config, newColumns);
    }
  }
}
