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

import org.lance.ipc.ColumnOrdering;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;

import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.internal.connector.SupportsMetadata;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public class LanceScan
    implements Batch, Scan, SupportsMetadata, SupportsReportStatistics, Serializable {
  private static final long serialVersionUID = 947284762748623947L;

  private final StructType schema;
  private final LanceSparkReadOptions readOptions;
  private final Optional<String> whereConditions;
  private final Optional<Integer> limit;
  private final Optional<Integer> offset;
  private final Optional<List<ColumnOrdering>> topNSortOrders;
  private final Optional<Aggregation> pushedAggregation;
  private final LanceStatistics statistics;
  private final String scanId = UUID.randomUUID().toString();

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  public LanceScan(
      StructType schema,
      LanceSparkReadOptions readOptions,
      Optional<String> whereConditions,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders,
      Optional<Aggregation> pushedAggregation,
      LanceStatistics statistics,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties) {
    this.schema = schema;
    this.readOptions = readOptions;
    this.whereConditions = whereConditions;
    this.limit = limit;
    this.offset = offset;
    this.topNSortOrders = topNSortOrders;
    this.pushedAggregation = pushedAggregation;
    this.statistics = statistics;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<LanceSplit> splits = LanceSplit.generateLanceSplits(readOptions);
    return IntStream.range(0, splits.size())
        .mapToObj(
            i ->
                new LanceInputPartition(
                    schema,
                    i,
                    splits.get(i),
                    readOptions,
                    whereConditions,
                    limit,
                    offset,
                    topNSortOrders,
                    pushedAggregation,
                    scanId,
                    initialStorageOptions,
                    namespaceImpl,
                    namespaceProperties))
        .toArray(InputPartition[]::new);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceReaderFactory();
  }

  @Override
  public StructType readSchema() {
    if (pushedAggregation.isPresent()) {
      return new StructType().add("count", org.apache.spark.sql.types.DataTypes.LongType);
    }
    return schema;
  }

  @Override
  public Map<String, String> getMetaData() {
    scala.collection.immutable.Map<String, String> empty =
        scala.collection.immutable.Map$.MODULE$.empty();
    scala.collection.immutable.Map<String, String> result = empty;
    result = result.$plus(scala.Tuple2.apply("whereConditions", whereConditions.toString()));
    result = result.$plus(scala.Tuple2.apply("limit", limit.toString()));
    result = result.$plus(scala.Tuple2.apply("offset", offset.toString()));
    result = result.$plus(scala.Tuple2.apply("topNSortOrders", topNSortOrders.toString()));
    result = result.$plus(scala.Tuple2.apply("pushedAggregation", pushedAggregation.toString()));
    return result;
  }

  @Override
  public Statistics estimateStatistics() {
    return statistics;
  }

  private static class LanceReaderFactory implements PartitionReaderFactory {
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
        AggregateFunc[] aggFunc =
            lancePartition.getPushedAggregation().get().aggregateExpressions();
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
}
