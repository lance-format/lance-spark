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

import org.lance.spark.LanceConfig;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A scan that combines indexed and unindexed fragments for count operations. Indexed fragments are
 * processed in a single partition using direct index queries, while unindexed fragments are
 * distributed normally (one partition per fragment).
 */
public class LanceSplitCountScan implements Batch, Scan, Serializable {
  private static final long serialVersionUID = 3847293847293847294L;

  private final List<Integer> indexedFragmentIds;
  private final List<Integer> unindexedFragmentIds;
  private final String indexName;
  private final Optional<String> whereCondition;
  private final LanceConfig config;

  public LanceSplitCountScan(
      List<Integer> indexedFragmentIds,
      List<Integer> unindexedFragmentIds,
      String indexName,
      Optional<String> whereCondition,
      LanceConfig config) {
    this.indexedFragmentIds = indexedFragmentIds;
    this.unindexedFragmentIds = unindexedFragmentIds;
    this.indexName = indexName;
    this.whereCondition = whereCondition;
    this.config = config;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<InputPartition> partitions = new ArrayList<>();

    // First partition: all indexed fragments (single worker with index query)
    if (!indexedFragmentIds.isEmpty()) {
      partitions.add(
          new LanceIndexedCountPartition(indexedFragmentIds, indexName, whereCondition, config));
    }

    // Remaining partitions: unindexed fragments (distributed normally)
    for (Integer fragmentId : unindexedFragmentIds) {
      List<Integer> singleFragment = new ArrayList<>();
      singleFragment.add(fragmentId);
      partitions.add(new LanceUnindexedCountPartition(singleFragment, whereCondition, config));
    }

    return partitions.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SplitCountReaderFactory();
  }

  @Override
  public StructType readSchema() {
    return new StructType().add("count", DataTypes.LongType);
  }

  /** Partition for indexed fragments that will use direct index query. */
  public static class LanceIndexedCountPartition implements InputPartition, Serializable {
    private static final long serialVersionUID = 4723894723984723985L;

    private final List<Integer> fragmentIds;
    private final String indexName;
    private final Optional<String> whereCondition;
    private final LanceConfig config;

    public LanceIndexedCountPartition(
        List<Integer> fragmentIds,
        String indexName,
        Optional<String> whereCondition,
        LanceConfig config) {
      this.fragmentIds = fragmentIds;
      this.indexName = indexName;
      this.whereCondition = whereCondition;
      this.config = config;
    }

    public List<Integer> getFragmentIds() {
      return fragmentIds;
    }

    public String getIndexName() {
      return indexName;
    }

    public Optional<String> getWhereCondition() {
      return whereCondition;
    }

    public LanceConfig getConfig() {
      return config;
    }
  }

  /** Partition for unindexed fragments that will use normal scan-based count. */
  public static class LanceUnindexedCountPartition implements InputPartition, Serializable {
    private static final long serialVersionUID = 4723894723984723986L;

    private final List<Integer> fragmentIds;
    private final Optional<String> whereCondition;
    private final LanceConfig config;

    public LanceUnindexedCountPartition(
        List<Integer> fragmentIds, Optional<String> whereCondition, LanceConfig config) {
      this.fragmentIds = fragmentIds;
      this.whereCondition = whereCondition;
      this.config = config;
    }

    public List<Integer> getFragmentIds() {
      return fragmentIds;
    }

    public Optional<String> getWhereCondition() {
      return whereCondition;
    }

    public LanceConfig getConfig() {
      return config;
    }
  }

  private static class SplitCountReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      throw new UnsupportedOperationException("Row-based reads not supported for split count scan");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      if (partition instanceof LanceIndexedCountPartition) {
        return new LanceIndexedCountPartitionReader((LanceIndexedCountPartition) partition);
      } else if (partition instanceof LanceUnindexedCountPartition) {
        return new LanceUnindexedCountPartitionReader((LanceUnindexedCountPartition) partition);
      } else {
        throw new IllegalArgumentException(
            "Unknown partition type: " + partition.getClass().getName());
      }
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }
  }
}
