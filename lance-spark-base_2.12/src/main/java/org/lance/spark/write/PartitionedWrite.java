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

import org.lance.FragmentMetadata;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.internal.LanceDatasetAdapter;
import org.lance.spark.partition.LancePartitionConfig;
import org.lance.spark.partition.Partition;
import org.lance.spark.partition.PartitionedLanceDataset;

import com.clearspring.analytics.util.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Write implementation for partitioned Lance dataset. */
public class PartitionedWrite implements Write {
  private final LancePartitionConfig config;
  private final StructType sparkSchema;
  private final boolean overwrite;

  PartitionedWrite(LancePartitionConfig config, StructType sparkSchema, boolean overwrite) {
    this.config = config;
    this.sparkSchema = sparkSchema;
    this.overwrite = overwrite;
  }

  @Override
  public BatchWrite toBatch() {
    return new PartitionedBatchWrite(config, sparkSchema, overwrite);
  }

  @Override
  public StreamingWrite toStreaming() {
    throw new UnsupportedOperationException();
  }

  /** Builder for partitioned write. */
  public static class PartitionedWriteBuilder implements SupportsTruncate, WriteBuilder {
    private final LancePartitionConfig config;
    private final StructType sparkSchema;
    private boolean overwrite = false;

    public PartitionedWriteBuilder(LancePartitionConfig config, StructType sparkSchema) {
      this.config = config;
      this.sparkSchema = sparkSchema;
    }

    @Override
    public Write build() {
      return new PartitionedWrite(config, sparkSchema, overwrite);
    }

    @Override
    public WriteBuilder truncate() {
      this.overwrite = true;
      return this;
    }
  }

  /** BatchWrite that routes rows to leaf Lance tables by partition columns. */
  static class PartitionedBatchWrite implements BatchWrite {
    private final LancePartitionConfig config;
    private final StructType sparkSchema;
    private final boolean overwrite;

    PartitionedBatchWrite(LancePartitionConfig config, StructType sparkSchema, boolean overwrite) {
      this.config = config;
      this.sparkSchema = sparkSchema;
      this.overwrite = overwrite;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new PartitionedDataWriterFactory(config);
    }

    @Override
    public boolean useCommitCoordinator() {
      return false;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      Map<LanceSparkReadOptions, List<FragmentMetadata>> byConfig = new HashMap<>();
      if (messages != null) {
        for (WriterCommitMessage msg : messages) {
          if (msg == null) {
            continue;
          }
          TaskCommit tc = (TaskCommit) msg;
          for (Map.Entry<LanceSparkReadOptions, List<FragmentMetadata>> e :
              tc.fragmentsByConfig.entrySet()) {
            LanceSparkReadOptions readOptions = e.getKey();
            List<FragmentMetadata> fragments = e.getValue();
            byConfig.computeIfAbsent(readOptions, k -> new ArrayList<>()).addAll(fragments);
          }
        }
      }

      if (overwrite) {
        PartitionedLanceDataset partitioned;
        try {
          partitioned = new PartitionedLanceDataset(config);
        } catch (NoSuchTableException e) {
          throw new RuntimeException("Failed to open partitioned table for overwrite commit", e);
        }
        List<LanceDataset> leaves = partitioned.root().leaves();

        for (LanceDataset dataset : leaves) {
          LanceSparkReadOptions readOptions = dataset.readOptions();
          if (byConfig.containsKey(readOptions)) {
            List<FragmentMetadata> fragments = byConfig.get(readOptions);
            LanceDatasetAdapter.overwriteFragments(readOptions, fragments, sparkSchema);
          } else {
            LanceDatasetAdapter.cleanupDataset(readOptions);
          }
        }
      } else {
        for (Map.Entry<LanceSparkReadOptions, List<FragmentMetadata>> e : byConfig.entrySet()) {
          LanceSparkReadOptions readOptions = e.getKey();
          List<FragmentMetadata> fragments = e.getValue();
          if (fragments.isEmpty()) {
            continue;
          }
          LanceDatasetAdapter.appendFragments(readOptions, fragments);
        }
      }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "PartitionedBatchWrite";
    }

    /** Commit message carrying fragments grouped by dataset config. */
    static class TaskCommit implements WriterCommitMessage {
      final Map<LanceSparkReadOptions, List<FragmentMetadata>> fragmentsByConfig;

      TaskCommit(Map<LanceSparkReadOptions, List<FragmentMetadata>> fragmentsByConfig) {
        this.fragmentsByConfig = fragmentsByConfig;
      }
    }
  }

  /** DataWriterFactory that creates writers aware of partition routing. */
  static class PartitionedDataWriterFactory implements DataWriterFactory {
    private final LancePartitionConfig config;

    PartitionedDataWriterFactory(LancePartitionConfig config) {
      this.config = config;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new PartitionedDataWriter(config, partitionId, taskId);
    }
  }

  /** Writer that dispatches rows to per-partition Lance writers. */
  static class PartitionedDataWriter implements DataWriter<InternalRow> {
    private final LancePartitionConfig config;
    private final PartitionedLanceDataset partitionedTable;
    private final int partitionId;
    private final long taskId;
    // TODO: change key to String(dataseturi)
    private Map<LanceSparkReadOptions, LanceDataWriter> writers = new HashMap<>();

    PartitionedDataWriter(LancePartitionConfig config, int partitionId, long taskId) {
      this.config = config;
      try {
        this.partitionedTable = new PartitionedLanceDataset(config);
      } catch (NoSuchTableException e) {
        throw new RuntimeException("Failed to create partition table.", e);
      }
      this.partitionId = partitionId;
      this.taskId = taskId;
    }

    @Override
    public void write(InternalRow record) throws IOException {
      if (partitionedTable.partitionLevels() == 0) {
        throw new UnsupportedOperationException("Partitioned write requires partition columns");
      }

      List<String> rawPartition = partitionedTable.resolvePartitionValues(record);
      Partition partition = partitionedTable.resolveOrCreatePartition(rawPartition);
      Preconditions.checkArgument(
          partition.isLeaf(), "Expect leaf partition with partition values {}", rawPartition);

      // Write record
      LanceSparkReadOptions readOptions = partition.dataset().readOptions();
      LanceDataWriter writer = writers.get(readOptions);
      if (writer == null) {
        LanceSparkWriteOptions writeOptions =
            LanceSparkWriteOptions.builder()
                .datasetUri(readOptions.getDatasetUri())
                .namespace(readOptions.getNamespace())
                .tableId(readOptions.getTableId())
                .fromOptions(config.options())
                .build();

        LanceDataWriter.WriterFactory factory =
            new LanceDataWriter.WriterFactory(partitionedTable.schema(), writeOptions);
        writer = (LanceDataWriter) factory.createWriter(partitionId, taskId);
        writers.put(readOptions, writer);
      }
      writer.write(record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      Map<LanceSparkReadOptions, List<FragmentMetadata>> byConfig = new HashMap<>();
      for (Map.Entry<LanceSparkReadOptions, LanceDataWriter> e : writers.entrySet()) {
        LanceSparkReadOptions opt = e.getKey();
        LanceDataWriter writer = e.getValue();
        LanceBatchWrite.TaskCommit tc = (LanceBatchWrite.TaskCommit) writer.commit();
        List<FragmentMetadata> fragments = tc.getFragments();
        byConfig.put(opt, new ArrayList<>(fragments));
        writer.close();
      }
      writers.clear();
      return new PartitionedBatchWrite.TaskCommit(byConfig);
    }

    @Override
    public void abort() throws IOException {
      for (LanceDataWriter writer : writers.values()) {
        writer.abort();
      }
      writers.clear();
    }

    @Override
    public void close() throws IOException {
      for (LanceDataWriter writer : writers.values()) {
        writer.close();
      }
      writers.clear();
    }
  }
}
