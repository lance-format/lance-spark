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
package com.lancedb.lance.spark.internal;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.compaction.Compaction;
import com.lancedb.lance.compaction.CompactionMetrics;
import com.lancedb.lance.compaction.CompactionOptions;
import com.lancedb.lance.compaction.CompactionPlan;
import com.lancedb.lance.compaction.CompactionTask;
import com.lancedb.lance.compaction.RewriteResult;
import com.lancedb.lance.fragment.FragmentMergeResult;
import com.lancedb.lance.operation.Merge;
import com.lancedb.lance.operation.Update;
import com.lancedb.lance.spark.LanceConfig;
import com.lancedb.lance.spark.SparkOptions;
import com.lancedb.lance.spark.read.LanceInputPartition;
import com.lancedb.lance.spark.utils.Optional;
import com.lancedb.lance.spark.write.LanceArrowWriter;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

public class LanceDatasetAdapter {
  public static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static Optional<StructType> getSchema(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return Optional.of(LanceArrowUtils.fromArrowSchema(dataset.getSchema()));
    } catch (IllegalArgumentException e) {
      // dataset not found
      return Optional.empty();
    }
  }

  public static Optional<StructType> getSchema(String datasetUri) {
    try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
      return Optional.of(LanceArrowUtils.fromArrowSchema(dataset.getSchema()));
    } catch (IllegalArgumentException e) {
      // dataset not found
      return Optional.empty();
    }
  }

  public static Optional<Long> getDatasetRowCount(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return Optional.of(dataset.countRows());
    } catch (IllegalArgumentException e) {
      // dataset not found
      return Optional.empty();
    }
  }

  public static Optional<Long> getDatasetDataSize(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return Optional.of(dataset.calculateDataSize());
    } catch (IllegalArgumentException e) {
      // dataset not found
      return Optional.empty();
    }
  }

  public static List<Integer> getFragmentIds(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return dataset.getFragments().stream().map(Fragment::getId).collect(Collectors.toList());
    }
  }

  public static List<FragmentMetadata> getFragments(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return dataset.getFragments().stream().map(Fragment::metadata).collect(Collectors.toList());
    }
  }

  public static LanceFragmentScanner getFragmentScanner(
      int fragmentId, LanceInputPartition inputPartition) {
    return LanceFragmentScanner.create(fragmentId, inputPartition);
  }

  public static void appendFragments(LanceConfig config, List<FragmentMetadata> fragments) {
    FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset datasetRead = Dataset.open(allocator, uri, options)) {
      Dataset.commit(
              allocator,
              config.getDatasetUri(),
              appendOp,
              java.util.Optional.of(datasetRead.version()),
              options.getStorageOptions())
          .close();
    }
  }

  public static void overwriteFragments(
      LanceConfig config, List<FragmentMetadata> fragments, StructType sparkSchema) {
    Schema schema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false);
    FragmentOperation.Overwrite overwrite = new FragmentOperation.Overwrite(fragments, schema);
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset datasetRead = Dataset.open(allocator, uri, options)) {
      Dataset.commit(
              allocator,
              config.getDatasetUri(),
              overwrite,
              java.util.Optional.of(datasetRead.version()),
              options.getStorageOptions())
          .close();
    }
  }

  public static void updateFragments(
      LanceConfig config,
      List<Long> removedFragmentIds,
      List<FragmentMetadata> updatedFragments,
      List<FragmentMetadata> newFragments) {

    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      Update update =
          Update.builder()
              .removedFragmentIds(removedFragmentIds)
              .updatedFragments(updatedFragments)
              .newFragments(newFragments)
              .build();

      dataset.newTransactionBuilder().operation(update).build().commit();
    }
  }

  public static void mergeFragments(
      LanceConfig config, List<FragmentMetadata> fragments, Schema schema) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      dataset
          .newTransactionBuilder()
          .operation(Merge.builder().fragments(fragments).schema(schema).build())
          .build()
          .commit();
    }
  }

  public static FragmentMergeResult mergeFragmentColumn(
      LanceConfig config, int fragmentId, ArrowArrayStream stream, String leftOn, String rightOn) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      Fragment fragment = dataset.getFragment(fragmentId);
      return fragment.mergeColumns(stream, leftOn, rightOn);
    }
  }

  public static FragmentMetadata deleteRows(
      LanceConfig config, int fragmentId, List<Integer> rowIndexes) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return dataset.getFragment(fragmentId).deleteRows(rowIndexes);
    }
  }

  public static LanceArrowWriter getArrowWriter(StructType sparkSchema, int batchSize) {
    return new LanceArrowWriter(
        allocator,
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false),
        sparkSchema,
        batchSize);
  }

  public static List<FragmentMetadata> createFragment(
      String datasetUri, ArrowReader reader, WriteParams params) {
    try (ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, arrowStream);
      return Fragment.create(datasetUri, arrowStream, params);
    }
  }

  public static CompactionPlan planCompaction(
      LanceConfig config, CompactionOptions compactOptions) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return Compaction.planCompaction(dataset, compactOptions);
    }
  }

  public static RewriteResult executeCompaction(LanceConfig config, CompactionTask task) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return task.execute(dataset);
    }
  }

  public static CompactionMetrics commitCompaction(
      LanceConfig config, List<RewriteResult> results, CompactionOptions compactOptions) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return Compaction.commitCompaction(dataset, results, compactOptions);
    }
  }

  public static void createDataset(String datasetUri, StructType sparkSchema, WriteParams params) {
    Dataset.create(
            allocator,
            datasetUri,
            LanceArrowUtils.toArrowSchema(sparkSchema, ZoneId.systemDefault().getId(), true, false),
            params)
        .close();
  }

  public static void dropDataset(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    Dataset.drop(uri, options.getStorageOptions());
  }
}
