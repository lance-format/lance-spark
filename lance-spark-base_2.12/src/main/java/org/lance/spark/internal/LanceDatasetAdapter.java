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
package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.ManifestSummary;
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.compaction.Compaction;
import org.lance.compaction.CompactionMetrics;
import org.lance.compaction.CompactionOptions;
import org.lance.compaction.CompactionPlan;
import org.lance.compaction.CompactionTask;
import org.lance.compaction.RewriteResult;
import org.lance.fragment.FragmentMergeResult;
import org.lance.index.Index;
import org.lance.index.IndexType;
import org.lance.operation.Merge;
import org.lance.operation.Update;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.lance.spark.LanceConfig;
import org.lance.spark.SparkOptions;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.Optional;
import org.lance.spark.write.LanceArrowWriter;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LanceDatasetAdapter {
  public static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /**
   * Opens a Lance dataset. Caller is responsible for closing the dataset.
   *
   * @param config the Lance configuration
   * @return the opened Dataset
   */
  public static Dataset openDataset(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    return Dataset.open(allocator, uri, options);
  }

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
      return getFragmentIds(dataset);
    }
  }

  /**
   * Get fragment IDs from an already-opened dataset.
   *
   * @param dataset the opened Dataset
   * @return list of fragment IDs
   */
  public static List<Integer> getFragmentIds(Dataset dataset) {
    return dataset.getFragments().stream().map(Fragment::getId).collect(Collectors.toList());
  }

  public static List<FragmentMetadata> getFragments(LanceConfig config) {
    String uri = config.getDatasetUri();
    ReadOptions options = SparkOptions.genReadOptionFromConfig(config);
    try (Dataset dataset = Dataset.open(allocator, uri, options)) {
      return dataset.getFragments().stream().map(Fragment::metadata).collect(Collectors.toList());
    }
  }

  /**
   * Get the row count from manifest summary metadata using an already-opened dataset. This is O(1)
   * using ManifestSummary.getTotalRows().
   *
   * @param dataset the opened Dataset
   * @return Optional containing the count if metadata is available, empty otherwise
   */
  public static Optional<Long> getCountFromMetadata(Dataset dataset) {
    try {
      ManifestSummary summary = dataset.getVersion().getManifestSummary();
      return Optional.of(summary.getTotalRows());
    } catch (Exception e) {
      // metadata not available
      return Optional.empty();
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

  /**
   * Get all indexes from the dataset.
   *
   * @param dataset the opened Dataset
   * @return list of Index objects with full metadata
   */
  public static List<Index> getIndexes(Dataset dataset) {
    return dataset.getIndexes();
  }

  /**
   * Get indexes that cover the specified column.
   *
   * @param dataset the opened Dataset
   * @param columnName the name of the column
   * @return list of indexes covering the column
   */
  public static List<Index> getIndexesForColumn(Dataset dataset, String columnName) {
    LanceSchema lanceSchema = dataset.getLanceSchema();
    Integer fieldId = findFieldIdByName(lanceSchema, columnName);
    if (fieldId == null) {
      return new ArrayList<>();
    }

    List<Index> allIndexes = dataset.getIndexes();
    return allIndexes.stream()
        .filter(index -> index.fields().contains(fieldId))
        .collect(Collectors.toList());
  }

  private static Integer findFieldIdByName(LanceSchema schema, String columnName) {
    for (LanceField field : schema.fields()) {
      if (field.getName().equals(columnName)) {
        return field.getId();
      }
    }
    return null;
  }

  /**
   * Check if an index is an exact index (BTREE or BITMAP). Exact indexes can be used for precise
   * counting without scanning.
   *
   * @param index the index to check
   * @return true if the index is an exact index
   */
  public static boolean isExactIndex(Index index) {
    IndexType type = index.indexType();
    return type == IndexType.BTREE || type == IndexType.BITMAP;
  }

  /**
   * Get fragment IDs not covered by an index.
   *
   * @param dataset the opened Dataset
   * @param index the index to check coverage for
   * @return list of fragment IDs not covered by the index
   */
  public static List<Integer> getUnindexedFragments(Dataset dataset, Index index) {
    Set<Integer> allFragments = new HashSet<>(getFragmentIds(dataset));
    index.fragments().ifPresent(allFragments::removeAll);
    return new ArrayList<>(allFragments);
  }

  /**
   * Get fragment IDs covered by an index.
   *
   * @param index the index
   * @return list of fragment IDs covered by the index, or empty if unknown
   */
  public static List<Integer> getIndexedFragments(Index index) {
    return index.fragments().orElse(new ArrayList<>());
  }

  /**
   * Count rows using a specific index.
   *
   * @param dataset the opened Dataset
   * @param indexName the name of the index
   * @param filter the filter expression
   * @param fragmentIds optional list of fragment IDs to restrict counting to
   * @return the count of matching rows
   */
  public static long countIndexedRows(
      Dataset dataset,
      String indexName,
      String filter,
      java.util.Optional<List<Integer>> fragmentIds) {
    return dataset.countIndexedRows(indexName, filter, fragmentIds);
  }
}
