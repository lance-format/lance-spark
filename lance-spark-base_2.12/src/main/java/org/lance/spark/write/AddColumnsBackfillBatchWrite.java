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

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.fragment.FragmentMergeResult;
import org.lance.operation.Merge;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.Utils;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AddColumnsBackfillBatchWrite implements BatchWrite {
  private static final Logger logger = LoggerFactory.getLogger(AddColumnsBackfillBatchWrite.class);

  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
  private final List<String> newColumns;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  public AddColumnsBackfillBatchWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      List<String> newColumns,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.newColumns = newColumns;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new AddColumnsWriterFactory(
        schema,
        writeOptions,
        newColumns,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<FragmentMetadata> fragments =
        Arrays.stream(messages)
            .map(m -> (TaskCommit) m)
            .map(TaskCommit::getFragments)
            .flatMap(List::stream)
            .collect(Collectors.toList());

    if (fragments.isEmpty()) {
      logger.info("No merged fragments to commit.");
      return;
    }

    StructType sparkSchema =
        Arrays.stream(messages)
            .map(m -> (TaskCommit) m)
            .map(TaskCommit::getSchema)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);

    if (sparkSchema == null) {
      throw new RuntimeException("No merged schema found in commit messages.");
    }

    // Some fragments may not be merged in spark task. But Lance's merge operation should only add
    // columns, not reduce fragments.
    // So the unmerged Fragments should be added back to the fragments list.
    Set<Integer> mergedFragmentIds =
        fragments.stream().map(FragmentMetadata::getId).collect(Collectors.toSet());

    // Get existing fragments
    try (Dataset dataset = Utils.openDataset(writeOptions)) {
      dataset.getFragments().stream()
          .filter(f -> !mergedFragmentIds.contains(f.getId()))
          .map(Fragment::metadata)
          .forEach(fragments::add);
    }

    // Commit merge operation using CommitBuilder
    Schema arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false);
    try (Dataset dataset = Utils.openDataset(writeOptions)) {
      Merge merge = Merge.builder().fragments(fragments).schema(arrowSchema).build();
      try (Transaction txn =
              new Transaction.Builder().readVersion(dataset.version()).operation(merge).build();
          Dataset committed =
              new CommitBuilder(dataset)
                  .writeParams(writeOptions.getStorageOptions())
                  .execute(txn)) {
        // auto-close txn and committed dataset
      }
    }
  }

  public static class AddColumnsWriter extends AbstractBackfillWriter {
    private final List<FragmentMetadata> fragments = new ArrayList<>();
    private Schema mergedSchema;

    public AddColumnsWriter(
        LanceSparkWriteOptions writeOptions,
        StructType schema,
        List<String> newColumns,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      super(
          writeOptions,
          schema,
          newColumns,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId);
    }

    @Override
    protected void processFragment(Fragment fragment, ArrowArrayStream stream) {
      FragmentMergeResult result =
          fragment.mergeColumns(
              stream,
              LanceDataset.ROW_ADDRESS_COLUMN.name(),
              LanceDataset.ROW_ADDRESS_COLUMN.name());
      fragments.add(result.getFragmentMetadata());
      mergedSchema = result.getSchema().asArrowSchema();
    }

    @Override
    protected WriterCommitMessage buildCommitMessage() {
      return new TaskCommit(
          fragments, mergedSchema == null ? null : LanceArrowUtils.fromArrowSchema(mergedSchema));
    }
  }

  public static class AddColumnsWriterFactory implements DataWriterFactory {
    private final LanceSparkWriteOptions writeOptions;
    private final StructType schema;
    private final List<String> newColumns;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    protected AddColumnsWriterFactory(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        List<String> newColumns,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      // Everything passed to writer factory should be serializable
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.newColumns = newColumns;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new AddColumnsWriter(
          writeOptions,
          schema,
          newColumns,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId);
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return String.format("AddColumnsWriterFactory(datasetUri=%s)", writeOptions.getDatasetUri());
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final List<FragmentMetadata> fragments;
    private final StructType schema;

    TaskCommit(List<FragmentMetadata> fragments, StructType schema) {
      this.fragments = fragments;
      this.schema = schema;
    }

    List<FragmentMetadata> getFragments() {
      return fragments;
    }

    StructType getSchema() {
      return schema;
    }
  }
}
