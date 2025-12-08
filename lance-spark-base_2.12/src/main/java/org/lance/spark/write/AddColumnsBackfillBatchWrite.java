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
import org.lance.fragment.FragmentMergeResult;
import org.lance.spark.LanceConfig;
import org.lance.spark.LanceDataset;
import org.lance.spark.internal.LanceDatasetAdapter;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AddColumnsBackfillBatchWrite implements BatchWrite {
  private static final Logger logger = LoggerFactory.getLogger(AddColumnsBackfillBatchWrite.class);

  private final StructType schema;
  private final LanceConfig config;
  private final List<String> newColumns;

  public AddColumnsBackfillBatchWrite(
      StructType schema, LanceConfig config, List<String> newColumns) {
    this.schema = schema;
    this.config = config;
    this.newColumns = newColumns;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new AddColumnsWriterFactory(schema, config, newColumns);
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
    LanceDatasetAdapter.getFragments(config).stream()
        .filter(f -> !mergedFragmentIds.contains(f.getId()))
        .forEach(fragments::add);

    Schema schema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false);
    LanceDatasetAdapter.mergeFragments(config, fragments, schema);
  }

  public static class AddColumnsWriter implements DataWriter<InternalRow> {
    private final LanceConfig config;
    private final StructType schema;
    private final int fragmentIdField;
    private final List<FragmentMetadata> fragments;

    private Schema mergedSchema;
    private StructType writerSchema;
    private int fragmentId = -1;
    private VectorSchemaRoot data;
    private org.lance.spark.arrow.LanceArrowWriter writer = null;

    public AddColumnsWriter(LanceConfig config, StructType schema, List<String> newColumns) {
      this.config = config;
      this.schema = schema;
      this.fragmentIdField = schema.fieldIndex(LanceDataset.FRAGMENT_ID_COLUMN.name());
      this.fragments = new ArrayList<>();

      this.writerSchema = new StructType();
      Arrays.stream(schema.fields())
          .filter(
              f ->
                  newColumns.contains(f.name())
                      || f.name().equals(LanceDataset.ROW_ADDRESS_COLUMN.name()))
          .forEach(f -> writerSchema = writerSchema.add(f));

      createWriter();
    }

    @Override
    public void write(InternalRow record) throws IOException {
      int fragId = record.getInt(fragmentIdField);

      if (fragmentId == -1) {
        fragmentId = fragId;
      }

      if (fragId != fragmentId && data != null) {
        // New fragment's data is coming, close the current fragment's writer.
        mergeFragment();

        fragmentId = fragId;
        createWriter();
      }

      for (int i = 0; i < writerSchema.fields().length; i++) {
        writer.field(i).write(record, schema.fieldIndex(writerSchema.fields()[i].name()));
      }
    }

    private void createWriter() {
      data =
          VectorSchemaRoot.create(
              LanceArrowUtils.toArrowSchema(writerSchema, "UTC", false, false),
              LanceDatasetAdapter.allocator);

      writer = org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(data, writerSchema);
    }

    private void mergeFragment() {
      writer.finish();

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(data, null, out)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        throw new RuntimeException("Cannot write schema root", e);
      }

      byte[] arrowData = out.toByteArray();
      ByteArrayInputStream in = new ByteArrayInputStream(arrowData);

      try (ArrowStreamReader reader = new ArrowStreamReader(in, LanceDatasetAdapter.allocator);
          ArrowArrayStream stream = ArrowArrayStream.allocateNew(LanceDatasetAdapter.allocator)) {
        Data.exportArrayStream(LanceDatasetAdapter.allocator, reader, stream);

        FragmentMergeResult result =
            LanceDatasetAdapter.mergeFragmentColumn(
                config,
                fragmentId,
                stream,
                LanceDataset.ROW_ADDRESS_COLUMN.name(),
                LanceDataset.ROW_ADDRESS_COLUMN.name());

        fragments.add(result.getFragmentMetadata());
        mergedSchema = result.getSchema().asArrowSchema();
      } catch (Exception e) {
        throw new RuntimeException("Cannot read arrow stream.", e);
      }

      data.close();
    }

    @Override
    public WriterCommitMessage commit() {
      if (fragmentId >= 0 && data != null) {
        mergeFragment();
      }

      return new TaskCommit(
          fragments, mergedSchema == null ? null : LanceArrowUtils.fromArrowSchema(mergedSchema));
    }

    @Override
    public void abort() {}

    @Override
    public void close() throws IOException {}
  }

  public static class AddColumnsWriterFactory implements DataWriterFactory {
    private final LanceConfig config;
    private final StructType schema;
    private final List<String> newColumns;

    protected AddColumnsWriterFactory(
        StructType schema, LanceConfig config, List<String> newColumns) {
      // Everything passed to writer factory should be serializable
      this.schema = schema;
      this.config = config;
      this.newColumns = newColumns;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new AddColumnsWriter(config, schema, newColumns);
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return String.format("AddColumnsWriterFactory(datasetUri=%s)", config.getDatasetUri());
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
