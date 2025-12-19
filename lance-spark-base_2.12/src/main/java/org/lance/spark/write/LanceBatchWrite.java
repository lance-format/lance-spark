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

import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.ReadOptions;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LanceBatchWrite implements BatchWrite {
  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
  private final boolean overwrite;

  public LanceBatchWrite(
      StructType schema, LanceSparkWriteOptions writeOptions, boolean overwrite) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new LanceDataWriter.WriterFactory(schema, writeOptions);
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

    String uri = writeOptions.getDatasetUri();
    Map<String, String> storageOptions = writeOptions.getStorageOptions();

    // Open dataset to get current version
    long version;
    ReadOptions readOptions = new ReadOptions.Builder().setStorageOptions(storageOptions).build();
    try (Dataset dataset =
        Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(uri)
            .readOptions(readOptions)
            .build()) {
      version = dataset.version();
    }

    FragmentOperation operation;
    if (overwrite || writeOptions.isOverwrite()) {
      Schema arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", true, false);
      operation = new FragmentOperation.Overwrite(fragments, arrowSchema);
    } else {
      operation = new FragmentOperation.Append(fragments);
    }

    Dataset.commit(
        LanceRuntime.allocator(), uri, operation, java.util.Optional.of(version), storageOptions);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return String.format("LanceBatchWrite(datasetUri=%s)", writeOptions.getDatasetUri());
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final List<FragmentMetadata> fragments;

    TaskCommit(List<FragmentMetadata> fragments) {
      this.fragments = fragments;
    }

    List<FragmentMetadata> getFragments() {
      return fragments;
    }
  }
}
