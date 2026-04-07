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
import org.lance.WriteParams;
import org.lance.spark.LanceRuntime;
import org.lance.spark.TestUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class StagedCommitTest {
  @TempDir Path tempDir;

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

  private String createDataset(String name) {
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), name);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build())
          .close();
    }
    return datasetUri;
  }

  @Test
  public void testCommitNewTable(TestInfo testInfo) {
    String datasetUri =
        TestUtils.getDatasetUri(tempDir.toString(), testInfo.getTestMethod().get().getName());
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA, datasetUri, Collections.emptyMap(), null, null, false);
    commit.commit();
    try (Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertEquals(0, dataset.countRows());
    }
  }

  @Test
  public void testCommitExistingTable(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator());
    StagedCommit commit =
        StagedCommit.forExistingTable(
            dataset, ARROW_SCHEMA, Collections.emptyMap(), null, null, false);
    commit.commit();
    try (Dataset reopened = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertEquals(0, reopened.countRows());
    }
  }

  @Test
  public void testAbortNewTableWithoutNamespace(TestInfo testInfo) {
    String datasetUri =
        TestUtils.getDatasetUri(tempDir.toString(), testInfo.getTestMethod().get().getName());
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA, datasetUri, Collections.emptyMap(), null, null, false);
    commit.abort();
  }

  @Test
  public void testAbortExistingTableClosesDataset(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator());
    StagedCommit commit =
        StagedCommit.forExistingTable(
            dataset, ARROW_SCHEMA, Collections.emptyMap(), null, null, false);
    commit.abort();
  }
}
