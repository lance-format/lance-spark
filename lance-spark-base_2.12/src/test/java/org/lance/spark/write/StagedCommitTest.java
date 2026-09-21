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
import java.util.HashMap;
import java.util.Map;

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
            ARROW_SCHEMA,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
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
            dataset,
            ARROW_SCHEMA,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
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
            ARROW_SCHEMA,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
    commit.abort();
  }

  @Test
  public void testAbortExistingTableClosesDataset(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator());
    StagedCommit commit =
        StagedCommit.forExistingTable(
            dataset,
            ARROW_SCHEMA,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
    commit.abort();
  }

  @Test
  public void testMergeStorageOptionsAddsNewKeys() {
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA,
            "unused://uri",
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));

    Map<String, String> extra = new HashMap<>();
    extra.put("access_key_id", "AKIA...");
    extra.put("secret_access_key", "s3cr3t");
    commit.mergeStorageOptions(extra);

    assertEquals(extra, commit.getStorageOptions());
  }

  @Test
  public void testMergeStorageOptionsOverridesExistingKeys() {
    Map<String, String> base = new HashMap<>();
    base.put("access_key_id", "stale-key");
    base.put("region", "us-west-2");
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA, "unused://uri", StagedCommitOptions.pathBased(base, false, null));

    commit.mergeStorageOptions(Collections.singletonMap("access_key_id", "fresh-key"));

    assertEquals("fresh-key", commit.getStorageOptions().get("access_key_id"));
    assertEquals("us-west-2", commit.getStorageOptions().get("region"));
  }

  @Test
  public void testMergeStorageOptionsNullAndEmptyAreNoOps() {
    Map<String, String> base = Collections.singletonMap("access_key_id", "AKIA...");
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA, "unused://uri", StagedCommitOptions.pathBased(base, false, null));

    commit.mergeStorageOptions(null);
    commit.mergeStorageOptions(Collections.emptyMap());

    assertEquals(base, commit.getStorageOptions());
  }

  @Test
  public void testMergeStorageOptionsDoesNotMutateCallerMap() {
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA,
            "unused://uri",
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));

    Map<String, String> extra = new HashMap<>();
    extra.put("access_key_id", "AKIA...");
    commit.mergeStorageOptions(extra);
    commit.getStorageOptions().put("access_key_id", "mutated-after-merge");

    assertEquals("AKIA...", extra.get("access_key_id"));
  }

  @Test
  public void testCommitNewTableWithFileFormatVersion(TestInfo testInfo) {
    String datasetUri =
        TestUtils.getDatasetUri(tempDir.toString(), testInfo.getTestMethod().get().getName());
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, "2.1"));
    commit.commit();
    try (Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertEquals("2.1", dataset.getLanceFileFormatVersion());
    }
  }

  @Test
  public void testSetFileFormatVersionOverridesStageTime(TestInfo testInfo) {
    String datasetUri =
        TestUtils.getDatasetUri(tempDir.toString(), testInfo.getTestMethod().get().getName());
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, "2.0"));
    commit.setFileFormatVersion("2.1");
    commit.commit();
    try (Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertEquals("2.1", dataset.getLanceFileFormatVersion());
    }
  }

  @Test
  public void testMergeStorageOptionsAcceptsUnmodifiableBaseMap() {
    // Path-based staged creates pass catalogConfig.getStorageOptions() directly, which
    // LanceSparkCatalogConfig wraps in Collections.unmodifiableMap(...). Without a defensive
    // copy in the constructor, merging into that map throws UnsupportedOperationException.
    Map<String, String> unmodifiableCatalogOptions =
        Collections.unmodifiableMap(Collections.singletonMap("region", "us-west-2"));
    StagedCommit commit =
        StagedCommit.forNewTable(
            ARROW_SCHEMA,
            "unused://uri",
            StagedCommitOptions.pathBased(unmodifiableCatalogOptions, false, null));

    assertDoesNotThrow(
        () -> commit.mergeStorageOptions(Collections.singletonMap("access_key_id", "AKIA...")));
    assertEquals("AKIA...", commit.getStorageOptions().get("access_key_id"));
    assertEquals("us-west-2", commit.getStorageOptions().get("region"));
  }
}
