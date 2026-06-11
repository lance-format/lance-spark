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
package org.lance.spark;

import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link LanceSparkWriteOptions}. */
public class LanceSparkWriteOptionsTest {

  private final String TEMP_URL = "file:///tmp/test";

  @Test
  public void versionIsNullByDefault() {
    LanceSparkWriteOptions opts = LanceSparkWriteOptions.from(TEMP_URL);
    assertNull(opts.getVersion());
  }

  @Test
  public void builderSetsVersion() {
    LanceSparkWriteOptions opts =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).version(7L).build();
    assertEquals(7L, opts.getVersion());
  }

  @Test
  public void fileFormatVersionUsesValueEquality() {
    LanceSparkWriteOptions left =
        LanceSparkWriteOptions.builder()
            .datasetUri(TEMP_URL)
            .fileFormatVersion(new String("stable"))
            .build();
    LanceSparkWriteOptions right =
        LanceSparkWriteOptions.builder()
            .datasetUri(TEMP_URL)
            .fileFormatVersion(new String("stable"))
            .build();

    assertEquals(left, right);
    assertEquals(left.hashCode(), right.hashCode());
  }

  @Test
  public void withVersionCopiesOptions() {
    LanceSparkWriteOptions base = LanceSparkWriteOptions.from(TEMP_URL);
    LanceSparkWriteOptions pinned = base.withVersion(3L);
    assertEquals(3L, pinned.getVersion());
    assertNull(base.getVersion());
  }

  @Test
  public void testEnableStableRowIdsParsedFromOptions() {
    final Map<String, String> options = new HashMap<>();
    options.put("path", TEMP_URL);
    options.put("enable_stable_row_ids", "true");

    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).fromOptions(options).build();

    assertTrue(writeOptions.getEnableStableRowIds());
  }

  @Test
  public void testEnableStableRowIdsFalseFromOptions() {
    final Map<String, String> options = new HashMap<>();
    options.put("path", TEMP_URL);
    options.put("enable_stable_row_ids", "false");

    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).fromOptions(options).build();

    assertFalse(writeOptions.getEnableStableRowIds());
  }

  @Test
  public void testEnableStableRowIdsNullWhenNotSet() {
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).build();

    assertNull(writeOptions.getEnableStableRowIds());
  }

  @Test
  public void testEnableStableRowIdsViaBuilder() {
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).enableStableRowIds(true).build();

    assertTrue(writeOptions.getEnableStableRowIds());
  }

  @Test
  public void testToWriteParamsPropagatesStableRowIds() {
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).enableStableRowIds(true).build();

    final WriteParams params = writeOptions.toWriteParams();
    assertTrue(params.getEnableStableRowIds().isPresent());
    assertTrue(params.getEnableStableRowIds().get());
  }

  @Test
  public void testToWriteParamsOmitsStableRowIdsWhenNull() {
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).build();

    final WriteParams params = writeOptions.toWriteParams();
    assertFalse(params.getEnableStableRowIds().isPresent());
  }

  @Test
  public void testFromOptionsWithAllWriteSettings() {
    final Map<String, String> options = new HashMap<>();
    options.put("path", TEMP_URL);
    options.put("write_mode", "OVERWRITE");
    options.put("max_row_per_file", "1000");
    options.put("max_rows_per_group", "500");
    options.put("max_bytes_per_file", "1048576");
    options.put("batch_size", "256");
    options.put("enable_stable_row_ids", "true");
    options.put("blob_pack_file_size_threshold", "2147483648");

    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).fromOptions(options).build();

    assertEquals(WriteParams.WriteMode.OVERWRITE, writeOptions.getWriteMode());
    assertEquals(1000, writeOptions.getMaxRowsPerFile());
    assertEquals(500, writeOptions.getMaxRowsPerGroup());
    assertEquals(1048576L, writeOptions.getMaxBytesPerFile());
    assertEquals(256, writeOptions.getBatchSize());
    assertTrue(writeOptions.getEnableStableRowIds());
    assertEquals(Long.valueOf(2147483648L), writeOptions.getBlobPackFileSizeThreshold());
  }

  @Test
  public void testWithCatalogDefaultsParsesWriteSettings() {
    final Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("batch_size", "512");
    catalogOptions.put("use_queued_write_buffer", "true");
    catalogOptions.put("queue_depth", "4");
    catalogOptions.put("max_batch_bytes", "4096");
    catalogOptions.put("blob_pack_file_size_threshold", "8192");

    final LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOptions);
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(TEMP_URL)
            .withCatalogDefaults(catalogConfig)
            .build();

    assertEquals(512, writeOptions.getBatchSize());
    assertTrue(writeOptions.isUseQueuedWriteBuffer());
    assertEquals(4, writeOptions.getQueueDepth());
    assertEquals(4096L, writeOptions.getMaxBatchBytes());
    assertEquals(Long.valueOf(8192L), writeOptions.getBlobPackFileSizeThreshold());
    // Catalog keys are carried into storage options as well as typed fields.
    assertEquals("512", writeOptions.getStorageOptions().get("batch_size"));
    assertEquals("8192", writeOptions.getStorageOptions().get("blob_pack_file_size_threshold"));
  }

  @Test
  public void testFromOptionsOverrideCatalogDefaults() {
    final Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("write_mode", "OVERWRITE");
    catalogOptions.put("max_row_per_file", "1000");
    catalogOptions.put("max_rows_per_group", "500");
    catalogOptions.put("max_bytes_per_file", "1048576");
    catalogOptions.put("file_format_version", "2.1");
    catalogOptions.put("batch_size", "512");
    catalogOptions.put("use_queued_write_buffer", "true");
    catalogOptions.put("queue_depth", "4");
    catalogOptions.put("enable_stable_row_ids", "true");
    catalogOptions.put("use_large_var_types", "true");
    catalogOptions.put("max_batch_bytes", "4096");
    catalogOptions.put("blob_pack_file_size_threshold", "8192");

    final Map<String, String> userOptions = new HashMap<>();
    userOptions.put("write_mode", "APPEND");
    userOptions.put("max_row_per_file", "2000");
    userOptions.put("max_rows_per_group", "1000");
    userOptions.put("max_bytes_per_file", "2097152");
    userOptions.put("file_format_version", "2.0");
    userOptions.put("batch_size", "1024");
    userOptions.put("use_queued_write_buffer", "false");
    userOptions.put("queue_depth", "2");
    userOptions.put("enable_stable_row_ids", "false");
    userOptions.put("use_large_var_types", "false");
    userOptions.put("max_batch_bytes", "16384");
    userOptions.put("blob_pack_file_size_threshold", "32768");

    final LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOptions);
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(TEMP_URL)
            .fromOptions(userOptions)
            .withCatalogDefaults(catalogConfig)
            .build();

    assertEquals(WriteParams.WriteMode.APPEND, writeOptions.getWriteMode());
    assertEquals(2000, writeOptions.getMaxRowsPerFile());
    assertEquals(1000, writeOptions.getMaxRowsPerGroup());
    assertEquals(2097152L, writeOptions.getMaxBytesPerFile());
    assertEquals("2.0", writeOptions.getFileFormatVersion());
    assertEquals(1024, writeOptions.getBatchSize());
    assertFalse(writeOptions.isUseQueuedWriteBuffer());
    assertEquals(2, writeOptions.getQueueDepth());
    assertFalse(writeOptions.getEnableStableRowIds());
    assertFalse(writeOptions.isUseLargeVarTypes());
    assertEquals(16384L, writeOptions.getMaxBatchBytes());
    assertEquals(Long.valueOf(32768L), writeOptions.getBlobPackFileSizeThreshold());
    // Typed fields and storage options stay consistent: user values win in both.
    assertEquals("1024", writeOptions.getStorageOptions().get("batch_size"));
    assertEquals("16384", writeOptions.getStorageOptions().get("max_batch_bytes"));
    assertEquals("APPEND", writeOptions.getStorageOptions().get("write_mode"));
    assertEquals("false", writeOptions.getStorageOptions().get("enable_stable_row_ids"));
  }

  @Test
  public void testCatalogDefaultsKeepTypedFieldsAndStorageOptionsConsistent() {
    final Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("batch_size", "512");

    final LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOptions);
    // Builder setters that pre-date withCatalogDefaults are re-derived from the merged
    // option map (read-path semantics): the catalog value wins for keys it defines, and
    // the typed field agrees with the storage option for every key present in the merged
    // map (setter-only values for absent keys live solely in the typed field).
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(TEMP_URL)
            .batchSize(1024)
            .withCatalogDefaults(catalogConfig)
            .build();

    assertEquals(512, writeOptions.getBatchSize());
    assertEquals("512", writeOptions.getStorageOptions().get("batch_size"));
  }

  @Test
  public void testBuilderSettersSurviveCatalogDefaultsForAbsentKeys() {
    final Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("queue_depth", "4");

    final LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOptions);
    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(TEMP_URL)
            .batchSize(1024)
            .enableStableRowIds(true)
            .withCatalogDefaults(catalogConfig)
            .build();

    assertEquals(4, writeOptions.getQueueDepth());
    assertEquals("4", writeOptions.getStorageOptions().get("queue_depth"));
    assertEquals(1024, writeOptions.getBatchSize());
    assertTrue(writeOptions.getEnableStableRowIds());
    // Setter-only values are not materialized into storage options (pre-existing builder
    // behavior): for keys absent from the merged map, the typed field is the only carrier.
    assertNull(writeOptions.getStorageOptions().get("batch_size"));
    assertNull(writeOptions.getStorageOptions().get("enable_stable_row_ids"));
  }

  @Test
  public void testBuilderDoesNotMutateCallerOptionMaps() {
    final Map<String, String> userOptions = new HashMap<>();
    userOptions.put("batch_size", "1024");
    final Map<String, String> userSnapshot = new HashMap<>(userOptions);

    final Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("queue_depth", "4");
    final Map<String, String> catalogSnapshot = new HashMap<>(catalogOptions);
    final LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOptions);

    LanceSparkWriteOptions.builder()
        .datasetUri(TEMP_URL)
        .fromOptions(userOptions)
        .withCatalogDefaults(catalogConfig)
        .build();

    assertEquals(userSnapshot, userOptions, "fromOptions must not mutate the caller's map");
    assertEquals(
        catalogSnapshot,
        catalogOptions,
        "withCatalogDefaults must not mutate the catalog config's option map");
  }

  @Test
  public void testRebuiltOverwriteOptionsSurviveJavaSerialization() throws Exception {
    final Map<String, String> options = new HashMap<>();
    options.put("batch_size", "256");
    options.put("max_batch_bytes", "4096");
    // Non-serializable stub: if the transient modifier were ever removed from the
    // namespace field, writeObject below would throw NotSerializableException.
    final LanceNamespace stubNamespace = TestUtils.stubNamespace();
    // Mirror the SparkWrite overwrite rebuild: toBuilder() + writeMode(OVERWRITE).
    final LanceSparkWriteOptions rebuilt =
        LanceSparkWriteOptions.builder()
            .datasetUri(TEMP_URL)
            .fromOptions(options)
            .blobPackFileSizeThreshold(8192L)
            .version(7L)
            .namespace(stubNamespace)
            .build()
            .toBuilder()
            .writeMode(WriteParams.WriteMode.OVERWRITE)
            .build();
    assertSame(stubNamespace, rebuilt.getNamespace());

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(rebuilt);
    }
    final LanceSparkWriteOptions copy;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (LanceSparkWriteOptions) in.readObject();
    }

    assertEquals(rebuilt, copy);
    assertEquals(WriteParams.WriteMode.OVERWRITE, copy.getWriteMode());
    assertEquals(256, copy.getBatchSize());
    assertEquals(4096L, copy.getMaxBatchBytes());
    assertEquals(Long.valueOf(8192L), copy.getBlobPackFileSizeThreshold());
    assertEquals(7L, copy.getVersion());
    assertNull(
        copy.getNamespace(),
        "namespace is transient: the non-null stub set above must not survive serialization");
  }
}
