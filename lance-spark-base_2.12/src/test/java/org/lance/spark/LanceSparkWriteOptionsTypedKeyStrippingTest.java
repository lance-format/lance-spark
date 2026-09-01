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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Mirror of {@link LanceSparkReadOptionsTypedKeyStrippingTest} for the write path. Verifies that
 * recognized typed write options ({@code write_mode}, {@code max_row_per_file}, {@code batch_size},
 * {@code path}, ...) are stripped from {@link LanceSparkWriteOptions#getStorageOptions()} after
 * parsing, so connector-level knobs do not leak into the Rust-side storage options map reserved for
 * object-store credentials and endpoint config.
 */
public class LanceSparkWriteOptionsTypedKeyStrippingTest {

  @Test
  public void writeModeIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkWriteOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkWriteOptions.CONFIG_WRITE_MODE, "APPEND");

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder().datasetUri("s3://bucket/path").fromOptions(opts).build();

    assertFalse(
        options.getStorageOptions().containsKey(LanceSparkWriteOptions.CONFIG_WRITE_MODE),
        "write_mode must not leak into Rust storage_options");
  }

  @Test
  public void maxRowsAndBytesKeysAreStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkWriteOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_FILE, "1000000");
    opts.put(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_GROUP, "8192");
    opts.put(LanceSparkWriteOptions.CONFIG_MAX_BYTES_PER_FILE, "1073741824");
    opts.put(LanceSparkWriteOptions.CONFIG_MAX_BATCH_BYTES, "268435456");

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder().datasetUri("s3://bucket/path").fromOptions(opts).build();

    Map<String, String> storage = options.getStorageOptions();
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_FILE));
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_GROUP));
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_MAX_BYTES_PER_FILE));
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_MAX_BATCH_BYTES));
  }

  @Test
  public void batchSizeAndQueueDepthAreStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkWriteOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkWriteOptions.CONFIG_BATCH_SIZE, "4096");
    opts.put(LanceSparkWriteOptions.CONFIG_QUEUE_DEPTH, "16");
    opts.put(LanceSparkWriteOptions.CONFIG_USE_QUEUED_WRITE_BUFFER, "true");

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder().datasetUri("s3://bucket/path").fromOptions(opts).build();

    Map<String, String> storage = options.getStorageOptions();
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_BATCH_SIZE));
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_QUEUE_DEPTH));
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_USE_QUEUED_WRITE_BUFFER));
  }

  @Test
  public void fileFormatAndStableRowIdsKeysAreStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkWriteOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkWriteOptions.CONFIG_FILE_FORMAT_VERSION, "2.1");
    opts.put(LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS, "true");
    opts.put(LanceSparkWriteOptions.CONFIG_USE_LARGE_VAR_TYPES, "true");

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder().datasetUri("s3://bucket/path").fromOptions(opts).build();

    Map<String, String> storage = options.getStorageOptions();
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_FILE_FORMAT_VERSION));
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS));
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_USE_LARGE_VAR_TYPES));
  }

  @Test
  public void blobPackFileSizeThresholdIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkWriteOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkWriteOptions.CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD, "1048576");

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder().datasetUri("s3://bucket/path").fromOptions(opts).build();

    assertFalse(
        options
            .getStorageOptions()
            .containsKey(LanceSparkWriteOptions.CONFIG_BLOB_PACK_FILE_SIZE_THRESHOLD));
  }

  @Test
  public void datasetUriPathKeyIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkWriteOptions.CONFIG_DATASET_URI, "s3://bucket/path");

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder().datasetUri("s3://bucket/path").fromOptions(opts).build();

    assertFalse(
        options.getStorageOptions().containsKey(LanceSparkWriteOptions.CONFIG_DATASET_URI),
        "path must not leak into Rust storage_options — it is a connector-level URL, not a "
            + "storage credential");
  }

  @Test
  public void genuineStorageKeysPassThroughUntouched() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkWriteOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    // Typed connector key (must be stripped)
    opts.put(LanceSparkWriteOptions.CONFIG_BATCH_SIZE, "4096");
    // Genuine Rust-side storage options (must pass through)
    opts.put("aws_region", "us-east-1");
    opts.put("aws_endpoint", "http://localhost:9000");
    opts.put("allow_http", "true");

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder().datasetUri("s3://bucket/path").fromOptions(opts).build();
    Map<String, String> storage = options.getStorageOptions();

    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_BATCH_SIZE));
    assertEquals("us-east-1", storage.get("aws_region"));
    assertEquals("http://localhost:9000", storage.get("aws_endpoint"));
    assertEquals("true", storage.get("allow_http"));
  }

  @Test
  public void catalogDefaultsWithTypedKeysDoNotLeak() {
    // Simulates spark-defaults.conf: spark.sql.catalog.foo.batch_size=4096
    //                                 spark.sql.catalog.foo.aws_region=us-east-1
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkWriteOptions.CONFIG_BATCH_SIZE, "4096");
    catalogOpts.put("aws_region", "us-east-1");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    LanceSparkWriteOptions options =
        LanceSparkWriteOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .build();

    Map<String, String> storage = options.getStorageOptions();
    assertFalse(storage.containsKey(LanceSparkWriteOptions.CONFIG_BATCH_SIZE));
    assertEquals("us-east-1", storage.get("aws_region"));
  }
}
