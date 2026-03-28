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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link LanceSparkWriteOptions}. */
public class LanceSparkWriteOptionsTest {

  private final String TEMP_URL = "file:///tmp/test";

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

    final LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder().datasetUri(TEMP_URL).fromOptions(options).build();

    assertEquals(WriteParams.WriteMode.OVERWRITE, writeOptions.getWriteMode());
    assertEquals(1000, writeOptions.getMaxRowsPerFile());
    assertEquals(500, writeOptions.getMaxRowsPerGroup());
    assertEquals(1048576L, writeOptions.getMaxBytesPerFile());
    assertEquals(256, writeOptions.getBatchSize());
    assertTrue(writeOptions.getEnableStableRowIds());
  }
}
