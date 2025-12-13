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

import org.lance.ReadOptions;
import org.lance.WriteParams;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkOptionsTest {

  private LanceConfig createConfig(Map<String, String> options) {
    options.put(LanceConfig.CONFIG_DATASET_URI, "file:///tmp/test.lance");
    return LanceConfig.from(options);
  }

  @Test
  public void testBatchSize() {
    // default value
    assertEquals(512, SparkOptions.getBatchSize(createConfig(new HashMap<>())));

    // valid values
    Map<String, String> options = new HashMap<>();
    options.put("batch_size", "1024");
    assertEquals(1024, SparkOptions.getBatchSize(createConfig(options)));

    options.clear();
    options.put("batch_size", "1");
    assertEquals(1, SparkOptions.getBatchSize(createConfig(options)));

    // invalid: non-numeric
    options.clear();
    options.put("batch_size", "invalid");
    LanceConfig invalidConfig = createConfig(options);
    assertThrows(NumberFormatException.class, () -> SparkOptions.getBatchSize(invalidConfig));

    // invalid: negative
    options.clear();
    options.put("batch_size", "-1");
    LanceConfig negativeConfig = createConfig(options);
    assertThrows(IllegalArgumentException.class, () -> SparkOptions.getBatchSize(negativeConfig));

    // invalid: zero
    options.clear();
    options.put("batch_size", "0");
    LanceConfig zeroConfig = createConfig(options);
    assertThrows(IllegalArgumentException.class, () -> SparkOptions.getBatchSize(zeroConfig));
  }

  @Test
  public void testTopNPushDown() {
    // default is true
    assertTrue(SparkOptions.enableTopNPushDown(createConfig(new HashMap<>())));

    // explicit true
    Map<String, String> options = new HashMap<>();
    options.put("topN_push_down", "true");
    assertTrue(SparkOptions.enableTopNPushDown(createConfig(options)));

    // explicit false
    options.clear();
    options.put("topN_push_down", "false");
    assertFalse(SparkOptions.enableTopNPushDown(createConfig(options)));

    // invalid value treated as false by Boolean.parseBoolean
    options.clear();
    options.put("topN_push_down", "invalid");
    assertFalse(SparkOptions.enableTopNPushDown(createConfig(options)));
  }

  @Test
  public void testOverwrite() {
    // default is append (not overwrite)
    assertFalse(SparkOptions.overwrite(createConfig(new HashMap<>())));

    // explicit append
    Map<String, String> options = new HashMap<>();
    options.put("write_mode", "append");
    assertFalse(SparkOptions.overwrite(createConfig(options)));

    // explicit overwrite
    options.clear();
    options.put("write_mode", "overwrite");
    assertTrue(SparkOptions.overwrite(createConfig(options)));

    // case insensitive
    options.clear();
    options.put("write_mode", "OVERWRITE");
    assertTrue(SparkOptions.overwrite(createConfig(options)));
  }

  @Test
  public void testReadOptions() {
    // default values (no options set)
    ReadOptions defaultOptions =
        SparkOptions.genReadOptionFromConfig(createConfig(new HashMap<>()));
    // Just verify it builds without error

    // with all read options
    Map<String, String> options = new HashMap<>();
    options.put("block_size", "4096");
    options.put("version", "5");
    options.put("index_cache_size", "100");
    options.put("metadata_cache_size", "200");
    ReadOptions readOptions = SparkOptions.genReadOptionFromConfig(createConfig(options));
    // Verify it builds without error

    // invalid block_size
    options.clear();
    options.put("block_size", "invalid");
    LanceConfig invalidBlockSize = createConfig(options);
    assertThrows(
        NumberFormatException.class, () -> SparkOptions.genReadOptionFromConfig(invalidBlockSize));

    // invalid version
    options.clear();
    options.put("version", "invalid");
    LanceConfig invalidVersion = createConfig(options);
    assertThrows(
        NumberFormatException.class, () -> SparkOptions.genReadOptionFromConfig(invalidVersion));

    // invalid index_cache_size
    options.clear();
    options.put("index_cache_size", "invalid");
    LanceConfig invalidIndexCache = createConfig(options);
    assertThrows(
        NumberFormatException.class, () -> SparkOptions.genReadOptionFromConfig(invalidIndexCache));

    // invalid metadata_cache_size
    options.clear();
    options.put("metadata_cache_size", "invalid");
    LanceConfig invalidMetadataCache = createConfig(options);
    assertThrows(
        NumberFormatException.class,
        () -> SparkOptions.genReadOptionFromConfig(invalidMetadataCache));
  }

  @Test
  public void testWriteParams() {
    // default values (no options set)
    WriteParams defaultParams =
        SparkOptions.genWriteParamsFromConfig(createConfig(new HashMap<>()));
    // Verify it builds without error

    // with numeric write options
    Map<String, String> options = new HashMap<>();
    options.put("max_row_per_file", "10000");
    options.put("max_rows_per_group", "1000");
    options.put("max_bytes_per_file", "1073741824");
    options.put("data_storage_version", "STABLE");
    WriteParams writeParams = SparkOptions.genWriteParamsFromConfig(createConfig(options));
    // Verify it builds without error

    // invalid max_row_per_file
    options.clear();
    options.put("max_row_per_file", "invalid");
    LanceConfig invalidMaxRow = createConfig(options);
    assertThrows(
        NumberFormatException.class, () -> SparkOptions.genWriteParamsFromConfig(invalidMaxRow));

    // invalid max_rows_per_group
    options.clear();
    options.put("max_rows_per_group", "invalid");
    LanceConfig invalidMaxRowsGroup = createConfig(options);
    assertThrows(
        NumberFormatException.class,
        () -> SparkOptions.genWriteParamsFromConfig(invalidMaxRowsGroup));

    // invalid max_bytes_per_file
    options.clear();
    options.put("max_bytes_per_file", "invalid");
    LanceConfig invalidMaxBytes = createConfig(options);
    assertThrows(
        NumberFormatException.class, () -> SparkOptions.genWriteParamsFromConfig(invalidMaxBytes));

    // invalid data_storage_version
    options.clear();
    options.put("data_storage_version", "INVALID_VERSION");
    LanceConfig invalidStorageVersion = createConfig(options);
    assertThrows(
        IllegalArgumentException.class,
        () -> SparkOptions.genWriteParamsFromConfig(invalidStorageVersion));

    // invalid write_mode
    options.clear();
    options.put("write_mode", "INVALID_MODE");
    LanceConfig invalidWriteMode = createConfig(options);
    assertThrows(
        IllegalArgumentException.class,
        () -> SparkOptions.genWriteParamsFromConfig(invalidWriteMode));
  }
}
