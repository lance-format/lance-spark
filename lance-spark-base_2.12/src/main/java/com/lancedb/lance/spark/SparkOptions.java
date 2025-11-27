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
package com.lancedb.lance.spark;

import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.WriteParams.LanceFileVersion;

import java.util.Map;

public class SparkOptions {

  private static final String block_size = "block_size";
  private static final String version = "version";
  private static final String index_cache_size = "index_cache_size";
  private static final String metadata_cache_size = "metadata_cache_size";
  private static final String write_mode = "write_mode";
  private static final String max_row_per_file = "max_row_per_file";
  private static final String max_rows_per_group = "max_rows_per_group";
  private static final String max_bytes_per_file = "max_bytes_per_file";
  private static final String batch_size = "batch_size";
  private static final String topN_push_down = "topN_push_down";
  private static final String data_storage_version = "data_storage_version";

  public static ReadOptions genReadOptionFromConfig(LanceConfig config) {
    ReadOptions.Builder builder = new ReadOptions.Builder();
    Map<String, String> maps = config.getOptions();
    if (maps.containsKey(block_size)) {
      builder.setBlockSize(Integer.parseInt(maps.get(block_size)));
    }
    if (maps.containsKey(version)) {
      builder.setVersion(Integer.parseInt(maps.get(version)));
    }
    if (maps.containsKey(index_cache_size)) {
      builder.setIndexCacheSize(Integer.parseInt(maps.get(index_cache_size)));
    }
    if (maps.containsKey(metadata_cache_size)) {
      builder.setMetadataCacheSize(Integer.parseInt(maps.get(metadata_cache_size)));
    }
    builder.setStorageOptions(genStorageOptions(config));
    return builder.build();
  }

  public static WriteParams genWriteParamsFromConfig(LanceConfig config) {
    WriteParams.Builder builder = new WriteParams.Builder();
    Map<String, String> maps = config.getOptions();
    if (maps.containsKey(write_mode)) {
      builder.withMode(WriteParams.WriteMode.valueOf(maps.get(write_mode)));
    }
    if (maps.containsKey(max_row_per_file)) {
      builder.withMaxRowsPerFile(Integer.parseInt(maps.get(max_row_per_file)));
    }
    if (maps.containsKey(max_rows_per_group)) {
      builder.withMaxRowsPerGroup(Integer.parseInt(maps.get(max_rows_per_group)));
    }
    if (maps.containsKey(max_bytes_per_file)) {
      builder.withMaxBytesPerFile(Long.parseLong(maps.get(max_bytes_per_file)));
    }
    if (maps.containsKey(data_storage_version)) {
      builder.withDataStorageVersion(LanceFileVersion.valueOf(maps.get(data_storage_version)));
    }
    builder.withStorageOptions(genStorageOptions(config));
    return builder.build();
  }

  private static Map<String, String> genStorageOptions(LanceConfig config) {
    return config.getOptions();
  }

  public static int getBatchSize(LanceConfig config) {
    Map<String, String> options = config.getOptions();
    if (options.containsKey(batch_size)) {
      return Integer.parseInt(options.get(batch_size));
    }
    return 512;
  }

  public static boolean enableTopNPushDown(LanceConfig config) {
    return Boolean.parseBoolean(config.getOptions().getOrDefault(topN_push_down, "true"));
  }

  public static boolean overwrite(LanceConfig config) {
    return config.getOptions().getOrDefault(write_mode, "append").equalsIgnoreCase("overwrite");
  }
}
