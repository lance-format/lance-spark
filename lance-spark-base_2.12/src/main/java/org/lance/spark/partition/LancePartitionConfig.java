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
package org.lance.spark.partition;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;

import static org.lance.spark.LanceSparkReadOptions.CONFIG_DATASET_URI;
import static org.lance.spark.LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS;

public class LancePartitionConfig implements Serializable {
  // Key of table name.
  public static final String TABLE_NAME = "table-name";
  // Key of table root location.
  public static final String TABLE_ROOT = "root";

  private final ImmutableMap<String, String> options;

  private LancePartitionConfig(ImmutableMap<String, String> options) {
    Preconditions.checkNotNull(options.get(TABLE_NAME), TABLE_NAME + " not set");
    Preconditions.checkNotNull(options.get(TABLE_ROOT), TABLE_ROOT + " not set");
    this.options = options;
  }

  public static LancePartitionConfig from(Map<String, String> options) {
    // Spark root location of table.
    String location = options.get(CONFIG_DATASET_URI);

    int lastSlashIndex = location.lastIndexOf('/');
    if (lastSlashIndex == -1) {
      throw new IllegalArgumentException("Invalid location: " + location);
    }
    String tableName = location.substring(lastSlashIndex + 1);

    return from(options, tableName, location);
  }

  public static LancePartitionConfig from(
      Map<String, String> options, String tableName, String location) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options);
    builder.put(TABLE_NAME, tableName);
    builder.put(TABLE_ROOT, location);
    return new LancePartitionConfig(builder.buildKeepingLast());
  }

  public static LancePartitionConfig create(Map<String, String> options) {
    return new LancePartitionConfig(ImmutableMap.copyOf(options));
  }

  public ImmutableMap<String, String> options() {
    return options;
  }

  public String tableName() {
    return options.get(TABLE_NAME);
  }

  public String location() {
    return options.get(TABLE_ROOT);
  }

  public boolean pushDownFilters() {
    return "true".equals(options.get(CONFIG_PUSH_DOWN_FILTERS));
  }
}
