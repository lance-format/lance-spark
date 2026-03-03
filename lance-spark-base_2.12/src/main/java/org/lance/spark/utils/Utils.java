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
package org.lance.spark.utils;

import org.lance.Dataset;
import org.lance.Version;
import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkCatalogConfig;
import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.time.Instant;
import java.util.List;

public class Utils {

  public static long parseVersion(String version) {
    return Long.parseUnsignedLong(version);
  }

  public static long findVersion(List<Version> versions, long timestamp) {
    long versionID = -1;
    Instant instant = instantFromTimestamp(timestamp);
    for (Version version : versions) {
      // Truncate version timestamp to microsecond precision to match Spark's
      // microsecond timestamp resolution, avoiding sub-microsecond mismatches
      Instant versionInstant = truncateToMicros(version.getDataTime().toInstant());
      if (versionInstant.compareTo(instant) <= 0) {
        versionID = version.getId();
      } else {
        break;
      }
    }
    if (versionID == -1) {
      throw new IllegalArgumentException("No version found with timestamp: " + timestamp);
    }
    return versionID;
  }

  public static Dataset openDataset(LanceSparkReadOptions readOptions) {
    if (readOptions.hasNamespace()) {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .namespace(readOptions.getNamespace())
          .tableId(readOptions.getTableId())
          .readOptions(readOptions.toReadOptions())
          .build();
    } else {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(readOptions.getDatasetUri())
          .readOptions(readOptions.toReadOptions())
          .build();
    }
  }

  public static StructType getSchema(Identifier ident, LanceSparkReadOptions readOptions)
      throws NoSuchTableException {
    try (Dataset dataset = openDataset(readOptions)) {
      return LanceArrowUtils.fromArrowSchema(dataset.getSchema());
    } catch (IllegalArgumentException e) {
      throw new NoSuchTableException(ident);
    }
  }

  /**
   * Creates LanceSparkReadOptions for this catalog.
   *
   * @param location the dataset URI
   * @param catalogConfig catalog configuration
   * @param versionId optional dataset version id
   * @param namespace optional namespace for credential vending
   * @param tableId optional table identifier
   * @param catalogName catalog name for cache isolation
   * @return a new LanceSparkReadOptions with catalog settings
   */
  public static LanceSparkReadOptions createReadOptions(
      String location,
      LanceSparkCatalogConfig catalogConfig,
      Optional<Long> versionId,
      Optional<LanceNamespace> namespace,
      Optional<List<String>> tableId,
      String catalogName) {
    LanceSparkReadOptions.Builder builder =
        LanceSparkReadOptions.builder()
            .datasetUri(location)
            .withCatalogDefaults(catalogConfig)
            .catalogName(catalogName);

    if (versionId.isPresent()) {
      builder.version(versionId.get().intValue());
    }
    if (tableId.isPresent()) {
      builder.tableId(tableId.get());
    }
    if (namespace.isPresent()) {
      builder.namespace(namespace.get());
    }

    return builder.build();
  }

  // Determine if the timestamp is in microseconds or nanoseconds and convert to Instant
  private static Instant instantFromTimestamp(long timestamp) {
    if (timestamp <= 0) {
      throw new IllegalArgumentException("Timestamp must be greater than zero");
    }
    return instantFromEpochMicros(timestamp);
  }

  private static Instant instantFromEpochMicros(long epochMicros) {
    long sec = Math.floorDiv(epochMicros, 1_000_000L);
    long nanoAdj = Math.floorMod(epochMicros, 1_000_000L) * 1_000L;
    return Instant.ofEpochSecond(sec, nanoAdj);
  }

  private static Instant truncateToMicros(Instant instant) {
    long nanos = instant.getNano();
    long truncatedNanos = (nanos / 1_000L) * 1_000L;
    return Instant.ofEpochSecond(instant.getEpochSecond(), truncatedNanos);
  }
}
