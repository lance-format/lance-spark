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
import org.lance.OpenDatasetBuilder;
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
import java.time.ZonedDateTime;
import java.util.List;

public class Utils {

  public static long parseVersion(String version) {
    return Long.parseUnsignedLong(version);
  }

  public static long findVersion(List<Version> versions, long timestampMicros) {
    long versionID = -1;
    Instant timestamp = instantFromEpochNanos(timestampMicros);
    for (Version version : versions) {
      ZonedDateTime dataTime = version.getDataTime();
      if (dataTime.toInstant().compareTo(timestamp) < 0) {
        versionID = version.getId();
      } else if (dataTime.toInstant().equals(timestamp)) {
        return version.getId();
      } else {
        break;
      }
    }
    if (versionID == -1) {
      throw new IllegalArgumentException("No version found with timestamp: " + timestampMicros);
    }
    return versionID;
  }

  public static StructType getSchema(
      Identifier ident,
      String datasetUri,
      LanceSparkReadOptions readOptions,
      LanceNamespace namespace)
      throws NoSuchTableException {
    Dataset dataset = null;
    try {
      OpenDatasetBuilder builder =
          Dataset.open()
              .allocator(LanceRuntime.allocator())
              .uri(datasetUri)
              .readOptions(readOptions.toReadOptions());
      if (namespace != null) {
        builder.namespace(namespace);
      }
      dataset = builder.build();
      return LanceArrowUtils.fromArrowSchema(dataset.getSchema());
    } catch (IllegalArgumentException e) {
      throw new NoSuchTableException(ident);
    } finally {
      if (dataset != null) {
        dataset.close();
      }
    }
  }

  public static LanceSparkReadOptions createReadOptions(
      String datasetUri, LanceSparkCatalogConfig catalogConfig) {
    return createReadOptions(datasetUri, null, catalogConfig, null, null);
  }

  public static LanceSparkReadOptions createReadOptions(
      String datasetUri,
      LanceSparkCatalogConfig catalogConfig,
      List<String> tableId,
      LanceNamespace namespace) {
    return createReadOptions(datasetUri, null, catalogConfig, namespace, tableId);
  }

  /**
   * Creates LanceSparkReadOptions for this catalog.
   *
   * @param location the dataset URI
   * @param versionId optional dataset version id
   * @return a new LanceSparkReadOptions with catalog settings
   */
  public static LanceSparkReadOptions createReadOptions(
      String location,
      Long versionId,
      LanceSparkCatalogConfig catalogConfig,
      LanceNamespace namespace,
      List<String> tableId) {
    LanceSparkReadOptions.Builder builder =
        LanceSparkReadOptions.builder().datasetUri(location).withCatalogDefaults(catalogConfig);

    if (tableId != null) {
      builder.tableId(tableId);
    }

    if (namespace != null) {
      builder.namespace(namespace);
    }

    if (versionId != null) {
      // TODO 修改version为long类型
      builder.version(versionId.intValue());
    }
    return builder.build();
  }

  // Convert microseconds since epoch to ZonedDateTime in UTC
  private static Instant instantFromEpochNanos(long epochNanos) {
    long sec = Math.floorDiv(epochNanos, 1_000_000_000L);
    long nanoAdj = Math.floorMod(epochNanos, 1_000_000_000L);
    return Instant.ofEpochSecond(sec, nanoAdj);
  }
}
