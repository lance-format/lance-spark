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
package org.lance.spark.read.nativeplan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Immutable v1 native Lance read descriptor for reflection-based consumers. */
public final class LanceNativeScanPlan implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final int DESCRIPTOR_VERSION = 1;

  private final int descriptorVersion;
  private final String scanId;
  private final String datasetUri;
  private final long resolvedVersion;
  private final String sparkReadSchemaJson;
  private final String projectedReadSchemaJson;
  private final String pushedFilterSql;
  private final Integer limit;
  private final Integer offset;
  private final int batchSize;
  private final Map<String, String> storageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;
  private final String catalogName;
  private final List<LanceNativeScanSplit> splits;

  public LanceNativeScanPlan(
      String scanId,
      String datasetUri,
      long resolvedVersion,
      String sparkReadSchemaJson,
      String projectedReadSchemaJson,
      String pushedFilterSql,
      Integer limit,
      Integer offset,
      int batchSize,
      Map<String, String> storageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      String catalogName,
      List<LanceNativeScanSplit> splits) {
    this.descriptorVersion = DESCRIPTOR_VERSION;
    this.scanId = Objects.requireNonNull(scanId, "scanId");
    this.datasetUri = Objects.requireNonNull(datasetUri, "datasetUri");
    this.resolvedVersion = resolvedVersion;
    this.sparkReadSchemaJson = Objects.requireNonNull(sparkReadSchemaJson, "sparkReadSchemaJson");
    this.projectedReadSchemaJson =
        Objects.requireNonNull(projectedReadSchemaJson, "projectedReadSchemaJson");
    this.pushedFilterSql = pushedFilterSql;
    this.limit = limit;
    this.offset = offset;
    this.batchSize = batchSize;
    this.storageOptions = immutableSortedMap(storageOptions);
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = immutableSortedMap(namespaceProperties);
    this.tableId = immutableList(tableId);
    this.catalogName = catalogName;
    this.splits = immutableList(Objects.requireNonNull(splits, "splits"));
  }

  public int getDescriptorVersion() {
    return descriptorVersion;
  }

  public String getScanId() {
    return scanId;
  }

  public String getDatasetUri() {
    return datasetUri;
  }

  public long getResolvedVersion() {
    return resolvedVersion;
  }

  public String getSparkReadSchemaJson() {
    return sparkReadSchemaJson;
  }

  public String getProjectedReadSchemaJson() {
    return projectedReadSchemaJson;
  }

  public boolean hasPushedFilterSql() {
    return pushedFilterSql != null;
  }

  public String getPushedFilterSql() {
    return pushedFilterSql;
  }

  public boolean hasLimit() {
    return limit != null;
  }

  public Integer getLimit() {
    return limit;
  }

  public boolean hasOffset() {
    return offset != null;
  }

  public Integer getOffset() {
    return offset;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  public boolean hasNamespaceImpl() {
    return namespaceImpl != null;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  public boolean hasTableId() {
    return !tableId.isEmpty();
  }

  public List<String> getTableId() {
    return tableId;
  }

  public boolean hasCatalogName() {
    return catalogName != null;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public List<LanceNativeScanSplit> getSplits() {
    return splits;
  }

  private static Map<String, String> immutableSortedMap(Map<String, String> input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new TreeMap<>(input));
  }

  private static <T> List<T> immutableList(List<T> input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(new ArrayList<>(input));
  }
}
