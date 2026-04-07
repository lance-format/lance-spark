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

import org.lance.namespace.LanceNamespace;

import java.util.List;
import java.util.Map;

/** Commit-level options for staged table operations (CREATE, REPLACE, CREATE_OR_REPLACE). */
public class StagedCommitOptions {

  private static final LanceNamespace NO_NAMESPACE = null;
  private static final List<String> NO_TABLE_ID = null;
  private static final boolean NO_MANAGED_VERSIONING = false;

  private final Map<String, String> storageOptions;
  private final boolean enableStableRowIds;
  private final LanceNamespace namespace;
  private final List<String> tableId;
  private final boolean managedVersioning;

  private StagedCommitOptions(
      final Map<String, String> storageOptions,
      final boolean enableStableRowIds,
      final LanceNamespace namespace,
      final List<String> tableId,
      final boolean managedVersioning) {
    this.storageOptions = storageOptions;
    this.enableStableRowIds = enableStableRowIds;
    this.namespace = namespace;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
  }

  public static StagedCommitOptions of(
      final Map<String, String> storageOptions,
      final boolean enableStableRowIds,
      final LanceNamespace namespace,
      final List<String> tableId,
      final boolean managedVersioning) {
    return new StagedCommitOptions(
        storageOptions, enableStableRowIds, namespace, tableId, managedVersioning);
  }

  public static StagedCommitOptions pathBased(
      final Map<String, String> storageOptions, final boolean enableStableRowIds) {
    return new StagedCommitOptions(
        storageOptions, enableStableRowIds, NO_NAMESPACE, NO_TABLE_ID, NO_MANAGED_VERSIONING);
  }

  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  public boolean isEnableStableRowIds() {
    return enableStableRowIds;
  }

  public LanceNamespace getNamespace() {
    return namespace;
  }

  public List<String> getTableId() {
    return tableId;
  }

  public boolean isManagedVersioning() {
    return managedVersioning;
  }
}
