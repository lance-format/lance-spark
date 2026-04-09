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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link StagedCommitOptions}. */
public class StagedCommitOptionsTest {

  private static final LanceNamespace NO_NAMESPACE = null;
  private static final List<String> NO_TABLE_ID = null;
  private static final boolean STABLE_ROW_IDS_ENABLED = true;
  private static final boolean STABLE_ROW_IDS_DISABLED = false;
  private static final boolean MANAGED_VERSIONING_ENABLED = true;
  private static final boolean MANAGED_VERSIONING_DISABLED = false;

  @Test
  public void testOfRetainsAllFields() {
    final Map<String, String> storageOptions = new HashMap<>();
    storageOptions.put("region", "us-east-1");
    storageOptions.put("timeout", "30");
    final List<String> tableId = Arrays.asList("catalog", "schema", "table");

    final StagedCommitOptions options =
        StagedCommitOptions.of(
            storageOptions,
            STABLE_ROW_IDS_ENABLED,
            NO_NAMESPACE,
            tableId,
            MANAGED_VERSIONING_ENABLED);

    assertEquals(storageOptions, options.getStorageOptions());
    assertTrue(options.isEnableStableRowIds());
    assertNull(options.getNamespace());
    assertEquals(tableId, options.getTableId());
    assertTrue(options.isManagedVersioning());
  }

  @Test
  public void testOfWithStableRowIdsDisabled() {
    final Map<String, String> storageOptions = Collections.singletonMap("key", "value");
    final List<String> tableId = Collections.singletonList("my_table");

    final StagedCommitOptions options =
        StagedCommitOptions.of(
            storageOptions,
            STABLE_ROW_IDS_DISABLED,
            NO_NAMESPACE,
            tableId,
            MANAGED_VERSIONING_DISABLED);

    assertFalse(options.isEnableStableRowIds());
    assertFalse(options.isManagedVersioning());
  }

  @Test
  public void testPathBasedSetsDefaults() {
    final Map<String, String> storageOptions = Collections.singletonMap("path", "/tmp/lance");

    final StagedCommitOptions options =
        StagedCommitOptions.pathBased(storageOptions, STABLE_ROW_IDS_ENABLED);

    assertEquals(storageOptions, options.getStorageOptions());
    assertTrue(options.isEnableStableRowIds());
    assertNull(options.getNamespace());
    assertNull(options.getTableId());
    assertFalse(options.isManagedVersioning());
  }

  @Test
  public void testPathBasedWithStableRowIdsDisabled() {
    final Map<String, String> storageOptions = Collections.emptyMap();

    final StagedCommitOptions options =
        StagedCommitOptions.pathBased(storageOptions, STABLE_ROW_IDS_DISABLED);

    assertFalse(options.isEnableStableRowIds());
    assertNull(options.getNamespace());
    assertNull(options.getTableId());
    assertFalse(options.isManagedVersioning());
  }

  @Test
  public void testOfWithEmptyStorageOptions() {
    final StagedCommitOptions options =
        StagedCommitOptions.of(
            Collections.emptyMap(),
            STABLE_ROW_IDS_DISABLED,
            NO_NAMESPACE,
            NO_TABLE_ID,
            MANAGED_VERSIONING_DISABLED);

    assertTrue(options.getStorageOptions().isEmpty());
    assertNull(options.getTableId());
  }

  @Test
  public void testStorageOptionsAreNotCopied() {
    final Map<String, String> storageOptions = new HashMap<>();
    storageOptions.put("key", "original");

    final StagedCommitOptions options =
        StagedCommitOptions.pathBased(storageOptions, STABLE_ROW_IDS_DISABLED);
    storageOptions.put("key", "modified");

    assertEquals("modified", options.getStorageOptions().get("key"));
  }
}
