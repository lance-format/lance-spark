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

import org.lance.memwal.ShardingSpec;
import org.lance.spark.write.StagedCommit;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseLanceNamespaceSparkCatalogInitializeTest {

  @TempDir private Path tempDir;

  @Test
  public void testInitializePreservesCaseSensitiveOptionKeys() {
    Map<String, String> rawOptions = new HashMap<>();
    rawOptions.put("impl", "dir");
    rawOptions.put("root", tempDir.toString());
    rawOptions.put("MyMixedCaseKey", "value1");
    rawOptions.put("ALLCAPS_KEY", "value2");
    rawOptions.put("camelCaseOption", "value3");

    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(rawOptions);

    TestCatalog catalog = new TestCatalog();
    catalog.initialize("test", options);

    Map<String, String> properties = catalog.getNamespaceProperties();

    assertTrue(
        properties.containsKey("MyMixedCaseKey"),
        "Should preserve mixed-case key 'MyMixedCaseKey', got keys: " + properties.keySet());
    assertTrue(
        properties.containsKey("ALLCAPS_KEY"),
        "Should preserve all-caps key 'ALLCAPS_KEY', got keys: " + properties.keySet());
    assertTrue(
        properties.containsKey("camelCaseOption"),
        "Should preserve camelCase key 'camelCaseOption', got keys: " + properties.keySet());

    assertEquals("value1", properties.get("MyMixedCaseKey"));
    assertEquals("value2", properties.get("ALLCAPS_KEY"));
    assertEquals("value3", properties.get("camelCaseOption"));
  }

  private static class TestCatalog extends BaseLanceNamespaceSparkCatalog {
    @Override
    public LanceDataset createDataset(
        LanceSparkReadOptions readOptions,
        StructType sparkSchema,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        boolean managedVersioning,
        String fileFormatVersion,
        Map<String, String> tableProperties,
        ShardingSpec shardingSpec) {
      return null;
    }

    @Override
    public LanceDataset createStagedDataset(
        LanceSparkReadOptions readOptions,
        StructType sparkSchema,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        boolean managedVersioning,
        StagedCommit stagedCommit,
        String fileFormatVersion,
        Map<String, String> tableProperties,
        ShardingSpec shardingSpec) {
      return null;
    }
  }
}
