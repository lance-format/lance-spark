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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link LanceSparkCatalogConfig#from(Map)} storage.* prefix handling. */
public class LanceSparkCatalogConfigTest {

  @Test
  public void testFromStripsStoragePrefixAndKeepsOthers() {
    Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("storage.region", "us-west-2");
    catalogOptions.put("storage.endpoint", "http://localhost:9000");
    catalogOptions.put("foo", "bar"); // non storage.* should be kept as-is

    LanceSparkCatalogConfig config = LanceSparkCatalogConfig.from(catalogOptions);

    Map<String, String> expected = new HashMap<>();
    expected.put("region", "us-west-2");
    expected.put("endpoint", "http://localhost:9000");
    expected.put("foo", "bar");

    assertEquals(expected, config.getStorageOptions());
  }

  @Test
  public void testFromIgnoresNullKeysOrValues() {
    Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("storage.region", "us-west-2");
    catalogOptions.put("a", null); // should be ignored
    catalogOptions.put(null, "v"); // should be ignored

    LanceSparkCatalogConfig config = LanceSparkCatalogConfig.from(catalogOptions);

    Map<String, String> expected = new HashMap<>();
    expected.put("region", "us-west-2");

    assertEquals(expected, config.getStorageOptions());
  }

  @Test
  public void testFromStorageKeyBecomesEmptyStringWhenOnlyPrefixProvided() {
    Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put("storage.", "v");

    LanceSparkCatalogConfig config = LanceSparkCatalogConfig.from(catalogOptions);

    Map<String, String> expected = new HashMap<>();
    expected.put("", "v"); // substring after "storage." is empty

    assertEquals(expected, config.getStorageOptions());
  }
}
