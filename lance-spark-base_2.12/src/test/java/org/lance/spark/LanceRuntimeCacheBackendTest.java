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

import org.lance.CacheBackendConfig;
import org.lance.Dataset;
import org.lance.Session;
import org.lance.spark.utils.Utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceRuntimeCacheBackendTest {

  @AfterEach
  void clearSessions() {
    LanceRuntime.clearGlobalSession();
  }

  @Test
  void createsAndReusesSessionFromBackendUris() {
    Session session =
        LanceRuntime.session("cache-uri", "moka://?capacity=1048576", "moka://?capacity=524288");

    assertSame(
        session,
        LanceRuntime.session("cache-uri", "moka://?capacity=1048576", "moka://?capacity=524288"));
  }

  @Test
  void rejectsChangingBackendAfterCatalogSessionIsCreated() {
    LanceRuntime.session("cache-switch", "moka://?capacity=1048576", "moka://?capacity=524288");

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                LanceRuntime.session(
                    "cache-switch", "moka://?capacity=2097152", "moka://?capacity=524288"));

    assertTrue(error.getMessage().contains("already initialized"));
  }

  @Test
  void switchesBackendConfigurationByCatalog() {
    Session smallCache =
        LanceRuntime.session("small-cache", "moka://?capacity=1048576", "moka://?capacity=524288");
    Session largeCache =
        LanceRuntime.session("large-cache", "moka://?capacity=2097152", "moka://?capacity=1048576");

    assertFalse(smallCache.isSameAs(largeCache));
  }

  @Test
  void configuredSessionIsUsedToOpenRealDataset() {
    String indexBackend = "moka://?capacity=1048576";
    String metadataBackend = "moka://?capacity=524288";
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .catalogName("dataset-cache")
            .indexCacheBackend(indexBackend)
            .metadataCacheBackend(metadataBackend)
            .build();

    try (Dataset dataset = Utils.openDatasetBuilder(readOptions).build()) {
      assertEquals(TestUtils.TestTable1Config.expectedValues.size(), dataset.countRows());
      assertTrue(
          dataset
              .session()
              .isSameAs(LanceRuntime.session("dataset-cache", indexBackend, metadataBackend)));
    }
  }

  @Test
  void registersStructuredJavaSdkSession() {
    Session registered =
        Session.builder()
            .indexCacheBackend(
                CacheBackendConfig.builder("moka").option("capacity", "1048576").build())
            .metadataCacheBackend(
                CacheBackendConfig.builder("moka").option("capacity", "524288").build())
            .build();

    LanceRuntime.registerSession("structured-cache", registered);

    assertSame(registered, LanceRuntime.session("structured-cache"));
  }

  @Test
  void rejectsReplacingRegisteredSession() {
    Session first = Session.builder().build();
    Session second = Session.builder().build();
    try {
      LanceRuntime.registerSession("registered-cache", first);
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> LanceRuntime.registerSession("registered-cache", second));
      assertTrue(error.getMessage().contains("already initialized"));
    } finally {
      second.close();
    }
  }
}
