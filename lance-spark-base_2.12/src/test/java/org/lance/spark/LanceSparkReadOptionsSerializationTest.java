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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LanceSparkReadOptionsSerializationTest {

  @Test
  public void testExecutorCredentialRefreshDefaultsToTrue() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").build();
    Assertions.assertTrue(
        options.isExecutorCredentialRefresh(),
        "executor_credential_refresh must default to true to preserve existing behavior");
  }

  @Test
  public void testExecutorCredentialRefreshParsedFromOptions() {
    LanceSparkReadOptions optionsFalse =
        LanceSparkReadOptions.from(
            Collections.singletonMap(
                LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false"),
            "s3://bucket/path");
    Assertions.assertFalse(optionsFalse.isExecutorCredentialRefresh());

    LanceSparkReadOptions optionsTrue =
        LanceSparkReadOptions.from(
            Collections.singletonMap(
                LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "true"),
            "s3://bucket/path");
    Assertions.assertTrue(optionsTrue.isExecutorCredentialRefresh());
  }

  @Test
  public void testDeprecatedNearestReadOptionFailsFast() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                LanceSparkReadOptions.from(
                    Collections.singletonMap("nearest", "{\"column\":\"vector\"}"),
                    "s3://bucket/path"));

    Assertions.assertTrue(exception.getMessage().contains("nearest"));
    Assertions.assertTrue(exception.getMessage().contains("VECTOR_SEARCH"));
  }

  @Test
  public void testExecutorCredentialRefreshSurvivesSerialization()
      throws IOException, ClassNotFoundException {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .executorCredentialRefresh(false)
            .build();
    Assertions.assertFalse(options.isExecutorCredentialRefresh());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(options);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    LanceSparkReadOptions deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (LanceSparkReadOptions) ois.readObject();
    }

    Assertions.assertFalse(
        deserialized.isExecutorCredentialRefresh(),
        "executor_credential_refresh must survive Java serialization (driver -> executor handoff)");
  }

  @Test
  public void testExecutorCredentialRefreshPreservedByWithVersion() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .executorCredentialRefresh(false)
            .build();

    LanceSparkReadOptions pinned = options.withVersion(7);
    Assertions.assertFalse(
        pinned.isExecutorCredentialRefresh(),
        "withVersion() must propagate the executor_credential_refresh flag");
  }

  @Test
  public void testExecutorCredentialRefreshFromCatalogDefaults() {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .build();

    Assertions.assertFalse(
        options.isExecutorCredentialRefresh(),
        "executor_credential_refresh set at catalog level must land in the typed field "
            + "so it takes effect for SELECT without .option(...) and for SQL DML");
  }

  @Test
  public void testPerReadOptionOverridesCatalogDefaults() {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    Map<String, String> merged = new HashMap<>(catalogConfig.getStorageOptions());
    merged.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "true");

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .fromOptions(merged)
            .build();

    Assertions.assertTrue(
        options.isExecutorCredentialRefresh(),
        "per-read .option(...) must override the catalog-level default");
  }
}
