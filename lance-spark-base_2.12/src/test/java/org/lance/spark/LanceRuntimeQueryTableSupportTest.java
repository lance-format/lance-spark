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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.QueryTableRequest;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceRuntimeQueryTableSupportTest {

  @Test
  void catalogOnlyNamespaceDoesNotSupportQueryTable() {
    assertFalse(LanceRuntime.supportsQueryTable(CatalogOnlyNamespace.class.getName()));
  }

  @Test
  void queryingNamespaceSupportsQueryTable() {
    assertTrue(LanceRuntime.supportsQueryTable(QueryingNamespace.class.getName()));
  }

  @Test
  void directoryNamespaceSupportsQueryTable() {
    assertTrue(LanceRuntime.supportsQueryTable("dir"));
  }

  @Test
  void nullAndUnknownImplsAreUnsupported() {
    assertFalse(LanceRuntime.supportsQueryTable(null));
    assertFalse(LanceRuntime.supportsQueryTable("org.lance.namespace.DoesNotExist"));
  }

  @Test
  void openTelemetryCanBeEnabledBySystemProperty() {
    String previous = System.getProperty(LanceRuntime.SPARK_CONF_OPEN_TELEMETRY_ENABLED);
    try {
      System.setProperty(LanceRuntime.SPARK_CONF_OPEN_TELEMETRY_ENABLED, "false");
      assertFalse(LanceRuntime.isOpenTelemetryEnabled());

      System.setProperty(LanceRuntime.SPARK_CONF_OPEN_TELEMETRY_ENABLED, "true");
      assertTrue(LanceRuntime.isOpenTelemetryEnabled());
    } finally {
      if (previous == null) {
        System.clearProperty(LanceRuntime.SPARK_CONF_OPEN_TELEMETRY_ENABLED);
      } else {
        System.setProperty(LanceRuntime.SPARK_CONF_OPEN_TELEMETRY_ENABLED, previous);
      }
    }
  }

  @Test
  void openTelemetryConfigurationUsesDocumentedPrecedence() {
    assertTrue(LanceRuntime.resolveOpenTelemetryEnabled("true", "false", "false"));
    assertFalse(LanceRuntime.resolveOpenTelemetryEnabled("false", "true", "true"));
    assertTrue(LanceRuntime.resolveOpenTelemetryEnabled(null, "true", "false"));
    assertTrue(LanceRuntime.resolveOpenTelemetryEnabled(null, null, "true"));
    assertFalse(LanceRuntime.resolveOpenTelemetryEnabled(null, null, null));
  }

  @Test
  void openTelemetryConfigurationRejectsInvalidValues() {
    assertTrue(LanceRuntime.resolveOpenTelemetryEnabled(" TRUE ", null, null));
    assertFalse(LanceRuntime.resolveOpenTelemetryEnabled("yes", "true", "true"));
  }

  /** Stands in for catalog-only namespaces such as Glue, which never implement queryTable. */
  public static class CatalogOnlyNamespace implements LanceNamespace {
    public CatalogOnlyNamespace() {}

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {}

    @Override
    public String namespaceId() {
      return "catalog-only";
    }
  }

  public static class QueryingNamespace extends CatalogOnlyNamespace {
    public QueryingNamespace() {}

    @Override
    public byte[] queryTable(QueryTableRequest request) {
      return new byte[0];
    }
  }
}
