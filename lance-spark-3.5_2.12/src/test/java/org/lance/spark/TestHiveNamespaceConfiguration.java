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

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestHiveNamespaceConfiguration {
  @Test
  public void testHadoopConfigurationAndCatalogOverride() {
    // This test source is shared with modules that do not include the Hive test dependencies.
    assumeTrue(
        getClass().getClassLoader().getResource("org/lance/namespace/hive2/Hive2Namespace.class")
            != null);
    SparkSession spark =
        SparkSession.builder()
            .master("local")
            .appName("hive-namespace-config-test")
            .config("spark.hadoop.hive.metastore.uris", "thrift://global:9083")
            .config("spark.sql.catalog.inherited", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.inherited.impl", "hive2")
            .config("spark.sql.catalog.override", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.override.impl", "hive2")
            .config(
                "spark.sql.catalog.override.hadoop.hive.metastore.uris", "thrift://catalog:9083")
            .getOrCreate();
    try {
      for (String name : new String[] {"inherited", "override"}) {
        BaseLanceNamespaceSparkCatalog catalog =
            (BaseLanceNamespaceSparkCatalog) spark.sessionState().catalogManager().catalog(name);
        String expected =
            name.equals("inherited") ? "thrift://global:9083" : "thrift://catalog:9083";
        LanceNamespace namespace =
            LanceRuntime.getOrCreateNamespace(
                catalog.getNamespaceImpl(), catalog.getNamespaceProperties());
        assertTrue(namespace.namespaceId().contains(expected), namespace.namespaceId());
      }
    } finally {
      spark.stop();
    }
  }
}
