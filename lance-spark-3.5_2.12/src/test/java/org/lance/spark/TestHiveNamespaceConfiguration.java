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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestHiveNamespaceConfiguration {
  @ParameterizedTest
  @CsvSource({
    "thrift://global:9083,,",
    "thrift://session:9083,thrift://session:9083,",
    "thrift://catalog:9083,thrift://session:9083,thrift://catalog:9083"
  })
  public void testHadoopConfigurationAndCatalogOverride(
      String expectedUri, String sessionUri, String catalogUri) {
    // This test source is shared with modules that do not include the Hive test dependencies.
    assumeTrue(
        getClass().getClassLoader().getResource("org/lance/namespace/hive2/Hive2Namespace.class")
            != null);
    SparkSession spark =
        SparkSession.builder()
            .master("local")
            .appName("hive-namespace-config-test")
            .config("spark.hadoop.hive.metastore.uris", "thrift://global:9083")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", "hive2")
            .getOrCreate();
    try {
      if (sessionUri != null) {
        spark.sql("SET hive.metastore.uris=" + sessionUri);
      }
      if (catalogUri != null) {
        spark.sql("SET spark.sql.catalog.lance.hadoop.hive.metastore.uris=" + catalogUri);
      }
      BaseLanceNamespaceSparkCatalog catalog =
          (BaseLanceNamespaceSparkCatalog) spark.sessionState().catalogManager().catalog("lance");
      LanceNamespace namespace =
          LanceRuntime.getOrCreateNamespace(
              catalog.getNamespaceImpl(), catalog.getNamespaceProperties());
      assertTrue(namespace.namespaceId().contains(expectedUri), namespace.namespaceId());
    } finally {
      spark.stop();
    }
  }
}
