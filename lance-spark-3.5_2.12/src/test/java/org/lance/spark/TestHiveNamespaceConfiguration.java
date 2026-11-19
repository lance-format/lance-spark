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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHiveNamespaceConfiguration {
  @ParameterizedTest
  @CsvSource({
    "thrift://global:9083,,",
    "thrift://session:9083,thrift://session:9083,",
    "thrift://catalog:9083,thrift://session:9083,thrift://catalog:9083"
  })
  public void testHadoopConfigurationAndCatalogOverride(
      String expectedUri, String sessionUri, String catalogUri) {
    SparkSession spark =
        SparkSession.builder()
            .master("local")
            .appName("hive-namespace-config-test")
            .config("spark.hadoop.hive.metastore.uris", "thrift://global:9083")
            .getOrCreate();
    try {
      if (sessionUri != null) {
        spark.sql("SET hive.metastore.uris=" + sessionUri);
      }
      Map<String, String> options = new HashMap<>();
      if (catalogUri != null) {
        options.put("hadoop.hive.metastore.uris", catalogUri);
      }
      Map<String, String> namespaceOptions =
          BaseLanceNamespaceSparkCatalog.buildNamespaceOptions(
              "hive2", new CaseInsensitiveStringMap(options));
      assertEquals(expectedUri, namespaceOptions.get("hadoop.hive.metastore.uris"));
    } finally {
      spark.stop();
    }
  }
}
