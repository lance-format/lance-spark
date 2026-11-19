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
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestHiveNamespaceConfiguration {
  @ParameterizedTest
  @CsvSource({
    "hive.metastore.uris,thrift://global:9083,,,thrift://global:9083",
    "hive.metastore.uris,thrift://global:9083,thrift://session:9083,,thrift://session:9083",
    "hive.metastore.uris,thrift://global:9083,thrift://session:9083,thrift://catalog:9083,thrift://catalog:9083",
    "fs.AbstractFileSystem.s3a.impl,GlobalFs,SessionFs,CatalogFs,CatalogFs"
  })
  public void testHadoopConfigurationAndCatalogOverride(
      String key,
      String globalValue,
      String sessionValue,
      String catalogValue,
      String expectedValue) {
    SparkSession spark =
        SparkSession.builder()
            .master("local")
            .appName("hive-namespace-config-test")
            .config("spark.hadoop." + key, globalValue)
            .getOrCreate();
    try {
      if (sessionValue != null) {
        spark.sql("SET " + key + "=" + sessionValue);
      }
      Map<String, String> options = new HashMap<>();
      if (catalogValue != null) {
        options.put("hadoop." + key, catalogValue);
      }
      Map<String, String> namespaceOptions =
          BaseLanceNamespaceSparkCatalog.buildNamespaceOptions(
              "hive2", new CaseInsensitiveStringMap(options));
      assertEquals(expectedValue, namespaceOptions.get("hadoop." + key));
      if (!key.equals(key.toLowerCase(Locale.ROOT))) {
        assertFalse(namespaceOptions.containsKey("hadoop." + key.toLowerCase(Locale.ROOT)));
      }
    } finally {
      spark.stop();
    }
  }
}
