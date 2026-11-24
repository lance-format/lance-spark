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
package org.lance.spark

import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotSame}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.lance.namespace.LanceNamespace

import java.util.HashMap

class TestHiveNamespaceInitialization {
  @ParameterizedTest
  @ValueSource(strings = Array(
    "hive2",
    "hive3",
    "org.lance.namespace.hive2.Hive2Namespace",
    "org.lance.namespace.hive3.Hive3Namespace"))
  def initializesClientPoolAndReconstructsFromCatalogProperties(impl: String): Unit = {
    val uri = "thrift://127.0.0.1:1"
    val spark = SparkSession.builder()
      .master("local")
      .appName("hive-namespace-initialization-test")
      .config("spark.hadoop.hive.metastore.uris", uri)
      .config("spark.sql.catalog.lance", classOf[LanceNamespaceSparkCatalog].getName)
      .config("spark.sql.catalog.lance.impl", impl)
      .getOrCreate()
    var driver: LanceNamespace = null
    try {
      val catalog = spark.sessionState.catalogManager.catalog("lance")
        .asInstanceOf[BaseLanceNamespaceSparkCatalog]
      val namespaceField = classOf[BaseLanceNamespaceSparkCatalog].getDeclaredField("namespace")
      namespaceField.setAccessible(true)
      driver = namespaceField.get(catalog).asInstanceOf[LanceNamespace]
      assertEquals(uri, clientConfiguration(driver).get("hive.metastore.uris"))

      val properties = SerializationUtils.clone(new HashMap(catalog.getNamespaceProperties))
      spark.stop()
      // Exercise the worker entry point using only the serialized configuration, without Spark.
      val worker = LanceRuntime.getOrCreateNamespace(catalog.getNamespaceImpl, properties)
      try {
        assertNotSame(driver, worker)
        assertEquals(uri, clientConfiguration(worker).get("hive.metastore.uris"))
      } finally {
        worker.asInstanceOf[AutoCloseable].close()
      }
    } finally {
      if (driver != null) driver.asInstanceOf[AutoCloseable].close()
      spark.stop()
    }
  }

  private def clientConfiguration(namespace: LanceNamespace): Configuration = {
    val pool = namespace.getClass.getMethod("getClientPool").invoke(namespace)
    // Inspect the configuration captured at initialization, not the namespace's mutable field.
    // No pool operation is invoked, so no metastore connection is opened.
    val confField = pool.getClass.getDeclaredField("hiveConf")
    confField.setAccessible(true)
    confField.get(pool).asInstanceOf[Configuration]
  }
}
