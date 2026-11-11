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
package org.lance.spark.auth

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

/** Runs SQL against a protected REST namespace with an executor in a separate JVM. */
abstract class BaseKerberosSparkIntegrationTest extends KerberosIntegrationTestBase {

  @Test
  @Timeout(180)
  def testSqlReadWriteAndExecutorCredentials(): Unit =
    withRestServer { (uri, namespaceRoot, authenticated) =>
      // The local cluster launches its executor with the Maven test classpath.
      val sparkHome = tempDir.resolve("spark-home")
      Files.createDirectories(sparkHome.resolve("jars"))
      val hadoopConf = Files.createDirectories(tempDir.resolve("hadoop-conf"))
      Files.writeString(
        hadoopConf.resolve("core-site.xml"),
        "<configuration><property><name>hadoop.security.authentication</name>" +
          "<value>kerberos</value></property></configuration>",
        UTF_8)
      val settings = Map(
        "spark.testing" -> "true",
        "spark.test.home" -> sparkHome.toString,
        "spark.hadoop.hadoop.security.authentication" -> "kerberos")
      val previousSettings = settings.keys.map(key => key -> Option(System.getProperty(key))).toMap
      settings.foreach { case (key, value) => System.setProperty(key, value) }
      val classpath = System.getProperty(
        "surefire.test.class.path",
        System.getProperty("java.class.path"))
      var spark: SparkSession = null
      try {
        spark = SparkSession.builder()
          .appName("lance-kerberos-executor-test")
          .master("local-cluster[1,1,1024]")
          .config("spark.ui.enabled", "false")
          .config("spark.driver.host", "127.0.0.1")
          .config("spark.driver.bindAddress", "127.0.0.1")
          .config("spark.executor.memory", "512m")
          .config("spark.executor.extraClassPath", hadoopConf + File.pathSeparator + classpath)
          .config(
            "spark.executor.extraJavaOptions",
            "-XX:ActiveProcessorCount=2 " +
              "-Djava.security.krb5.conf=" + kdc.getKrb5conf.getAbsolutePath)
          .config("spark.executorEnv.KRB5CCNAME", tempDir.resolve("missing.ccache").toString)
          .config(
            "spark.executorEnv.SPARK_SCALA_VERSION",
            scala.util.Properties.versionNumberString.split('.').take(2).mkString("."))
          .config("spark.rpc.netty.dispatcher.numThreads", "2")
          .config("spark.task.maxFailures", "1")
          .config("spark.deploy.maxExecutorRetries", "1")
          .config("spark.sql.shuffle.partitions", "1")
          .config("spark.hadoop.hadoop.security.authentication", "kerberos")
          .getOrCreate()
        configureCatalog(spark, "keytab", uri, explicitKeytab = true)
        configureCatalog(spark, "login", uri, explicitKeytab = false)
        spark.sessionState.catalogManager.catalog("keytab")
        spark.sessionState.catalogManager.catalog("login")
        spark.udf.register("worker_pid", (_: Long) => ProcessHandle.current().pid())
        spark.udf.register(
          "provider_source",
          (_: Long) =>
            classOf[KerberosContextProvider].getProtectionDomain.getCodeSource.getLocation.toString)

        spark.sql("CREATE NAMESPACE keytab.demo")
        spark.sql("CREATE TABLE keytab.demo.data (id BIGINT) USING lance")
        spark.sql("INSERT INTO keytab.demo.data VALUES (1), (2), (3)")
        val rows = spark.sql(
          "SELECT id, worker_pid(id) AS pid, provider_source(id) AS source " +
            "FROM keytab.demo.data ORDER BY id").collect()

        assertEquals(Seq(1L, 2L, 3L), rows.map(_.getLong(0)).toSeq)
        rows.foreach(row => assertNotEquals(ProcessHandle.current().pid(), row.getLong(1)))
        val providerSource =
          classOf[KerberosContextProvider].getProtectionDomain.getCodeSource.getLocation.toString
        rows.foreach(row => assertEquals(providerSource, row.getString(2)))
        assertTrue(authenticated.get() > 0)

        // The driver login works, while the independent executor has no Kerberos login identity.
        assertEquals("id", spark.table("login.demo.data").schema.fields.head.name)
        val error = assertThrows(classOf[Exception], () => spark.table("login.demo.data").collect())
        val causes = Iterator.iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null).map(_.getMessage).mkString("\n")
        assertTrue(causes.contains("Kerberos credentials are required"), causes)
      } finally {
        try {
          if (spark != null) spark.stop()
        } finally {
          previousSettings.foreach {
            case (key, Some(value)) => System.setProperty(key, value)
            case (key, None) => System.clearProperty(key)
          }
        }
      }
    }

  private def configureCatalog(
      spark: SparkSession,
      name: String,
      uri: String,
      explicitKeytab: Boolean): Unit = {
    val prefix = "spark.sql.catalog." + name
    val options = Map(
      "" -> "org.lance.spark.LanceNamespaceSparkCatalog",
      ".impl" -> "rest",
      ".uri" -> uri,
      ".executor_credential_refresh" -> "true",
      ".dynamic_context_provider.impl" -> classOf[KerberosContextProvider].getName,
      ".dynamic_context_provider.service_principal" -> servicePrincipal)
    options.foreach { case (key, value) => spark.conf.set(prefix + key, value) }
    if (explicitKeytab) {
      spark.conf.set(prefix + ".dynamic_context_provider.principal", clientPrincipal)
      spark.conf.set(
        prefix + ".dynamic_context_provider.keytab",
        tempDir.resolve("client.keytab").toString)
    }
  }
}
