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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._
import org.lance.Dataset
import org.lance.namespace.{LanceNamespace, RestNamespace}
import org.lance.namespace.errors.UnauthenticatedException
import org.lance.namespace.model.{CreateNamespaceRequest, DescribeTableRequest, RegisterTableRequest}
import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.utils.Utils
import org.lance.spark.write.SingleBatchArrowReader

import java.io.IOException
import java.net.URI
import java.nio.file.{Files, Paths}
import java.security.PrivilegedExceptionAction
import java.util.{Arrays, Collections, HashMap => JHashMap}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.collection.JavaConverters._

/** Exercises real Kerberos tickets and the Lance REST dynamic-provider loading path. */
@Timeout(60)
class TestKerberosContextProvider extends KerberosIntegrationTestBase {

  @Test
  def testUsesSparkLoginOutsideTheCallingSubject(): Unit = {
    val otherUser = UserGroupInformation.createRemoteUser("other")
    otherUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        val provider = new KerberosContextProvider(properties())
        assertEquals(clientPrincipal, accept(header(provider)))
      }
    })
  }

  @Test
  def testExplicitKeytabKeepsTheSparkLoginIdentity(): Unit = {
    val original = UserGroupInformation.createRemoteUser("other")
    UserGroupInformation.setLoginUser(original)
    val provider = new KerberosContextProvider(properties(
      "principal" -> clientPrincipal,
      "keytab" -> tempDir.resolve("client.keytab").toString))

    assertEquals(clientPrincipal, accept(header(provider)))
    assertSame(original, UserGroupInformation.getLoginUser)
  }

  @Test
  def testRejectsMissingKerberosCredentials(): Unit = {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("other"))
    val error = assertThrows(
      classOf[IllegalStateException],
      () => new KerberosContextProvider(properties()))

    assertTrue(error.getMessage.contains("Kerberos credentials are required"))
  }

  @Test
  def testRejectsAnUnreadableKeytab(): Unit = {
    assertThrows(
      classOf[IOException],
      () =>
        new KerberosContextProvider(properties(
          "principal" -> clientPrincipal,
          "keytab" -> tempDir.resolve("missing.keytab").toString)))
  }

  @Test
  def testRejectsKeytabLoginWhenHadoopSecurityIsDisabled(): Unit = {
    val conf = new Configuration(false)
    conf.set("hadoop.security.authentication", "simple")
    UserGroupInformation.setConfiguration(conf)
    try {
      val error = assertThrows(
        classOf[IllegalStateException],
        () =>
          new KerberosContextProvider(properties(
            "principal" -> clientPrincipal,
            "keytab" -> tempDir.resolve("client.keytab").toString)))

      assertTrue(error.getMessage.contains("Hadoop Kerberos authentication must be enabled"))
    } finally {
      conf.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(conf)
    }
  }

  @Test
  def testGssFailureReturnsAnInvalidAuthenticationToken(): Unit = {
    val provider = new KerberosContextProvider(
      properties("service_principal" -> ("HTTP/missing@" + kdc.getRealm)))

    assertEquals("Negotiate AA==", header(provider))
  }

  @Test
  def testConcurrentCallsProduceIndependentlyAcceptedTokens(): Unit = {
    val provider = new KerberosContextProvider(properties())
    assertConcurrentTokens(provider)
  }

  @Test
  def testConcurrentCallsDuringKeytabRelogin(): Unit = {
    val provider = new KerberosContextProvider(properties())
    val secondProvider = new KerberosContextProvider(properties())
    UserGroupInformation.setShouldRenewImmediatelyForTests(true)
    try assertConcurrentTokens(provider, secondProvider)
    finally UserGroupInformation.setShouldRenewImmediatelyForTests(false)
  }

  @Test
  def testKeytabReloginRecoversAfterKeytabIsRestored(): Unit = {
    val keytab = tempDir.resolve("client.keytab")
    val backup = tempDir.resolve("client.keytab.backup")
    val provider = new KerberosContextProvider(properties(
      "principal" -> clientPrincipal,
      "keytab" -> keytab.toString))
    assertEquals(clientPrincipal, accept(header(provider)))
    UserGroupInformation.setShouldRenewImmediatelyForTests(true)
    try {
      Files.move(keytab, backup)
      try assertEquals("Negotiate AA==", header(provider))
      finally Files.move(backup, keytab)
      assertEquals(clientPrincipal, accept(header(provider)))
    } finally UserGroupInformation.setShouldRenewImmediatelyForTests(false)
  }

  @Test
  def testRestNamespaceLoadsProviderAndDescribesARealDataset(): Unit =
    withRestServer { (uri, namespaceRoot, authenticated) =>
      val allocator = new RootAllocator()
      var namespace: RestNamespace = null
      try {
        val options = new JHashMap[String, String]()
        options.put("uri", uri)
        options.put("headers.Authorization", "invalid static header")
        options.put("dynamic_context_provider.impl", classOf[KerberosContextProvider].getName)
        properties().asScala.foreach { case (key, value) =>
          options.put("dynamic_context_provider." + key, value)
        }
        namespace = LanceNamespace.connect("rest", options, allocator).asInstanceOf[RestNamespace]
        val id = Arrays.asList("demo", "table")
        namespace.createNamespace(
          new CreateNamespaceRequest().id(Collections.singletonList("demo")))
        val location = namespaceRoot.resolve("table.lance").toString
        writeDataset(location, allocator)
        namespace.registerTable(new RegisterTableRequest().id(id).location("table.lance"))
        val response = namespace.describeTable(new DescribeTableRequest().id(id))

        assertEquals(
          namespaceRoot.resolve("table.lance"),
          Paths.get(URI.create(response.getLocation)))
        val dataset =
          Utils.openDatasetBuilder(LanceSparkReadOptions.from(response.getLocation)).build()
        try assertEquals(1L, dataset.countRows())
        finally dataset.close()
        assertTrue(authenticated.get() >= 3)

        val acceptedBeforeFailure = authenticated.get()
        val rejectedOptions = new JHashMap[String, String](options)
        rejectedOptions.put(
          "headers.Authorization",
          header(new KerberosContextProvider(properties())))
        rejectedOptions.put(
          "dynamic_context_provider.service_principal",
          "HTTP/missing@" + kdc.getRealm)
        val rejectedNamespace = LanceNamespace.connect("rest", rejectedOptions, allocator)
          .asInstanceOf[RestNamespace]
        try {
          assertThrows(
            classOf[UnauthenticatedException],
            () => rejectedNamespace.describeTable(new DescribeTableRequest().id(id)))
        } finally rejectedNamespace.close()
        assertEquals(acceptedBeforeFailure, authenticated.get())
      } finally {
        if (namespace != null) namespace.close()
        allocator.close()
      }
    }

  private def assertConcurrentTokens(providers: KerberosContextProvider*): Unit = {
    val executor = Executors.newFixedThreadPool(4)
    try {
      val tasks = (0 until 12).map { index =>
        new Callable[String] {
          override def call(): String = header(providers(index % providers.size))
        }
      }.asJava
      val headers = executor.invokeAll(tasks, 30, TimeUnit.SECONDS).asScala.map(_.get())

      assertEquals(12, headers.toSet.size)
      headers.foreach(value => assertEquals(clientPrincipal, accept(value)))
    } finally executor.shutdownNow()
  }

  private def writeDataset(location: String, allocator: RootAllocator): Unit = {
    val schema =
      new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(32, true))))
    val root = VectorSchemaRoot.create(schema, allocator)
    val reader = new SingleBatchArrowReader(allocator, root)
    try {
      root.allocateNew()
      root.getVector(0).asInstanceOf[IntVector].setSafe(0, 42)
      root.setRowCount(1)
      val dataset = Dataset.write().allocator(allocator).reader(reader).uri(location).execute()
      try assertEquals(1L, dataset.countRows())
      finally dataset.close()
    } finally {
      reader.close()
      root.close()
    }
  }

}
