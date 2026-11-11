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

import com.sun.net.httpserver.HttpServer
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.ietf.jgss.{GSSCredential, GSSManager}
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test, TestInstance, Timeout}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.lance.Dataset
import org.lance.namespace.{LanceNamespace, RestAdapter, RestNamespace}
import org.lance.namespace.errors.UnauthenticatedException
import org.lance.namespace.model.{CreateNamespaceRequest, DescribeTableRequest, RegisterTableRequest}
import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.utils.Utils
import org.lance.spark.write.SingleBatchArrowReader

import java.io.IOException
import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Path, Paths}
import java.security.PrivilegedExceptionAction
import java.util.{Arrays, Base64, Collections, HashMap => JHashMap, Map => JMap}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Exercises real Kerberos tickets and the Lance REST dynamic-provider loading path. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class TestKerberosContextProvider {

  private var tempDir: Path = _
  private var kdc: MiniKdc = _
  private var previousLogin: UserGroupInformation = _
  private var previousKrb5Conf: Option[String] = None
  private var client: UserGroupInformation = _
  private var server: UserGroupInformation = _
  private var clientPrincipal: String = _
  private var servicePrincipal: String = _

  @BeforeAll
  def startKdc(@TempDir directory: Path): Unit = {
    tempDir = directory
    previousLogin = UserGroupInformation.getLoginUser
    previousKrb5Conf = Option(System.getProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF))
    kdc = new MiniKdc(MiniKdc.createConf(), tempDir.resolve("kdc").toFile)
    kdc.start()
    val clientKeytab = tempDir.resolve("client.keytab").toFile
    val serverKeytab = tempDir.resolve("server.keytab").toFile
    kdc.createPrincipal(clientKeytab, "spark")
    kdc.createPrincipal(serverKeytab, "HTTP/localhost")
    clientPrincipal = "spark@" + kdc.getRealm
    servicePrincipal = "HTTP/localhost@" + kdc.getRealm
    val conf = new Configuration(false)
    conf.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(conf)
    client = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      clientPrincipal,
      clientKeytab.getAbsolutePath)
    server = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      servicePrincipal,
      serverKeytab.getAbsolutePath)
  }

  @BeforeEach
  def useClientLogin(): Unit = UserGroupInformation.setLoginUser(client)

  @AfterAll
  def stopKdc(): Unit = {
    try {
      if (kdc != null) kdc.stop()
    } finally {
      UserGroupInformation.reset()
      if (previousLogin != null) UserGroupInformation.setLoginUser(previousLogin)
      previousKrb5Conf match {
        case Some(path) => System.setProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF, path)
        case None => System.clearProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF)
      }
    }
  }

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
    val executor = Executors.newFixedThreadPool(4)
    try {
      val tasks = (1 to 12).map { _ =>
        new Callable[String] {
          override def call(): String = header(provider)
        }
      }.asJava
      val headers = executor.invokeAll(tasks, 30, TimeUnit.SECONDS).asScala.map(_.get())

      assertEquals(12, headers.toSet.size)
      headers.foreach(value => assertEquals(clientPrincipal, accept(value)))
    } finally executor.shutdownNow()
  }

  @Test
  def testRestNamespaceLoadsProviderAndDescribesARealDataset(): Unit = {
    val allocator = new RootAllocator()
    val namespaceRoot = tempDir.resolve("namespace")
    val backend = new RestAdapter(
      "dir",
      Collections.singletonMap("root", namespaceRoot.toString),
      "127.0.0.1",
      0)
    val proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val http = HttpClient.newHttpClient()
    val failure = new AtomicReference[Throwable]()
    val authenticated = new AtomicInteger()
    var namespace: RestNamespace = null
    try {
      backend.start()
      proxy.createContext(
        "/",
        exchange => {
          try {
            val authorization = exchange.getRequestHeaders.getFirst("Authorization")
            val identity =
              try {
                Option(authorization).map(accept)
              } catch {
                case NonFatal(_) => None
              }
            if (!identity.contains(clientPrincipal)) {
              val body =
                "{\"code\":16,\"error\":\"Kerberos authentication required\"}".getBytes(UTF_8)
              exchange.getResponseHeaders.set("WWW-Authenticate", "Negotiate")
              exchange.getResponseHeaders.set("Content-Type", "application/json")
              exchange.sendResponseHeaders(401, body.length.toLong)
              exchange.getResponseBody.write(body)
            } else {
              authenticated.incrementAndGet()
              val request = HttpRequest.newBuilder(URI.create(
                s"http://127.0.0.1:${backend.getPort}${exchange.getRequestURI}"))
                .header("Content-Type", "application/json")
                .method(
                  exchange.getRequestMethod,
                  HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody.readAllBytes()))
                .build()
              val response = http.send(request, HttpResponse.BodyHandlers.ofByteArray())
              exchange.getResponseHeaders.set("Content-Type", "application/json")
              exchange.sendResponseHeaders(response.statusCode(), response.body().length.toLong)
              exchange.getResponseBody.write(response.body())
            }
          } catch {
            case NonFatal(e) =>
              failure.compareAndSet(null, e)
              exchange.sendResponseHeaders(500, -1)
          } finally exchange.close()
        })
      proxy.start()
      val uri = s"http://127.0.0.1:${proxy.getAddress.getPort}"
      val unauthenticated = http.send(
        HttpRequest.newBuilder(URI.create(uri)).GET().build(),
        HttpResponse.BodyHandlers.discarding())
      assertEquals(401, unauthenticated.statusCode())

      val options = new JHashMap[String, String]()
      options.put("uri", uri)
      options.put("headers.Authorization", "invalid static header")
      options.put("dynamic_context_provider.impl", classOf[KerberosContextProvider].getName)
      properties().asScala.foreach { case (key, value) =>
        options.put("dynamic_context_provider." + key, value)
      }
      namespace = LanceNamespace.connect("rest", options, allocator).asInstanceOf[RestNamespace]
      val id = Arrays.asList("demo", "table")
      namespace.createNamespace(new CreateNamespaceRequest().id(Collections.singletonList("demo")))
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
      assertNull(failure.get())
    } finally {
      if (namespace != null) namespace.close()
      proxy.stop(0)
      backend.close()
      allocator.close()
    }
  }

  private def header(provider: KerberosContextProvider): String =
    provider.provideContext("describe_table", "demo$table").get("headers.Authorization")

  private def accept(authorization: String): String = {
    assertTrue(authorization.startsWith("Negotiate "))
    val token = Base64.getDecoder.decode(authorization.substring("Negotiate ".length))
    server.doAs(new PrivilegedExceptionAction[String] {
      override def run(): String = {
        val context = GSSManager.getInstance().createContext(null: GSSCredential)
        try {
          context.acceptSecContext(token, 0, token.length)
          assertTrue(context.isEstablished)
          context.getSrcName.toString
        } finally context.dispose()
      }
    })
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

  private def properties(entries: (String, String)*): JMap[String, String] = {
    val result = new JHashMap[String, String]()
    result.put("service_principal", servicePrincipal)
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }
}
