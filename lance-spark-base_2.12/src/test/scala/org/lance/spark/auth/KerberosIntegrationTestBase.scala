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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.ietf.jgss.{GSSCredential, GSSManager}
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, TestInstance}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.lance.namespace.RestAdapter

import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.security.PrivilegedExceptionAction
import java.util.{Base64, Collections, HashMap => JHashMap, Map => JMap}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.util.control.NonFatal

/** Shared local KDC and protected REST server for Kerberos integration tests. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class KerberosIntegrationTestBase {

  protected var tempDir: Path = _
  protected var kdc: MiniKdc = _
  private var previousLogin: UserGroupInformation = _
  private var previousKrb5Conf: Option[String] = None
  protected var client: UserGroupInformation = _
  protected var server: UserGroupInformation = _
  protected var clientPrincipal: String = _
  protected var servicePrincipal: String = _
  private val kerberosConf = new Configuration(false)
  kerberosConf.set("hadoop.security.authentication", "kerberos")
  kerberosConf.setLong("hadoop.kerberos.min.seconds.before.relogin", 0)

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
    UserGroupInformation.setConfiguration(kerberosConf)
    client = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      clientPrincipal,
      clientKeytab.getAbsolutePath)
    server = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      servicePrincipal,
      serverKeytab.getAbsolutePath)
  }

  @BeforeEach
  def useClientLogin(): Unit = {
    UserGroupInformation.setConfiguration(kerberosConf)
    UserGroupInformation.setLoginUser(client)
  }

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

  protected def withRestServer(body: (String, Path, AtomicInteger) => Unit): Unit = {
    val namespaceRoot = Files.createTempDirectory(tempDir, "namespace-")
    val backend = new RestAdapter(
      "dir",
      Collections.singletonMap("root", namespaceRoot.toString),
      "127.0.0.1",
      0)
    val proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val http = HttpClient.newHttpClient()
    val failure = new AtomicReference[Throwable]()
    val authenticated = new AtomicInteger()
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

      body(uri, namespaceRoot, authenticated)
      assertNull(failure.get())
    } finally {
      proxy.stop(0)
      backend.close()
    }
  }

  protected def header(provider: KerberosContextProvider): String =
    provider.provideContext("describe_table", "demo$table").get("headers.Authorization")

  protected def accept(authorization: String): String = {
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

  protected def properties(entries: (String, String)*): JMap[String, String] = {
    val result = new JHashMap[String, String]()
    result.put("service_principal", servicePrincipal)
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }
}
