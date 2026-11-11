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

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.{HashMap => JHashMap, Map => JMap}

/** Tests for the Kerberos provider configuration contract. */
class TestKerberosAuthConfig {

  private val servicePrincipal = "HTTP/lance.example.com@EXAMPLE.COM"
  private val clientPrincipal = "spark@EXAMPLE.COM"

  @Test
  def testDefaultsToSparkLoginIdentity(): Unit = {
    val config = new KerberosAuthConfig(properties())

    assertEquals(servicePrincipal, config.servicePrincipal)
    assertTrue(config.principal.isEmpty)
    assertTrue(config.keytab.isEmpty)
  }

  @Test
  def testAcceptsExplicitPrincipalAndKeytab(): Unit = {
    val config = new KerberosAuthConfig(
      properties("principal" -> clientPrincipal, "keytab" -> "/etc/security/spark user.keytab"))

    assertEquals(servicePrincipal, config.servicePrincipal)
    assertEquals(Some(clientPrincipal), config.principal)
    assertEquals(Some("/etc/security/spark user.keytab"), config.keytab)
  }

  @Test
  def testRequiresServicePrincipal(): Unit = {
    val error = assertThrows(
      classOf[IllegalArgumentException],
      () => new KerberosAuthConfig(new JHashMap[String, String]()))

    assertTrue(error.getMessage.contains("'service_principal' is required"))
  }

  @Test
  def testRejectsBlankOptions(): Unit = {
    for (key <- Seq("service_principal", "principal", "keytab"); value <- Seq("", " \t\n")) {
      val error = assertThrows(
        classOf[IllegalArgumentException],
        () => new KerberosAuthConfig(properties(key -> value)))

      assertTrue(error.getMessage.contains(s"'$key' must not be blank"))
    }
  }

  @Test
  def testRejectsPrincipalWithoutKeytab(): Unit = {
    val error = assertThrows(
      classOf[IllegalArgumentException],
      () => new KerberosAuthConfig(properties("principal" -> clientPrincipal)))

    assertTrue(error.getMessage.contains("'principal' and 'keytab' must be configured together"))
  }

  @Test
  def testRejectsKeytabWithoutPrincipal(): Unit = {
    val error = assertThrows(
      classOf[IllegalArgumentException],
      () => new KerberosAuthConfig(properties("keytab" -> "/etc/security/spark.keytab")))

    assertTrue(error.getMessage.contains("'principal' and 'keytab' must be configured together"))
  }

  private def properties(entries: (String, String)*): JMap[String, String] = {
    val result = new JHashMap[String, String]()
    result.put("service_principal", servicePrincipal)
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }
}
