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

import java.util.{Map => JMap}

/**
 * Kerberos options after Lance Core strips the `dynamic_context_provider.` prefix.
 *
 * @param properties provider-specific options
 */
final private[spark] class KerberosAuthConfig(properties: JMap[String, String]) {

  /** Complete REST service principal, for example `HTTP/lance.example.com@EXAMPLE.COM`. */
  val servicePrincipal: String = optionalProperty("service_principal").getOrElse {
    throw new IllegalArgumentException("Kerberos option 'service_principal' is required")
  }

  /** Client principal for an explicit keytab login, or None to use the Spark login identity. */
  val principal: Option[String] = optionalProperty("principal")

  /** Local keytab path paired with the client principal. */
  val keytab: Option[String] = optionalProperty("keytab")

  require(
    principal.isDefined == keytab.isDefined,
    "Kerberos options 'principal' and 'keytab' must be configured together")

  private def optionalProperty(key: String): Option[String] = {
    Option(properties.get(key)).map { value =>
      require(value.trim.nonEmpty, s"Kerberos option '$key' must not be blank")
      value
    }
  }
}
