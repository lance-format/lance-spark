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

import org.apache.hadoop.security.UserGroupInformation
import org.ietf.jgss.{GSSContext, GSSManager, Oid}
import org.lance.namespace.DynamicContextProvider
import org.slf4j.LoggerFactory

import java.security.{AccessController, PrivilegedAction, PrivilegedExceptionAction}
import java.util.{Base64, Collections, Map => JMap}
import javax.security.auth.Subject

import scala.util.control.NonFatal

/**
 * Supplies an initial SPNEGO authentication token for each REST namespace operation.
 *
 * Uses the Spark login identity by default, or an independent UGI when principal/keytab are supplied.
 * Each callback creates its own GSSContext so concurrent operations never share negotiation state.
 * The endpoint must accept the initial token without further negotiation. Server authentication is
 * provided by the REST client's TLS configuration.
 *
 * @param properties provider options with the `dynamic_context_provider.` prefix removed
 */
final class KerberosContextProvider(properties: JMap[String, String])
  extends DynamicContextProvider {

  private val log = LoggerFactory.getLogger(classOf[KerberosContextProvider])
  private val config = new KerberosAuthConfig(properties)
  private val principalNameType = new Oid("1.2.840.113554.1.2.2.1")
  private val spnegoMechanism = new Oid("1.3.6.1.5.5.2")
  private val user = config.principal match {
    case Some(principal) =>
      if (!UserGroupInformation.isSecurityEnabled) {
        throw new IllegalStateException(
          "Hadoop Kerberos authentication must be enabled for keytab login")
      }
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, config.keytab.get)
    case None => UserGroupInformation.getLoginUser
  }
  // Hadoop holds this same lock when replacing and destroying tickets during re-login.
  private val credentialLock = user.doAs(new PrivilegedAction[AnyRef] {
    override def run(): AnyRef =
      Subject.getSubject(AccessController.getContext).getPrivateCredentials
  })

  if (!user.hasKerberosCredentials) {
    throw new IllegalStateException(
      "Kerberos credentials are required. Enable Hadoop Kerberos authentication and log in " +
        "through Spark, or configure both provider principal and keytab options.")
  }

  /**
   * Returns an Authorization header using a fresh token under the selected UGI's Subject.
   * Authentication failures are logged and return an invalid token for the server to reject.
   *
   * @param operation namespace operation name
   * @param objectId namespace or table identifier
   * @return context containing `headers.Authorization`
   */
  override def provideContext(operation: String, objectId: String): JMap[String, String] = {
    try {
      credentialLock.synchronized {
        user.checkTGTAndReloginFromKeytab()
        user.doAs(new PrivilegedExceptionAction[JMap[String, String]] {
          override def run(): JMap[String, String] = {
            val manager = GSSManager.getInstance()
            val serviceName = manager.createName(config.servicePrincipal, principalNameType)
            val context = manager.createContext(
              serviceName,
              spnegoMechanism,
              null,
              GSSContext.DEFAULT_LIFETIME)
            try {
              // DynamicContextProvider supplies request headers and cannot consume server responses.
              context.requestMutualAuth(false)
              val token = context.initSecContext(Array.emptyByteArray, 0, 0)
              if (token == null || token.isEmpty) {
                throw new IllegalStateException(
                  "GSSAPI did not produce a Kerberos authentication token")
              }
              Collections.singletonMap(
                "headers.Authorization",
                "Negotiate " + Base64.getEncoder.encodeToString(token))
            } finally context.dispose()
          }
        })
      }
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        failedContext(e)
      case NonFatal(e) =>
        failedContext(e)
    }
  }

  private def failedContext(error: Throwable): JMap[String, String] = {
    log.warn("Failed to generate a Kerberos authentication header", error)
    // The JNI callback cannot safely propagate Java exceptions. A zero byte is not a GSS token.
    // Override static credentials so the Kerberos-protected endpoint rejects this request.
    Collections.singletonMap("headers.Authorization", "Negotiate AA==")
  }
}
