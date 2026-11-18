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

import com.sun.net.httpserver.HttpServer;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Path-based opens that fail in the native object store, through {@code catalog.loadTable}. A local
 * HTTP server stands in for S3 and answers every request with one error status, so no cloud account
 * is needed. The native open reports these failures as {@link IOException}.
 */
public abstract class BasePathAccessNativeErrorTest {

  private static final LanceIdentifier PATH = new LanceIdentifier("s3://bucket/table.lance");

  private HttpServer server;

  protected abstract BaseLanceNamespaceSparkCatalog newCatalog();

  @AfterEach
  public void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  /** A catalog whose S3 endpoint answers every request with {@code status} and an S3 error code. */
  private BaseLanceNamespaceSparkCatalog catalogAnswering(int status, String code)
      throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body =
              ("<Error><Code>" + code + "</Code><Message>m</Message></Error>")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/xml");
          exchange.sendResponseHeaders(status, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();

    Map<String, String> conf = new HashMap<>();
    conf.put("storage.aws_endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
    conf.put("storage.aws_allow_http", "true");
    conf.put("storage.aws_access_key_id", "test-key");
    conf.put("storage.aws_secret_access_key", "test-secret");
    conf.put("storage.aws_region", "us-east-1");
    BaseLanceNamespaceSparkCatalog catalog = newCatalog();
    catalog.initialize("path_access_native_error", new CaseInsensitiveStringMap(conf));
    return catalog;
  }

  @Test
  public void storageAuthFailureGetsGuidance() throws IOException {
    BaseLanceNamespaceSparkCatalog catalog = catalogAnswering(403, "AccessDenied");

    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> catalog.loadTable(PATH));

    assertTrue(error.getMessage().contains(".option("), error.getMessage());
    assertTrue(error.getMessage().contains("403 Forbidden"), error.getMessage());
    assertTrue(error.getCause() instanceof IOException, String.valueOf(error.getCause()));
  }

  @Test
  public void storageAuthFailureWhileResolvingATimestampGetsGuidance() throws IOException {
    BaseLanceNamespaceSparkCatalog catalog = catalogAnswering(403, "AccessDenied");

    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> catalog.loadTable(PATH, 1L));

    assertTrue(error.getMessage().contains(".option("), error.getMessage());
    assertTrue(error.getCause() instanceof IOException, String.valueOf(error.getCause()));
  }

  @Test
  public void otherStorageFailuresAreRethrownUnchanged() throws IOException {
    BaseLanceNamespaceSparkCatalog catalog = catalogAnswering(404, "NoSuchKey");

    IOException error = assertThrows(IOException.class, () -> catalog.loadTable(PATH));

    assertTrue(error.getMessage().contains("404 Not Found"), error.getMessage());
    assertFalse(error.getMessage().contains(".option("), error.getMessage());
  }
}
