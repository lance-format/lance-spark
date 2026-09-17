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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.lance.namespace.RestAdapter;

public final class LanceRestDirNamespaceServer {
  private static final String CREATE_TABLE_VERSION_COUNT_PATH =
      "/__lance_test/create_table_version_count";

  private LanceRestDirNamespaceServer() {}

  public static void main(String[] args) throws Exception {
    String root = args.length > 0 ? args[0] : "/home/lance/rest-data";
    String host = args.length > 1 ? args[1] : "127.0.0.1";
    int port = args.length > 2 ? Integer.parseInt(args[2]) : 10024;
    boolean managedVersioning = args.length > 3 && Boolean.parseBoolean(args[3]);

    Map<String, String> backendConfig = new HashMap<>();
    backendConfig.put("root", root);
    if (managedVersioning) {
      // DirectoryNamespace uses manifest storage to track versions managed by the namespace.
      backendConfig.put("manifest_enabled", "true");
      backendConfig.put("table_version_tracking_enabled", "true");
    }

    RestAdapter adapter = new RestAdapter("dir", backendConfig, host, managedVersioning ? 0 : port);
    adapter.start();

    ManagedVersioningProxy proxy =
        managedVersioning ? new ManagedVersioningProxy(host, port, adapter.getPort()) : null;
    if (proxy != null) {
      proxy.start();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (proxy != null) {
                    proxy.close();
                  }
                  adapter.close();
                }));
    System.out.printf(
        "Lance REST directory namespace listening on http://%s:%d with root %s "
            + "(managed versioning: %s)%n",
        host, managedVersioning ? port : adapter.getPort(), root, managedVersioning);
    new CountDownLatch(1).await();
  }

  /**
   * Proxies the managed REST test server so tests can observe successful namespace-owned version
   * commits. Direct Dataset commits bypass this proxy endpoint and therefore do not increment the
   * counter.
   */
  private static final class ManagedVersioningProxy implements AutoCloseable {
    private static final Set<String> HOP_BY_HOP_HEADERS =
        Set.of("connection", "content-length", "expect", "host", "transfer-encoding", "upgrade");

    private final String backendUri;
    private final HttpClient client = HttpClient.newHttpClient();
    private final AtomicLong createTableVersionCount = new AtomicLong();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final HttpServer server;

    private ManagedVersioningProxy(String host, int port, int backendPort) throws IOException {
      this.backendUri = "http://" + host + ":" + backendPort;
      this.server = HttpServer.create(new InetSocketAddress(host, port), 0);
      this.server.createContext("/", this::handle);
      this.server.setExecutor(executor);
    }

    private void start() {
      server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
      try {
        if (CREATE_TABLE_VERSION_COUNT_PATH.equals(exchange.getRequestURI().getPath())) {
          handleCreateTableVersionCount(exchange);
          return;
        }

        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        URI target = URI.create(backendUri + exchange.getRequestURI().toASCIIString());
        HttpRequest.Builder request =
            HttpRequest.newBuilder(target)
                .method(
                    exchange.getRequestMethod(),
                    HttpRequest.BodyPublishers.ofByteArray(requestBody));
        exchange
            .getRequestHeaders()
            .forEach(
                (name, values) -> {
                  if (!isHopByHopHeader(name)) {
                    values.forEach(value -> request.header(name, value));
                  }
                });

        HttpResponse<byte[]> response;
        try {
          response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
          sendResponse(
              exchange,
              502,
              ("REST proxy failed: " + errorMessage(e)).getBytes(StandardCharsets.UTF_8));
          return;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          sendResponse(exchange, 502, "REST proxy interrupted".getBytes(StandardCharsets.UTF_8));
          return;
        }

        response
            .headers()
            .map()
            .forEach(
                (name, values) -> {
                  if (!isHopByHopHeader(name)) {
                    values.forEach(value -> exchange.getResponseHeaders().add(name, value));
                  }
                });
        if (isSuccessfulCreateTableVersion(exchange, response.statusCode())) {
          createTableVersionCount.incrementAndGet();
        }
        sendResponse(exchange, response.statusCode(), response.body());
      } catch (RuntimeException e) {
        sendResponse(
            exchange,
            502,
            ("REST proxy failed: " + errorMessage(e)).getBytes(StandardCharsets.UTF_8));
      } finally {
        exchange.close();
      }
    }

    private void handleCreateTableVersionCount(HttpExchange exchange) throws IOException {
      if (!"GET".equals(exchange.getRequestMethod())) {
        sendResponse(exchange, 405, new byte[0]);
        return;
      }
      exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
      sendResponse(
          exchange,
          200,
          Long.toString(createTableVersionCount.get()).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean isSuccessfulCreateTableVersion(HttpExchange exchange, int statusCode) {
      String path = exchange.getRequestURI().getPath();
      return "POST".equals(exchange.getRequestMethod())
          && path.startsWith("/v1/table/")
          && path.endsWith("/version/create")
          && statusCode >= 200
          && statusCode < 300;
    }

    private static boolean isHopByHopHeader(String name) {
      return HOP_BY_HOP_HEADERS.contains(name.toLowerCase());
    }

    private static String errorMessage(Exception error) {
      return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, byte[] body)
        throws IOException {
      exchange.sendResponseHeaders(statusCode, body.length);
      exchange.getResponseBody().write(body);
    }

    @Override
    public void close() {
      server.stop(0);
      executor.shutdownNow();
    }
  }
}
