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

import org.lance.namespace.RestAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public final class LanceRestDirNamespaceServer {
  private LanceRestDirNamespaceServer() {}

  public static void main(String[] args) throws Exception {
    String root = args.length > 0 ? args[0] : "/home/lance/rest-data";
    String host = args.length > 1 ? args[1] : "127.0.0.1";
    int port = args.length > 2 ? Integer.parseInt(args[2]) : 10024;

    Map<String, String> backendConfig = new HashMap<>();
    backendConfig.put("root", root);

    RestAdapter adapter = new RestAdapter("dir", backendConfig, host, port);
    Runtime.getRuntime().addShutdownHook(new Thread(adapter::close));
    adapter.start();
    System.out.printf(
        "Lance REST directory namespace listening on http://%s:%d with root %s%n",
        host, adapter.getPort(), root);
    new CountDownLatch(1).await();
  }
}
