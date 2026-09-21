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
package org.lance.spark.internal;

import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;

/** Owns a namespace client created on an executor for one Spark task or scan handle. */
public final class ExecutorNamespace implements AutoCloseable {
  private final LanceSparkReadOptions readOptions;
  private LanceNamespace namespace;

  private ExecutorNamespace(LanceSparkReadOptions readOptions, LanceNamespace namespace) {
    this.readOptions = readOptions;
    this.namespace = namespace;
  }

  /**
   * Initializes the executor namespace when credential refresh is enabled for this implementation.
   * The returned owner must be closed after every dataset or scanner using the namespace.
   */
  public static ExecutorNamespace acquire(LanceInputPartition inputPartition) {
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    String namespaceImpl = inputPartition.getNamespaceImpl();
    if (namespaceImpl == null || !readOptions.isExecutorCredentialRefresh()) {
      return new ExecutorNamespace(readOptions, null);
    }
    if (!LanceRuntime.useNamespaceOnWorkers(namespaceImpl)) {
      readOptions.setNamespace(null);
      return new ExecutorNamespace(readOptions, null);
    }

    LanceNamespace namespace =
        LanceRuntime.getOrCreateNamespace(namespaceImpl, inputPartition.getNamespaceProperties());
    readOptions.setNamespace(namespace);
    return new ExecutorNamespace(readOptions, namespace);
  }

  @Override
  public void close() {
    LanceNamespace toClose = namespace;
    namespace = null;
    if (toClose != null && readOptions.getNamespace() == toClose) {
      readOptions.setNamespace(null);
    }
    if (toClose instanceof AutoCloseable) {
      try {
        ((AutoCloseable) toClose).close();
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException("Failed to close executor namespace", e);
      }
    }
  }
}
