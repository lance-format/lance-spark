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
package org.lance.spark.utils;

import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.util.LanceSerializeUtil;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The credential/open context for a blob source dataset, captured on the driver when its scan is
 * built and shipped to write executors so they can reopen the source to resolve blob references.
 *
 * <p>This mirrors how distributed compaction and index builds carry credentials to executors (see
 * {@code OptimizeTaskExecutor} / {@code FragmentIndexTask}): the executor reconstructs the open via
 * {@link Utils#openDatasetBuilder(LanceSparkReadOptions)} with {@code runtimeNamespace(...)} so
 * that vended credentials returned by {@code namespace.describeTable()} (e.g. STS tokens from
 * Iceberg REST, Polaris, Unity) keep auto-refreshing while blobs are read.
 *
 * <p>The blob source is generally a <em>different</em> table than the one a write task targets, so
 * this context cannot be derived from the write options; it must travel from the read side.
 */
public class BlobSourceContext implements Serializable {
  private static final long serialVersionUID = 1L;

  private final LanceSparkReadOptions readOptions;
  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;

  public BlobSourceContext(
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
  }

  public LanceSparkReadOptions getReadOptions() {
    return readOptions;
  }

  public Map<String, String> getInitialStorageOptions() {
    return initialStorageOptions;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  public List<String> getTableId() {
    return readOptions.getTableId();
  }

  /**
   * Decodes the contexts that {@code LanceBlobSourceContextRule} injected into the write options,
   * keyed by source dataset URI. Returns an empty map when absent (e.g. no blob sources, or the SQL
   * extension is not enabled); {@link BlobReferenceResolver} then falls back to opening sources by
   * URI alone.
   */
  public static Map<String, BlobSourceContext> decodeFromWriteOptions(
      CaseInsensitiveStringMap writeOptions) {
    String encoded = writeOptions.get(LanceConstant.BLOB_SOURCE_CONTEXTS_KEY);
    if (encoded == null || encoded.isEmpty()) {
      return Collections.emptyMap();
    }
    return LanceSerializeUtil.decode(encoded);
  }
}
