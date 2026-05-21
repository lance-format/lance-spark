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

import org.lance.BlobFile;
import org.lance.Dataset;
import org.lance.spark.LanceSparkReadOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves {@link BlobReference} objects to actual blob bytes by opening the source datasets and
 * calling {@code Dataset.takeBlobs()}.
 *
 * <p>Datasets are cached for the lifetime of this resolver to amortize open costs across batches.
 * Resolution is done in true batches: all pending references are grouped by (datasetUri,
 * columnName) and each group is resolved with a single {@code takeBlobs()} call.
 *
 * <p>Source datasets are opened through {@link Utils#openDatasetBuilder(LanceSparkReadOptions)}
 * using the per-source {@link BlobSourceContext} captured on the driver (keyed by dataset URI).
 * This keeps the namespace client attached via {@code runtimeNamespace(...)} so vended credentials
 * keep auto-refreshing — exactly how distributed compaction/index builds open datasets on
 * executors. When no context is registered for a URI (e.g. a local filesystem source, or when the
 * SQL extension that captures contexts is not enabled), it falls back to opening by URI with
 * default options.
 */
public class BlobReferenceResolver implements AutoCloseable {

  /** Cache of opened datasets keyed by dataset URI. */
  private final Map<String, Dataset> datasetCache = new HashMap<>();

  /** Per-source open/credential context, keyed by dataset URI. */
  private final Map<String, BlobSourceContext> sourceContexts;

  public BlobReferenceResolver() {
    this(Collections.emptyMap());
  }

  public BlobReferenceResolver(Map<String, BlobSourceContext> sourceContexts) {
    this.sourceContexts =
        sourceContexts != null ? sourceContexts : Collections.<String, BlobSourceContext>emptyMap();
  }

  /**
   * Resolves a single blob reference to actual blob bytes.
   *
   * @param ref the blob reference to resolve
   * @return the actual blob bytes
   * @throws IOException if reading the blob fails
   */
  public byte[] resolve(BlobReference ref) throws IOException {
    Dataset dataset = getOrOpenDataset(ref.getDatasetUri());
    List<Long> rowAddresses = new ArrayList<>(1);
    rowAddresses.add(ref.getRowAddress());
    List<BlobFile> blobs = dataset.takeBlobs(rowAddresses, ref.getColumnName());
    if (blobs.isEmpty()) {
      return new byte[0];
    }
    try (BlobFile blob = blobs.get(0)) {
      return blob.read();
    }
  }

  /**
   * Checks if a byte array is a blob reference and resolves it. If the bytes are not a blob
   * reference, returns them unchanged.
   */
  public byte[] resolveIfNeeded(byte[] bytes) throws IOException {
    if (BlobReference.isBlobReference(bytes)) {
      BlobReference ref = BlobReference.deserialize(bytes);
      return resolve(ref);
    }
    return bytes;
  }

  /**
   * Resolves a batch of blob references to their actual bytes, keyed by the caller-supplied vector
   * indices. References are grouped by (datasetUri, columnName) and each group is resolved with a
   * single {@code takeBlobs()} call.
   *
   * <p>The caller is responsible for writing the resolved bytes into the target vector. Resolved
   * bytes are returned as a map rather than written here because back-filling a variable-width
   * Arrow vector out of order corrupts its offset buffer; the caller must emit the whole vector in
   * a single ascending pass.
   *
   * @param indices vector indices corresponding to each blob reference
   * @param refs blob references to resolve
   * @return a map from vector index to resolved blob bytes
   * @throws IOException if reading blobs fails
   */
  public Map<Integer, byte[]> resolveBatch(List<Integer> indices, List<BlobReference> refs)
      throws IOException {
    Map<Integer, byte[]> resolved = new HashMap<>(refs.size());

    // Group by (datasetUri, columnName)
    Map<String, List<IndexedRef>> groups = new HashMap<>();
    for (int i = 0; i < refs.size(); i++) {
      int vectorIndex = indices.get(i);
      BlobReference ref = refs.get(i);
      String groupKey = ref.getDatasetUri() + "\0" + ref.getColumnName();
      groups
          .computeIfAbsent(groupKey, k -> new ArrayList<>())
          .add(new IndexedRef(vectorIndex, ref));
    }

    // Resolve each group with a single takeBlobs() call
    for (List<IndexedRef> group : groups.values()) {
      BlobReference first = group.get(0).ref;
      Dataset dataset = getOrOpenDataset(first.getDatasetUri());

      List<Long> rowAddresses = new ArrayList<>(group.size());
      for (IndexedRef ir : group) {
        rowAddresses.add(ir.ref.getRowAddress());
      }

      List<BlobFile> blobs = dataset.takeBlobs(rowAddresses, first.getColumnName());

      for (int i = 0; i < group.size(); i++) {
        IndexedRef ir = group.get(i);
        if (i < blobs.size()) {
          try (BlobFile blob = blobs.get(i)) {
            resolved.put(ir.vectorIndex, blob.read());
          }
        } else {
          resolved.put(ir.vectorIndex, new byte[0]);
        }
      }
    }
    return resolved;
  }

  private Dataset getOrOpenDataset(String datasetUri) {
    return datasetCache.computeIfAbsent(datasetUri, this::openDataset);
  }

  private Dataset openDataset(String datasetUri) {
    BlobSourceContext context = sourceContexts.get(datasetUri);
    if (context != null) {
      // Reopen the source the same way executors do for compaction/index: route through the
      // namespace client so vended (auto-refreshing) credentials remain valid while reading blobs.
      return Utils.openDatasetBuilder(context.getReadOptions())
          .initialStorageOptions(context.getInitialStorageOptions())
          .runtimeNamespace(
              context.getNamespaceImpl(), context.getNamespaceProperties(), context.getTableId())
          .build();
    }
    // No captured context (e.g. local filesystem source, or the capture extension is not enabled).
    return Utils.openDatasetBuilder(LanceSparkReadOptions.from(datasetUri)).build();
  }

  @Override
  public void close() {
    for (Dataset dataset : datasetCache.values()) {
      try {
        dataset.close();
      } catch (Exception e) {
        // Best effort cleanup
      }
    }
    datasetCache.clear();
  }

  private static class IndexedRef {
    final int vectorIndex;
    final BlobReference ref;

    IndexedRef(int vectorIndex, BlobReference ref) {
      this.vectorIndex = vectorIndex;
      this.ref = ref;
    }
  }
}
