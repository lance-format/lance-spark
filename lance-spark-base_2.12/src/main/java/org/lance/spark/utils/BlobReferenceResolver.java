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
 * columnName), deduplicated by row address within each group, and each group is resolved with a
 * single {@code takeBlobs()} call.
 *
 * <p><b>takeBlobs ordering contract.</b> {@code Dataset.takeBlobs(addresses, column)} returns one
 * {@code BlobFile} per requested address, in the same order as the input (Lance's take operation
 * sorts internally for IO efficiency but remaps the result back to the requested order; when row
 * addresses are projected it also errors rather than silently dropping deleted rows). We still
 * deduplicate addresses before calling it — a one-to-many JOIN (this feature's primary target)
 * repeats the same source row across many output rows, so reading each distinct blob once both
 * avoids redundant reads and removes any reliance on how takeBlobs treats repeated addresses. A
 * returned-count mismatch (e.g. a null-descriptor row that take silently drops) is treated as a
 * hard error rather than risking a positional skew that would write the wrong bytes downstream.
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
   * indices. References are grouped by (datasetUri, columnName), deduplicated by row address, and
   * each group is resolved with a single {@code takeBlobs()} call (see the class javadoc for the
   * ordering/dedup contract this relies on).
   *
   * <p>The caller is responsible for writing the resolved bytes into the target vector. Resolved
   * bytes are returned as a map rather than written here because back-filling a variable-width
   * Arrow vector out of order corrupts its offset buffer; the caller must emit the whole vector in
   * a single ascending pass.
   *
   * @param indices vector indices corresponding to each blob reference
   * @param refs blob references to resolve
   * @return a map from vector index to resolved blob bytes
   * @throws IOException if reading blobs fails, or if {@code takeBlobs} returns an unexpected
   *     count/null that would make positional mapping unsafe
   */
  public Map<Integer, byte[]> resolveBatch(List<Integer> indices, List<BlobReference> refs)
      throws IOException {
    Map<Integer, byte[]> resolved = new HashMap<>(refs.size());

    // Group by (datasetUri, columnName), deduplicating row addresses within each group.
    Map<String, Group> groups = new HashMap<>();
    for (int i = 0; i < refs.size(); i++) {
      BlobReference ref = refs.get(i);
      String groupKey = ref.getDatasetUri() + "\0" + ref.getColumnName();
      Group group = groups.computeIfAbsent(groupKey, k -> new Group(ref));
      group.add(ref.getRowAddress(), indices.get(i));
    }

    // Resolve each group with a single takeBlobs() call over its distinct addresses, then fan the
    // bytes back out to every vector index that referenced that address.
    for (Group group : groups.values()) {
      Dataset dataset = getOrOpenDataset(group.datasetUri);
      List<Long> addresses = group.distinctAddresses; // requested order
      List<BlobFile> blobs = dataset.takeBlobs(addresses, group.columnName);

      // takeBlobs must return exactly one BlobFile per requested address, in order. A mismatch
      // means the selection hit deleted/null-descriptor rows, in which case positional mapping
      // would skew and silently write the wrong bytes into the target table — fail loudly instead.
      if (blobs.size() != addresses.size()) {
        throw new IOException(
            String.format(
                "takeBlobs returned %d blobs for %d requested addresses (column=%s, dataset=%s); "
                    + "cannot map results to rows",
                blobs.size(), addresses.size(), group.columnName, group.datasetUri));
      }

      for (int i = 0; i < addresses.size(); i++) {
        BlobFile blob = blobs.get(i);
        if (blob == null) {
          throw new IOException(
              String.format(
                  "takeBlobs returned a null blob for address %d (column=%s, dataset=%s)",
                  addresses.get(i), group.columnName, group.datasetUri));
        }
        byte[] data;
        try (BlobFile b = blob) {
          data = b.read();
        }
        for (int vectorIndex : group.indicesByAddress.get(addresses.get(i))) {
          resolved.put(vectorIndex, data);
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

  /**
   * A set of references sharing one (datasetUri, columnName), with row addresses deduplicated.
   * {@link #distinctAddresses} preserves first-seen order and is the exact list handed to {@code
   * takeBlobs}; {@link #indicesByAddress} fans each address's resolved bytes back to every vector
   * index that referenced it.
   */
  private static class Group {
    final String datasetUri;
    final String columnName;
    final List<Long> distinctAddresses = new ArrayList<>();
    final Map<Long, List<Integer>> indicesByAddress = new HashMap<>();

    Group(BlobReference first) {
      this.datasetUri = first.getDatasetUri();
      this.columnName = first.getColumnName();
    }

    void add(long address, int vectorIndex) {
      List<Integer> forAddress = indicesByAddress.get(address);
      if (forAddress == null) {
        forAddress = new ArrayList<>();
        indicesByAddress.put(address, forAddress);
        distinctAddresses.add(address);
      }
      forAddress.add(vectorIndex);
    }
  }
}
