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
import org.lance.ReadOptions;
import org.lance.spark.LanceRuntime;

import org.apache.arrow.vector.LargeVarBinaryVector;

import java.io.IOException;
import java.util.ArrayList;
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
 */
public class BlobReferenceResolver implements AutoCloseable {

  /** Cache of opened datasets keyed by dataset URI. */
  private final Map<String, Dataset> datasetCache = new HashMap<>();

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
   * Resolves a batch of blob references and writes the resolved bytes directly into the target
   * vector. References are grouped by (datasetUri, columnName) and each group is resolved with a
   * single {@code takeBlobs()} call.
   *
   * @param indices vector indices corresponding to each blob reference
   * @param refs blob references to resolve
   * @param vector the target vector to back-fill with resolved bytes
   * @throws IOException if reading blobs fails
   */
  public void resolveBatch(
      List<Integer> indices, List<BlobReference> refs, LargeVarBinaryVector vector)
      throws IOException {
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
            byte[] data = blob.read();
            vector.setSafe(ir.vectorIndex, data);
          }
        } else {
          vector.setSafe(ir.vectorIndex, new byte[0]);
        }
      }
    }
  }

  private Dataset getOrOpenDataset(String datasetUri) {
    return datasetCache.computeIfAbsent(
        datasetUri,
        uri -> {
          ReadOptions.Builder builder = new ReadOptions.Builder();
          builder.setSession(LanceRuntime.session());
          return Dataset.open()
              .allocator(LanceRuntime.allocator())
              .uri(uri)
              .readOptions(builder.build())
              .build();
        });
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
