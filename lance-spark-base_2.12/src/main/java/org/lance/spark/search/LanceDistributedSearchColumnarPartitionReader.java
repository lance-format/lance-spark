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
package org.lance.spark.search;

import org.lance.Dataset;
import org.lance.index.DistanceType;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceConstant;
import org.lance.spark.internal.ExecutorNamespace;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

/**
 * Worker-side partition reader for distributed VECTOR_SEARCH. Opens the Lance dataset locally and
 * runs one of:
 *
 * <ul>
 *   <li>indexed unit ({@code indexSegments} non-empty) → {@code ScanOptions.indexSegments(...)}
 *   <li>fallback unit ({@code fragmentIds} non-empty) → {@code ScanOptions.fragmentIds(...)}
 * </ul>
 *
 * and iterates Arrow batches into Spark {@link ColumnarBatch}.
 */
public class LanceDistributedSearchColumnarPartitionReader
    implements PartitionReader<ColumnarBatch> {
  private final LanceDistributedSearchInputPartition partition;
  private ExecutorNamespace executorNamespace;
  private Dataset dataset;
  private LanceScanner scanner;
  private ArrowReader reader;
  private ColumnarBatch currentBatch;
  private boolean finished;

  public LanceDistributedSearchColumnarPartitionReader(
      LanceDistributedSearchInputPartition partition) {
    this.partition = partition;
  }

  @Override
  public boolean next() throws IOException {
    if (finished) {
      return false;
    }
    if (reader == null) {
      openReader();
    }
    if (reader.loadNextBatch()) {
      currentBatch =
          LanceSearchColumnarPartitionReader.toColumnarBatch(
              reader.getVectorSchemaRoot(), partition.getSchema());
      return true;
    }
    finished = true;
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    Throwable first = null;
    if (currentBatch != null) {
      try {
        currentBatch.close();
      } catch (Throwable t) {
        first = t;
      }
    }
    first = closeQuietly(reader, first);
    first = closeQuietly(scanner, first);
    first = closeQuietly(dataset, first);
    first = closeQuietly(executorNamespace, first);
    if (first != null) {
      throw new IOException("Failed to close LanceDistributedSearchColumnarPartitionReader", first);
    }
  }

  private static Throwable closeQuietly(AutoCloseable closeable, Throwable carried) {
    if (closeable == null) {
      return carried;
    }
    try {
      closeable.close();
      return carried;
    } catch (Throwable t) {
      return carried == null ? t : carried;
    }
  }

  private void openReader() throws IOException {
    LanceSearchQuery query = partition.getQuery();
    executorNamespace =
        ExecutorNamespace.acquire(
            query.getReadOptions(), query.getNamespaceImpl(), query.getNamespaceProperties());
    dataset =
        Utils.openDatasetBuilder(query.getReadOptions())
            .initialStorageOptions(query.getInitialStorageOptions())
            .build();
    ScanOptions opts = buildScanOptions(partition);
    try {
      scanner = dataset.newScan(opts);
      reader = scanner.scanBatches();
    } catch (Exception e) {
      throw new IOException(
          "Failed to open distributed search scan for partition: " + describe(partition), e);
    }
  }

  static ScanOptions buildScanOptions(LanceDistributedSearchInputPartition p) {
    LanceSearchQuery base = p.getQuery();
    String column = base.getVectorColumn();
    if (column == null || column.isEmpty()) {
      throw new IllegalStateException(
          "vector column must be resolved on the driver before scheduling worker tasks");
    }

    boolean fallbackUnit = p.getIndexSegments().isEmpty();

    Query.Builder q =
        new Query.Builder()
            .setColumn(column)
            .setKey(toFloatArray(base.getVector()))
            .setK(base.getK());
    // lance-core defaults use_index to true, and a fragment-restricted scan still picks up an
    // index segment covering the fragment, so a flat unit has to opt out to stay exact.
    if (fallbackUnit) {
      q.setUseIndex(false);
    }
    if (base.getDistanceType() != null && !base.getDistanceType().isEmpty()) {
      q.setDistanceType(parseDistanceType(base.getDistanceType()));
    }
    if (base.getNprobes() != null) {
      // Exact bounds, matching how lance maps a namespace query's nprobes since lance#9184. A
      // server on an older mapping treats it as a minimum; see vector-search.md.
      q.setNprobes(base.getNprobes());
    }
    if (base.getEf() != null) {
      q.setEf(base.getEf());
    }
    if (base.getRefineFactor() != null) {
      q.setRefineFactor(base.getRefineFactor());
    }

    ScanOptions.Builder b = new ScanOptions.Builder().nearest(q.build());
    // ensure_not_fragment_scan rejects a fragment-restricted nearest scan unless prefilter is
    // set, so a fallback unit always sets it. Indexed units only do what the user asked; a
    // filtered query cannot reach a worker without prefilter=true.
    if (fallbackUnit || Boolean.TRUE.equals(base.getPrefilter())) {
      b.prefilter(true);
    }
    if (base.getFilter() != null) {
      b.filter(base.getFilter());
    }
    if (!base.getOutputColumns().isEmpty()) {
      b.columns(base.getOutputColumns());
    }
    if (Boolean.TRUE.equals(base.getWithRowId()) || schemaHasRowId(p.getSchema())) {
      b.withRowId(true);
    }

    if (!p.getIndexSegments().isEmpty()) {
      b.indexSegments(p.getIndexSegments());
    } else {
      b.fragmentIds(p.getFragmentIds());
    }
    return b.build();
  }

  private static boolean schemaHasRowId(StructType schema) {
    for (StructField field : schema.fields()) {
      if (field.name().equals(LanceConstant.ROW_ID)) {
        return true;
      }
    }
    return false;
  }

  private static DistanceType parseDistanceType(String name) {
    switch (LanceSearchQuery.canonicalizeDistanceType(name)) {
      case "l2":
        return DistanceType.L2;
      case "cosine":
        return DistanceType.Cosine;
      case "dot":
        return DistanceType.Dot;
      case "hamming":
        return DistanceType.Hamming;
      default:
        throw new IllegalArgumentException("Unsupported distance_type: " + name);
    }
  }

  private static float[] toFloatArray(List<Float> vec) {
    float[] arr = new float[vec.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = vec.get(i);
    }
    return arr;
  }

  private static String describe(LanceDistributedSearchInputPartition p) {
    if (!p.getIndexSegments().isEmpty()) {
      return "indexed segments=" + p.getIndexSegments();
    }
    return "fallback fragments=" + p.getFragmentIds();
  }
}
