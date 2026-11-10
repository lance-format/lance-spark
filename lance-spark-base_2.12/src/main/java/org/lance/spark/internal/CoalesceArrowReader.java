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

import org.lance.Dataset;
import org.lance.spark.LanceConstant;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Streaming {@link ArrowReader} that reads initial (non-fallback) columns directly from a fragment
 * scan and populates {@code COALESCE(primary, fallback)} fallback columns on demand via {@code
 * dataset.takeRows} only when the corresponding primary column contains {@code NULL}s.
 */
public final class CoalesceArrowReader extends ArrowReader {
  private final Dataset dataset;
  private final ArrowReader delegate;
  private final List<String> columns;
  private final List<String> initialColumns;
  private final Map<String, String> activeCoalesce;

  CoalesceArrowReader(
      Dataset dataset,
      ArrowReader delegate,
      List<String> columns,
      Map<String, String> coalesceColumns) {
    super(dataset.allocator());
    this.dataset = dataset;
    this.delegate = delegate;
    this.columns = new ArrayList<>(columns);
    this.activeCoalesce = resolveActiveCoalesce(this.columns, coalesceColumns);
    this.initialColumns = computeInitialColumns(this.columns, this.activeCoalesce);
  }

  public static Map<String, String> resolveActiveCoalesce(
      List<String> columns, Map<String, String> coalesceColumns) {
    Map<String, String> validCoalesce = new LinkedHashMap<>();
    Set<String> columnSet = new HashSet<>(columns);
    if (coalesceColumns != null && !coalesceColumns.isEmpty()) {
      for (Map.Entry<String, String> entry : coalesceColumns.entrySet()) {
        String primary = entry.getKey();
        String fallback = entry.getValue();
        if (primary != null
            && fallback != null
            && !primary.equals(fallback)
            && columnSet.contains(primary)
            && columnSet.contains(fallback)
            && !validCoalesce.containsValue(primary)
            && !validCoalesce.containsKey(fallback)) {
          validCoalesce.put(primary, fallback);
        }
      }
    }
    return Collections.unmodifiableMap(validCoalesce);
  }

  static List<String> computeInitialColumns(
      List<String> columns, Map<String, String> activeCoalesce) {
    Set<String> fallbackSet = new HashSet<>(activeCoalesce.values());
    return columns.stream().filter(c -> !fallbackSet.contains(c)).collect(Collectors.toList());
  }

  @Override
  protected Schema readSchema() {
    return new Schema(
        columns.stream()
            .map(name -> dataset.getSchema().findField(name))
            .collect(Collectors.toList()));
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    if (!delegate.loadNextBatch()) {
      return false;
    }
    VectorSchemaRoot srcRoot = delegate.getVectorSchemaRoot();
    int rowCount = srcRoot.getRowCount();
    VectorSchemaRoot outRoot = getVectorSchemaRoot();

    List<Field> initFields = new ArrayList<>(initialColumns.size());
    List<FieldVector> srcVectors = new ArrayList<>(initialColumns.size());
    List<FieldVector> dstVectors = new ArrayList<>(initialColumns.size());
    for (String col : initialColumns) {
      FieldVector srcVec = srcRoot.getVector(col);
      FieldVector dstVec = outRoot.getVector(col);
      initFields.add(dstVec.getField());
      srcVectors.add(srcVec);
      dstVectors.add(dstVec);
    }
    try (ArrowRecordBatch batch =
        new VectorUnloader(new VectorSchemaRoot(initFields, srcVectors, rowCount))
            .getRecordBatch()) {
      new VectorLoader(new VectorSchemaRoot(initFields, dstVectors, 0)).load(batch);
    }

    UInt8Vector rowIdVec = (UInt8Vector) srcRoot.getVector(LanceConstant.ROW_ID);
    populateFallbacksFromRowIdVector(dataset, outRoot, activeCoalesce, rowIdVec, rowCount);
    outRoot.setRowCount(rowCount);
    return true;
  }

  static void populateFallbacksFromRowIdVector(
      Dataset dataset,
      VectorSchemaRoot outRoot,
      Map<String, String> activeCoalesce,
      UInt8Vector rowIdVec,
      int rowCount)
      throws IOException {
    Map<String, List<FieldVector>> fallbackToPrimaryVecs = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : activeCoalesce.entrySet()) {
      FieldVector primaryVec = outRoot.getVector(entry.getKey());
      fallbackToPrimaryVecs
          .computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
          .add(primaryVec);
    }

    List<String> fullNullFallbacks = null;
    for (Map.Entry<String, List<FieldVector>> entry : fallbackToPrimaryVecs.entrySet()) {
      String fallbackCol = entry.getKey();
      List<FieldVector> primaryVecs = entry.getValue();
      FieldVector fallbackVec = outRoot.getVector(fallbackCol);
      if (primaryVecs.size() == 1) {
        FieldVector primaryVec = primaryVecs.get(0);
        int nullCount = primaryVec.getNullCount();
        if (nullCount == 0) {
          fallbackVec.reset();
          fallbackVec.setValueCount(rowCount);
        } else if (nullCount == rowCount && rowCount > 0) {
          if (fullNullFallbacks == null) {
            fullNullFallbacks = new ArrayList<>(fallbackToPrimaryVecs.size());
          }
          fullNullFallbacks.add(fallbackCol);
        } else {
          List<Long> nullRowIds = new ArrayList<>(nullCount);
          int[] nullIndices = new int[nullCount];
          int k = 0;
          for (int i = 0; i < rowCount; i++) {
            if (primaryVec.isNull(i)) {
              nullRowIds.add(rowIdVec.get(i));
              nullIndices[k++] = i;
            }
          }
          fetchPartialFallback(
              dataset, fallbackCol, fallbackVec, nullRowIds, nullIndices, rowCount);
        }
      } else {
        boolean allNonNull = true;
        boolean anyFullNull = false;
        for (FieldVector primaryVec : primaryVecs) {
          int nullCount = primaryVec.getNullCount();
          if (nullCount > 0) {
            allNonNull = false;
          }
          if (nullCount == rowCount && rowCount > 0) {
            anyFullNull = true;
            break;
          }
        }
        if (allNonNull) {
          fallbackVec.reset();
          fallbackVec.setValueCount(rowCount);
        } else if (anyFullNull) {
          if (fullNullFallbacks == null) {
            fullNullFallbacks = new ArrayList<>(fallbackToPrimaryVecs.size());
          }
          fullNullFallbacks.add(fallbackCol);
        } else {
          List<Long> nullRowIds = new ArrayList<>(rowCount);
          int[] nullIndices = new int[rowCount];
          int k = 0;
          for (int i = 0; i < rowCount; i++) {
            boolean rowNeedsFallback = false;
            for (FieldVector primaryVec : primaryVecs) {
              if (primaryVec.isNull(i)) {
                rowNeedsFallback = true;
                break;
              }
            }
            if (rowNeedsFallback) {
              nullRowIds.add(rowIdVec.get(i));
              nullIndices[k++] = i;
            }
          }
          if (k == 0) {
            fallbackVec.reset();
            fallbackVec.setValueCount(rowCount);
          } else if (k == rowCount) {
            if (fullNullFallbacks == null) {
              fullNullFallbacks = new ArrayList<>(fallbackToPrimaryVecs.size());
            }
            fullNullFallbacks.add(fallbackCol);
          } else {
            fetchPartialFallback(
                dataset, fallbackCol, fallbackVec, nullRowIds, nullIndices, rowCount);
          }
        }
      }
    }
    if (fullNullFallbacks != null && !fullNullFallbacks.isEmpty()) {
      List<Long> allRowIds = new ArrayList<>(rowCount);
      for (int i = 0; i < rowCount; i++) {
        allRowIds.add(rowIdVec.get(i));
      }
      fetchFullNullFallbacks(dataset, outRoot, fullNullFallbacks, allRowIds, rowCount);
    }
  }

  private static void fetchFullNullFallbacks(
      Dataset dataset,
      VectorSchemaRoot outRoot,
      List<String> fullNullFallbacks,
      List<Long> rowIds,
      int rowCount)
      throws IOException {
    int offset = 0;
    boolean allocatedForCopy = false;
    try (ArrowReader fbReader = dataset.takeRows(rowIds, fullNullFallbacks)) {
      while (fbReader.loadNextBatch()) {
        VectorSchemaRoot fbRoot = fbReader.getVectorSchemaRoot();
        int batchRows = fbRoot.getRowCount();
        if (offset == 0 && batchRows == rowCount) {
          List<Field> fbFields = new ArrayList<>(fullNullFallbacks.size());
          List<FieldVector> dstVectors = new ArrayList<>(fullNullFallbacks.size());
          for (String fbCol : fullNullFallbacks) {
            FieldVector dstVec = outRoot.getVector(fbCol);
            dstVec.reset();
            fbFields.add(dstVec.getField());
            dstVectors.add(dstVec);
          }
          try (ArrowRecordBatch batch = new VectorUnloader(fbRoot).getRecordBatch()) {
            new VectorLoader(new VectorSchemaRoot(fbFields, dstVectors, 0)).load(batch);
          }
          return;
        }
        if (!allocatedForCopy) {
          for (String fbCol : fullNullFallbacks) {
            FieldVector fbVec = outRoot.getVector(fbCol);
            fbVec.reset();
            fbVec.allocateNew();
          }
          allocatedForCopy = true;
        }
        for (String fbCol : fullNullFallbacks) {
          FieldVector srcVec = fbRoot.getVector(fbCol);
          FieldVector dstVec = outRoot.getVector(fbCol);
          for (int j = 0; j < batchRows; j++) {
            if (!srcVec.isNull(j)) {
              dstVec.copyFromSafe(j, offset + j, srcVec);
            }
          }
        }
        offset += batchRows;
      }
    }
    if (!allocatedForCopy) {
      for (String fbCol : fullNullFallbacks) {
        FieldVector fbVec = outRoot.getVector(fbCol);
        fbVec.reset();
        fbVec.allocateNew();
      }
    }
    for (String fbCol : fullNullFallbacks) {
      outRoot.getVector(fbCol).setValueCount(rowCount);
    }
  }

  private static void fetchPartialFallback(
      Dataset dataset,
      String fallbackCol,
      FieldVector fallbackVec,
      List<Long> nullRowIds,
      int[] nullIndices,
      int rowCount)
      throws IOException {
    fallbackVec.reset();
    fallbackVec.allocateNew();
    int fetchedIdx = 0;
    try (ArrowReader fbReader =
        dataset.takeRows(nullRowIds, Collections.singletonList(fallbackCol))) {
      while (fbReader.loadNextBatch()) {
        FieldVector srcVec = fbReader.getVectorSchemaRoot().getVector(fallbackCol);
        int srcCount = fbReader.getVectorSchemaRoot().getRowCount();
        for (int j = 0; j < srcCount; j++) {
          if (!srcVec.isNull(j)) {
            fallbackVec.copyFromSafe(j, nullIndices[fetchedIdx], srcVec);
          }
          fetchedIdx++;
        }
      }
    }
    fallbackVec.setValueCount(rowCount);
  }

  @Override
  public long bytesRead() {
    return delegate.bytesRead();
  }

  @Override
  protected void closeReadSource() throws IOException {
    delegate.close();
  }
}
