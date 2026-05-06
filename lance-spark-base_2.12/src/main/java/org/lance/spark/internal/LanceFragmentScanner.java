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
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private final Dataset dataset;
  private final LanceScanner scanner;
  private final int fragmentId;
  private final boolean withFragemtId;
  private final LanceInputPartition inputPartition;

  private LanceFragmentScanner(
      Dataset dataset,
      LanceScanner scanner,
      int fragmentId,
      boolean withFragmentId,
      LanceInputPartition inputPartition) {
    this.dataset = dataset;
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.withFragemtId = withFragmentId;
    this.inputPartition = inputPartition;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    Dataset dataset = null;
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
      if (inputPartition.getNamespaceImpl() != null) {
        if (LanceRuntime.useNamespaceOnWorkers(inputPartition.getNamespaceImpl())) {
          readOptions.setNamespace(
              LanceRuntime.getOrCreateNamespace(
                  inputPartition.getNamespaceImpl(), inputPartition.getNamespaceProperties()));
        } else {
          readOptions.setNamespace(null);
        }
      }
      dataset =
          Utils.openDatasetBuilder(readOptions)
              .initialStorageOptions(inputPartition.getInitialStorageOptions())
              .build();
      Fragment fragment = dataset.getFragment(fragmentId);
      if (fragment == null) {
        throw new IllegalStateException(
            String.format(
                "Fragment %d not found in dataset at %s (version=%s)",
                fragmentId, readOptions.getDatasetUri(), readOptions.getVersion()));
      }
      ScanOptions.Builder scanOptions = new ScanOptions.Builder();
      List<String> projectedColumns = getColumnNames(inputPartition.getSchema());
      if (projectedColumns.isEmpty() && inputPartition.getSchema().isEmpty()) {
        // Lance requires at least one projected column. Use _rowid as a lightweight
        // sentinel so the scanner still returns the correct row count (e.g. SELECT 1).
        // Only do this when the schema is truly empty; when the schema contains virtual
        // columns (e.g. _fragid, blob position/size) that are not passed to the scanner
        // but added later by the batch scanner, adding _rowid here would shift column
        // indices and cause Spark to read wrong data.
        scanOptions.withRowId(true);
      }
      scanOptions.columns(projectedColumns);
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptions.filter(inputPartition.getWhereCondition().get());
      }
      scanOptions.batchSize(readOptions.getBatchSize());
      if (readOptions.getNearest() != null) {
        scanOptions.nearest(readOptions.getNearest());
        // We strictly set `prefilter = true` here to ensure query correctness.
        // This is necessary due to the combination of two factors:
        // 1. Spark currently performs the vector search by individually scanning each fragment.
        // 2. Lance mandates that `prefilter` must be enabled for fragmented vector queries.
        // If Spark's execution model or Lance's search functionality changes in the future,
        // we need to revisit this.
        scanOptions.prefilter(true);
      }
      if (inputPartition.getLimit().isPresent()) {
        scanOptions.limit(inputPartition.getLimit().get());
      }
      if (inputPartition.getOffset().isPresent()) {
        scanOptions.offset(inputPartition.getOffset().get());
      }
      if (inputPartition.getTopNSortOrders().isPresent()) {
        scanOptions.setColumnOrderings(inputPartition.getTopNSortOrders().get());
      }
      boolean withFragmentId =
          inputPartition.getSchema().getFieldIndex(LanceConstant.FRAGMENT_ID).nonEmpty();
      return new LanceFragmentScanner(
          dataset,
          fragment.newScan(scanOptions.build()),
          fragmentId,
          withFragmentId,
          inputPartition);
    } catch (Throwable throwable) {
      if (dataset != null) {
        try {
          dataset.close();
        } catch (Throwable closeError) {
          throwable.addSuppressed(closeError);
        }
      }
      throw new RuntimeException(throwable);
    }
  }

  /**
   * @return the arrow reader. The caller is responsible for closing the reader
   */
  public ArrowReader getArrowReader() {
    return scanner.scanBatches();
  }

  @Override
  public void close() throws IOException {
    Throwable primary = null;
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Throwable t) {
        primary = t;
      }
    }
    if (dataset != null) {
      try {
        dataset.close();
      } catch (Throwable t) {
        if (primary != null) {
          primary.addSuppressed(t);
        } else {
          primary = t;
        }
      }
    }
    if (primary != null) {
      if (primary instanceof IOException) {
        throw (IOException) primary;
      }
      if (primary instanceof RuntimeException) {
        throw (RuntimeException) primary;
      }
      if (primary instanceof Error) {
        throw (Error) primary;
      }
      throw new IOException(primary);
    }
  }

  public int fragmentId() {
    return fragmentId;
  }

  public boolean withFragemtId() {
    return withFragemtId;
  }

  public LanceInputPartition getInputPartition() {
    return inputPartition;
  }

  /**
   * Builds the projection column list for the scanner. Regular data columns come first, followed by
   * special metadata columns in the order matching {@link
   * org.lance.spark.LanceDataset#METADATA_COLUMNS}. All special columns (_rowid, _rowaddr, version
   * columns) go through scanner.project() for consistent output ordering.
   */
  private static List<String> getColumnNames(StructType schema) {
    // Collect all field names in the schema for quick lookup
    java.util.Set<String> schemaFields = new java.util.HashSet<>();
    for (StructField field : schema.fields()) {
      schemaFields.add(field.name());
    }

    // Regular data columns (exclude all special/metadata columns)
    List<String> columns =
        Arrays.stream(schema.fields())
            .map(StructField::name)
            .filter(
                name ->
                    !name.equals(LanceConstant.FRAGMENT_ID)
                        && !name.equals(LanceConstant.ROW_ID)
                        && !name.equals(LanceConstant.ROW_ADDRESS)
                        && !name.equals(LanceConstant.ROW_CREATED_AT_VERSION)
                        && !name.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
                        && !name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
                        && !name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX))
            .collect(Collectors.toList());

    // Append special columns in METADATA_COLUMNS order (must match Rust scanner output order)
    if (schemaFields.contains(LanceConstant.ROW_ID)) {
      columns.add(LanceConstant.ROW_ID);
    }
    if (schemaFields.contains(LanceConstant.ROW_ADDRESS)) {
      columns.add(LanceConstant.ROW_ADDRESS);
    }
    if (schemaFields.contains(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_LAST_UPDATED_AT_VERSION);
    }
    if (schemaFields.contains(LanceConstant.ROW_CREATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_CREATED_AT_VERSION);
    }

    return columns;
  }
}
