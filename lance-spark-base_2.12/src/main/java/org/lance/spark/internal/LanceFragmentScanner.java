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

import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private final LanceScanner scanner;
  private final int fragmentId;
  private final boolean withFragemtId;
  private final LanceInputPartition inputPartition;

  private LanceFragmentScanner(
      LanceScanner scanner,
      int fragmentId,
      boolean withFragmentId,
      LanceInputPartition inputPartition) {
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.withFragemtId = withFragmentId;
    this.inputPartition = inputPartition;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
      Fragment fragment =
          LanceDatasetCache.getFragment(
              readOptions,
              fragmentId,
              inputPartition.getInitialStorageOptions(),
              inputPartition.getNamespaceImpl(),
              inputPartition.getNamespaceProperties());
      ScanOptions.Builder scanOptions = new ScanOptions.Builder();
      scanOptions.columns(getColumnNames(inputPartition.getSchema()));
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptions.filter(inputPartition.getWhereCondition().get());
      }
      scanOptions.batchSize(readOptions.getBatchSize());
      if (readOptions.getNearest() != null) {
        scanOptions.nearest(readOptions.getNearest());
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
          fragment.newScan(scanOptions.build()), fragmentId, withFragmentId, inputPartition);
    } catch (Throwable throwable) {
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
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
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
