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
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private final Dataset dataset;
  private final LanceScanner scanner;
  private final int fragmentId;
  private final boolean withFragmentId;
  private final LanceInputPartition inputPartition;
  private final long datasetOpenTimeNs;
  private final long scannerCreateTimeNs;

  /**
   * Whether the scanner requested _rowaddr for blob reference support. When true, the _rowaddr
   * column in the Arrow batch was implicitly added and should be stripped from user-visible output.
   */
  private final boolean withRowAddrForBlobs;

  /** The names of blob columns in the projected schema. */
  private final Set<String> blobColumnNames;

  private LanceFragmentScanner(
      Dataset dataset,
      LanceScanner scanner,
      int fragmentId,
      boolean withFragmentId,
      LanceInputPartition inputPartition,
      long datasetOpenTimeNs,
      long scannerCreateTimeNs,
      boolean withRowAddrForBlobs,
      Set<String> blobColumnNames) {
    this.dataset = dataset;
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.withFragmentId = withFragmentId;
    this.inputPartition = inputPartition;
    this.datasetOpenTimeNs = datasetOpenTimeNs;
    this.scannerCreateTimeNs = scannerCreateTimeNs;
    this.withRowAddrForBlobs = withRowAddrForBlobs;
    this.blobColumnNames = blobColumnNames;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    Dataset dataset = null;
    LanceScanner lanceScanner = null;
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
      if (inputPartition.getNamespaceImpl() != null && readOptions.isExecutorCredentialRefresh()) {
        if (LanceRuntime.useNamespaceOnWorkers(inputPartition.getNamespaceImpl())) {
          readOptions.setNamespace(
              LanceRuntime.getOrCreateNamespace(
                  inputPartition.getNamespaceImpl(), inputPartition.getNamespaceProperties()));
        } else {
          readOptions.setNamespace(null);
        }
      }
      long dsOpenStart = System.nanoTime();
      dataset =
          Utils.openDatasetBuilder(readOptions)
              .initialStorageOptions(inputPartition.getInitialStorageOptions())
              .build();
      long dsOpenTimeNs = System.nanoTime() - dsOpenStart;
      Fragment fragment = dataset.getFragment(fragmentId);
      if (fragment == null) {
        throw new IllegalStateException(
            String.format(
                "Fragment %d not found in dataset at %s (version=%s)",
                fragmentId, readOptions.getDatasetUri(), readOptions.getVersion()));
      }
      ScanOptions.Builder scanOptions = new ScanOptions.Builder();

      // Detect blob columns in the schema
      Set<String> blobColumnNames = getBlobColumnNames(inputPartition.getSchema());
      boolean hasBlobColumns = !blobColumnNames.isEmpty();

      List<String> projectedColumns = getColumnNames(inputPartition.getSchema());
      if (projectedColumns.isEmpty() && inputPartition.getSchema().isEmpty()) {
        scanOptions.withRowId(true);
      }
      if (hasField(inputPartition.getSchema(), LanceConstant.ROW_ID)) {
        scanOptions.withRowId(true);
      }

      // Request _rowaddr when blob columns are present so we can build blob references.
      boolean userRequestedRowAddr =
          hasField(inputPartition.getSchema(), LanceConstant.ROW_ADDRESS);
      boolean withRowAddrForBlobs = hasBlobColumns && !userRequestedRowAddr;
      if (hasBlobColumns || userRequestedRowAddr) {
        scanOptions.withRowAddress(true);
      }

      scanOptions.columns(projectedColumns);
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptions.filter(inputPartition.getWhereCondition().get());
      }
      scanOptions.batchSize(readOptions.getBatchSize());
      if (readOptions.getNearest() != null) {
        scanOptions.nearest(readOptions.getNearest());
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
      long scanCreateStart = System.nanoTime();
      lanceScanner = fragment.newScan(scanOptions.build());
      long scanCreateTimeNs = System.nanoTime() - scanCreateStart;
      return new LanceFragmentScanner(
          dataset,
          lanceScanner,
          fragmentId,
          withFragmentId,
          inputPartition,
          dsOpenTimeNs,
          scanCreateTimeNs,
          withRowAddrForBlobs,
          blobColumnNames);
    } catch (Throwable throwable) {
      if (lanceScanner != null) {
        try {
          lanceScanner.close();
        } catch (Throwable closeError) {
          throwable.addSuppressed(closeError);
        }
      }
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

  public boolean withFragmentId() {
    return withFragmentId;
  }

  public LanceInputPartition getInputPartition() {
    return inputPartition;
  }

  public long getDatasetOpenTimeNs() {
    return datasetOpenTimeNs;
  }

  public long getScannerCreateTimeNs() {
    return scannerCreateTimeNs;
  }

  /** Whether the scanner implicitly requested _rowaddr for blob reference support. */
  public boolean isWithRowAddrForBlobs() {
    return withRowAddrForBlobs;
  }

  /** Returns the blob column names in the projected schema. */
  public Set<String> getBlobColumnNames() {
    return blobColumnNames;
  }

  /** Returns the dataset URI for blob references. */
  public String getDatasetUri() {
    return inputPartition.getReadOptions().getDatasetUri();
  }

  private static Set<String> getBlobColumnNames(StructType schema) {
    Set<String> blobColumns = new HashSet<>();
    for (StructField field : schema.fields()) {
      if (BlobUtils.isBlobSparkField(field)) {
        blobColumns.add(field.name());
      }
    }
    return blobColumns;
  }

  private static List<String> getColumnNames(StructType schema) {
    java.util.Set<String> schemaFields = new java.util.HashSet<>();
    for (StructField field : schema.fields()) {
      schemaFields.add(field.name());
    }

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
    if (schemaFields.contains(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_LAST_UPDATED_AT_VERSION);
    }
    if (schemaFields.contains(LanceConstant.ROW_CREATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_CREATED_AT_VERSION);
    }

    return columns;
  }

  private static boolean hasField(StructType schema, String name) {
    for (StructField field : schema.fields()) {
      if (field.name().equals(name)) {
        return true;
      }
    }
    return false;
  }
}
