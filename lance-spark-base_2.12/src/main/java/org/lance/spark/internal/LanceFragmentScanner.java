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
import org.lance.fragment.DataFile;
import org.lance.ipc.ColumnOrdering;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.ipc.ScanStats;
import org.lance.schema.LanceField;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private final Dataset dataset;
  private final LanceScanner scanner;
  private final int fragmentId;
  private final LanceInputPartition inputPartition;
  private final long datasetOpenTimeNs;
  private final long scannerCreateTimeNs;

  /** The names of blob columns in the projected schema. */
  private final Set<String> blobColumnNames;

  /**
   * Projected schema columns that are physically absent from all data files of this fragment and
   * omitted from the native scan so the reader can materialize them as null {@code
   * ConstantColumnVector}s without native allocation.
   */
  private final Set<String> missingColumnNames;

  private LanceFragmentScanner(
      Dataset dataset,
      LanceScanner scanner,
      int fragmentId,
      LanceInputPartition inputPartition,
      long datasetOpenTimeNs,
      long scannerCreateTimeNs,
      Set<String> blobColumnNames,
      Set<String> missingColumnNames) {
    this.dataset = dataset;
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.inputPartition = inputPartition;
    this.datasetOpenTimeNs = datasetOpenTimeNs;
    this.scannerCreateTimeNs = scannerCreateTimeNs;
    this.blobColumnNames = blobColumnNames;
    this.missingColumnNames = missingColumnNames;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    return create(fragmentId, inputPartition, false);
  }

  public static LanceFragmentScanner create(
      int fragmentId, LanceInputPartition inputPartition, boolean omitMissingColumns) {
    Dataset dataset = null;
    LanceScanner lanceScanner = null;
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
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
                fragmentId, readOptions.getDatasetUri(), readOptions.getRef()));
      }
      StructType scanSchema = inputPartition.getSchema();
      Set<String> blobColumnNames = getBlobColumnNames(scanSchema);
      Set<String> missingColumnNames =
          omitMissingColumns
              ? findMissingProjectedColumns(dataset, fragment, inputPartition, blobColumnNames)
              : Collections.emptySet();
      ScanOptions builtScanOptions =
          buildScanOptions(inputPartition, blobColumnNames, missingColumnNames);

      long scanCreateStart = System.nanoTime();
      lanceScanner = fragment.newScan(builtScanOptions);
      long scanCreateTimeNs = System.nanoTime() - scanCreateStart;
      return new LanceFragmentScanner(
          dataset,
          lanceScanner,
          fragmentId,
          inputPartition,
          dsOpenTimeNs,
          scanCreateTimeNs,
          blobColumnNames,
          missingColumnNames);
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

  /**
   * Exports this fragment scan into a caller-owned Arrow C Data Interface stream. The Lance native
   * side populates the {@code ArrowArrayStream} at {@code streamAddress} directly, so only the
   * C-struct address crosses the JVM/native boundary. The caller owns the stream and must close it
   * (which releases the native scan via the stream's release callback); the scanner and dataset
   * held by this object are released separately by {@link #close()}.
   *
   * @param streamAddress the memory address of a freshly-allocated, empty {@code ArrowArrayStream}
   */
  public void exportArrowStream(long streamAddress) throws IOException {
    scanner.exportArrowStream(streamAddress);
  }

  /**
   * @return the Arrow schema the native scan produces, including row IDs or addresses requested
   *     internally for empty projections and blob references
   */
  public Schema schema() {
    return scanner.schema();
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

  public LanceInputPartition getInputPartition() {
    return inputPartition;
  }

  public long getDatasetOpenTimeNs() {
    return datasetOpenTimeNs;
  }

  public long getScannerCreateTimeNs() {
    return scannerCreateTimeNs;
  }

  /** Returns the blob column names in the projected schema. */
  public Set<String> getBlobColumnNames() {
    return blobColumnNames;
  }

  /**
   * Returns whether {@code columnName} was physically absent from all data files of this fragment
   * and omitted from the native scan projection.
   */
  public boolean isMissingColumn(String columnName) {
    return missingColumnNames.contains(columnName);
  }

  /** Returns the dataset URI for blob references. */
  public String getDatasetUri() {
    return inputPartition.getReadOptions().getDatasetUri();
  }

  static ScanOptions buildScanOptions(
      LanceInputPartition inputPartition,
      Set<String> blobColumnNames,
      Set<String> missingColumnNames) {
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    StructType scanSchema = inputPartition.getSchema();
    boolean hasBlobColumns = !blobColumnNames.isEmpty();

    ScanOptions.Builder scanOptions = new ScanOptions.Builder();
    List<String> projectedColumns = getColumnNames(scanSchema);
    if (!missingColumnNames.isEmpty()) {
      projectedColumns.removeIf(missingColumnNames::contains);
    }
    if (readOptions.getFullTextQuery() != null
        && Arrays.stream(scanSchema.fields()).anyMatch(Utils::isScoreMetadataColumn)) {
      projectedColumns.add(LanceConstant.SCORE);
    }

    // Request _rowaddr when blob columns are present so we can build blob references.
    boolean userRequestedRowId = hasField(scanSchema, LanceConstant.ROW_ID);
    boolean userRequestedRowAddr = hasField(scanSchema, LanceConstant.ROW_ADDRESS);
    boolean needsRowAddr = hasBlobColumns || userRequestedRowAddr;
    if (needsRowAddr) {
      scanOptions.withRowAddress(true);
    }
    if (userRequestedRowId || (projectedColumns.isEmpty() && !needsRowAddr)) {
      scanOptions.withRowId(true);
    }

    scanOptions.columns(projectedColumns);
    if (inputPartition.getWhereCondition().isPresent()) {
      scanOptions.filter(inputPartition.getWhereCondition().get());
    }
    scanOptions.batchSize(readOptions.getBatchSize());
    if (readOptions.getFullTextQuery() != null) {
      scanOptions.fullTextQuery(readOptions.getFullTextQuery());
      scanOptions.prefilter(true);
      scanOptions.disableScoringAutoprojection(true);
    }
    scanOptions.useScalarIndex(readOptions.isUseScalarIndex());
    if (inputPartition.getLimit().isPresent()) {
      scanOptions.limit(inputPartition.getLimit().get());
    }
    if (inputPartition.getOffset().isPresent()) {
      scanOptions.offset(inputPartition.getOffset().get());
    }
    if (inputPartition.getTopNSortOrders().isPresent()) {
      scanOptions.setColumnOrderings(inputPartition.getTopNSortOrders().get());
    }

    // Collect scan stats
    scanOptions.collectStats(true);
    return scanOptions.build();
  }

  static Set<String> findMissingProjectedColumns(
      Dataset dataset,
      Fragment fragment,
      LanceInputPartition inputPartition,
      Set<String> blobColumnNames) {
    List<DataFile> dataFiles = fragment.metadata().getFiles();
    if (dataFiles == null || dataFiles.isEmpty()) {
      return Collections.emptySet();
    }
    Set<Integer> fragmentFieldIds = new HashSet<>();
    for (DataFile dataFile : dataFiles) {
      int[] fields = dataFile.getFields();
      if (fields != null) {
        for (int fieldId : fields) {
          if (fieldId >= 0) {
            fragmentFieldIds.add(fieldId);
          }
        }
      }
    }

    StructType scanSchema = inputPartition.getSchema();
    Set<String> excludedColumns = new HashSet<>(blobColumnNames);
    for (StructField field : scanSchema.fields()) {
      String blobBase = BlobUtils.blobV1VirtualColumnBaseName(field.name());
      if (blobBase != null) {
        excludedColumns.add(blobBase);
      }
    }
    if (inputPartition.getTopNSortOrders().isPresent()) {
      for (ColumnOrdering ordering : inputPartition.getTopNSortOrders().get()) {
        excludedColumns.add(ordering.getColumnName());
      }
    }

    Map<String, LanceField> topLevelLanceFields = new HashMap<>();
    for (LanceField lanceField : dataset.getLanceSchema().fields()) {
      topLevelLanceFields.put(lanceField.getName(), lanceField);
    }

    Set<String> missing = new HashSet<>();
    for (StructField sparkField : scanSchema.fields()) {
      String name = sparkField.name();
      if (!sparkField.nullable()
          || excludedColumns.contains(name)
          || Utils.isScoreMetadataColumn(sparkField)
          || name.equals(LanceConstant.FRAGMENT_ID)
          || name.equals(LanceConstant.ROW_ID)
          || name.equals(LanceConstant.ROW_ADDRESS)
          || name.equals(LanceConstant.ROW_CREATED_AT_VERSION)
          || name.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
          || name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
          || name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX)) {
        continue;
      }
      LanceField lanceField = topLevelLanceFields.get(name);
      if (lanceField != null
          && lanceField.isNullable()
          && !containsAnyFieldIdInSubtree(lanceField, fragmentFieldIds)) {
        missing.add(name);
      }
    }
    return missing;
  }

  private static boolean containsAnyFieldIdInSubtree(
      LanceField field, Set<Integer> fragmentFieldIds) {
    if (fragmentFieldIds.contains(field.getId())) {
      return true;
    }
    for (LanceField child : field.getChildren()) {
      if (containsAnyFieldIdInSubtree(child, fragmentFieldIds)) {
        return true;
      }
    }
    return false;
  }

  private static Set<String> getBlobColumnNames(StructType schema) {
    Set<String> blobColumns = new HashSet<>();
    for (StructField field : schema.fields()) {
      if (BlobUtils.isBlobReadColumn(field)) {
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
            .filter(field -> !Utils.isScoreMetadataColumn(field))
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
    // Blob v1 virtual columns are derived from their base blob column, so scan the base column even
    // when the query only projects the virtual columns.
    for (StructField field : schema.fields()) {
      String baseName = BlobUtils.blobV1VirtualColumnBaseName(field.name());
      if (baseName != null && !columns.contains(baseName)) {
        columns.add(baseName);
      }
    }
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

  public Optional<ScanStats> getScanStats() {
    return scanner == null ? Optional.empty() : scanner.getStats();
  }
}
