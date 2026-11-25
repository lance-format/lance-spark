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
import org.lance.ipc.ScanStats;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.FragmentSplit;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LanceFragmentScanner implements AutoCloseable {
  private final Dataset dataset;
  private final LanceScanner scanner;
  private final int fragmentId;
  private final LanceInputPartition inputPartition;
  private final long datasetOpenTimeNs;
  private final long scannerCreateTimeNs;

  /** The names of blob columns in the projected schema. */
  private final Set<String> blobColumnNames;

  private LanceFragmentScanner(
      Dataset dataset,
      LanceScanner scanner,
      int fragmentId,
      LanceInputPartition inputPartition,
      long datasetOpenTimeNs,
      long scannerCreateTimeNs,
      Set<String> blobColumnNames) {
    this.dataset = dataset;
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.inputPartition = inputPartition;
    this.datasetOpenTimeNs = datasetOpenTimeNs;
    this.scannerCreateTimeNs = scannerCreateTimeNs;
    this.blobColumnNames = blobColumnNames;
  }

  /** Opens the given fragment with its nonempty list of planned splits. */
  public static LanceFragmentScanner create(
      int fragmentId, List<FragmentSplit> fragmentSplits, LanceInputPartition inputPartition) {
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
      ScanOptions scanOptions = inputPartition.fragmentScanOptions(fragmentSplits);
      Set<String> blobColumnNames = getBlobColumnNames(inputPartition.getSchema());

      long scanCreateStart = System.nanoTime();
      lanceScanner = fragment.newScan(scanOptions);
      long scanCreateTimeNs = System.nanoTime() - scanCreateStart;
      return new LanceFragmentScanner(
          dataset,
          lanceScanner,
          fragmentId,
          inputPartition,
          dsOpenTimeNs,
          scanCreateTimeNs,
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

  /** Returns the dataset URI for blob references. */
  public String getDatasetUri() {
    return inputPartition.getReadOptions().getDatasetUri();
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

  public Optional<ScanStats> getScanStats() {
    return scanner == null ? Optional.empty() : scanner.getStats();
  }
}
