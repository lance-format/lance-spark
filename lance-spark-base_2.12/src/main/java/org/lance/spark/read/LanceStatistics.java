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
package org.lance.spark.read;

import org.lance.ManifestSummary;

import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Lance dataset statistics based on ManifestSummary. This provides O(1) access to pre-computed
 * statistics from the manifest metadata.
 */
public class LanceStatistics implements Statistics, Serializable {
  private static final long serialVersionUID = 1L;

  private final long numRows;
  private final long sizeInBytes;

  /**
   * Per-column stats reported via {@link Statistics#columnStats}. Always non-null but typically
   * empty when no zonemap-indexed projected column has data — kept as a defensive copy so callers
   * cannot mutate state visible to Spark's CBO.
   */
  private final Map<NamedReference, ColumnStatistics> columnStats;

  /**
   * Create statistics from a ManifestSummary.
   *
   * @param summary the manifest summary containing pre-computed statistics
   */
  public LanceStatistics(ManifestSummary summary) {
    this(summary.getTotalRows(), summary.getTotalFilesSize(), Collections.emptyMap());
  }

  /** Create statistics with explicit values (e.g., after scaling for pruned fragments). */
  public LanceStatistics(long numRows, long sizeInBytes) {
    this(numRows, sizeInBytes, Collections.emptyMap());
  }

  /** Create statistics with explicit values and per-column statistics for CBO. */
  public LanceStatistics(
      long numRows, long sizeInBytes, Map<NamedReference, ColumnStatistics> columnStats) {
    this.numRows = numRows;
    this.sizeInBytes = sizeInBytes;
    this.columnStats =
        columnStats == null || columnStats.isEmpty()
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new java.util.LinkedHashMap<>(columnStats));
  }

  /**
   * Estimate post-pruning statistics by scaling full-table stats by the ratio of surviving
   * fragments. This enables Spark's JoinSelection to pick BroadcastHashJoin when the post-pruning
   * size is below the broadcast threshold, rather than defaulting to SortMergeJoin + SPJ.
   *
   * @param totalRows total rows in the dataset
   * @param totalFilesSize total file size in bytes
   * @param totalFragments total number of fragments in the dataset
   * @param survivingFragments number of fragments that survive zonemap pruning
   * @return scaled statistics, or full-table statistics if scaling is not applicable
   */
  public static LanceStatistics estimatePostPruning(
      long totalRows, long totalFilesSize, long totalFragments, int survivingFragments) {
    if (totalFragments <= 0 || survivingFragments >= totalFragments) {
      return new LanceStatistics(totalRows, totalFilesSize);
    }
    double ratio = (double) survivingFragments / totalFragments;
    return new LanceStatistics((long) (totalRows * ratio), (long) (totalFilesSize * ratio));
  }

  /**
   * Estimate post-projection size using {@code sizeInBytes × (projectedWidths / fullWidths)}, the
   * same formula Spark's DSv2 {@code FileScan.estimateStatistics} applies after column pruning (see
   * {@code org.apache.spark.sql.execution.datasources.v2.FileScan}). Lets {@code JoinSelection} see
   * a projection-aware size so broadcast decisions line up with what DSv1 Parquet produces through
   * the optimizer's {@code Project.computeStats()} path — within the 8-byte-per-row overhead DSv1
   * adds via {@code EstimationUtils.getSizePerRow}, which rounds estimates apart by a few percent
   * but doesn't shift broadcast thresholds in practice.
   *
   * <p>Width-weighted (not {@code numRows × sumOfWidths}) on purpose: the ratio preserves the
   * compression baked into {@code fullSizeInBytes}, whereas a row-count formula would ignore
   * on-disk encoding and systematically over-count columnar sources.
   *
   * @param numRows projected row count (post fragment-pruning if any)
   * @param fullSizeInBytes on-disk size of the full (pre-projection) dataset in bytes
   * @param fullSchema schema of the full dataset before column pruning
   * @param projectedSchema pruned schema containing only columns actually read by the scan
   * @return statistics with {@code sizeInBytes} scaled by width-weighted column ratio
   */
  public static LanceStatistics estimateProjected(
      long numRows, long fullSizeInBytes, StructType fullSchema, StructType projectedSchema) {
    return estimateProjected(
        numRows, fullSizeInBytes, fullSchema, projectedSchema, Collections.emptyMap());
  }

  /**
   * Same as {@link #estimateProjected(long, long, StructType, StructType)} but additionally carries
   * an aggregated {@code columnStats} map so Spark's CBO can read per-column min/max/null counts
   * via {@link Statistics#columnStats()}. Map keys must be top-level {@link NamedReference}s
   * matching the projected schema's field names; entries for non-projected columns are dropped
   * silently (Spark would ignore them anyway).
   */
  public static LanceStatistics estimateProjected(
      long numRows,
      long fullSizeInBytes,
      StructType fullSchema,
      StructType projectedSchema,
      Map<NamedReference, ColumnStatistics> columnStats) {
    long projWidth = sumWidths(projectedSchema);
    long fullWidth = sumWidths(fullSchema);
    long sizeInBytes;
    if (fullWidth <= 0 || projWidth <= 0 || projWidth >= fullWidth) {
      sizeInBytes = fullSizeInBytes;
    } else {
      // Promote to double to avoid long overflow in fullSizeInBytes * projWidth.
      sizeInBytes = (long) ((double) fullSizeInBytes * projWidth / fullWidth);
    }
    // Clamp to 1: integer truncation can round very small scaled sizes to 0, which
    // JoinSelection reads as "below threshold" and would unintentionally force a broadcast.
    return new LanceStatistics(numRows, Math.max(sizeInBytes, 1L), columnStats);
  }

  private static long sumWidths(StructType schema) {
    if (schema == null) {
      return 0;
    }
    long sum = 0;
    for (StructField field : schema.fields()) {
      sum += field.dataType().defaultSize();
    }
    return sum;
  }

  @Override
  public OptionalLong sizeInBytes() {
    return OptionalLong.of(sizeInBytes);
  }

  @Override
  public OptionalLong numRows() {
    return OptionalLong.of(numRows);
  }

  @Override
  public Map<NamedReference, ColumnStatistics> columnStats() {
    return columnStats;
  }
}
