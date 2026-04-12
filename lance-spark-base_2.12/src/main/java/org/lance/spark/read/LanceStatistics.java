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

import org.apache.spark.sql.connector.read.Statistics;

import java.io.Serializable;
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
   * Create statistics from a ManifestSummary.
   *
   * @param summary the manifest summary containing pre-computed statistics
   */
  public LanceStatistics(ManifestSummary summary) {
    this(summary.getTotalRows(), summary.getTotalFilesSize());
  }

  /** Create statistics with explicit values (e.g., after scaling for pruned fragments). */
  public LanceStatistics(long numRows, long sizeInBytes) {
    this.numRows = numRows;
    this.sizeInBytes = sizeInBytes;
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

  @Override
  public OptionalLong sizeInBytes() {
    return OptionalLong.of(sizeInBytes);
  }

  @Override
  public OptionalLong numRows() {
    return OptionalLong.of(numRows);
  }
}
