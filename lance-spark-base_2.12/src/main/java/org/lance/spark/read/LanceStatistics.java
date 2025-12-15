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
    this.numRows = summary.getTotalRows();
    this.sizeInBytes = summary.getTotalFilesSize();
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
