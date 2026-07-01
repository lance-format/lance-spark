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

/**
 * Per-column TBLPROPERTIES key suffixes used only by ANALYZE-stats tests to assemble and assert the
 * on-disk key shape. Production code never references these: the ANALYZE writer serializes each
 * column stat with Spark's {@code CatalogColumnStat.toMap} and the read path deserializes with
 * {@code CatalogColumnStat.fromMap}, so the suffix naming is Spark's own, prefixed with {@link
 * LanceStatsKeys#COLUMN_PREFIX}. These constants live in the test sources rather than in {@link
 * LanceStatsKeys} so the production wire-format class carries only the keys production actually
 * reads or writes.
 */
public final class LanceStatsTestKeys {

  private LanceStatsTestKeys() {}

  public static final String COLUMN_SUFFIX_VERSION = ".version";
  public static final String COLUMN_SUFFIX_MIN = ".min";
  public static final String COLUMN_SUFFIX_MAX = ".max";
  public static final String COLUMN_SUFFIX_NULL_COUNT = ".nullCount";
  public static final String COLUMN_SUFFIX_DISTINCT_COUNT = ".distinctCount";
  public static final String COLUMN_SUFFIX_AVG_LEN = ".avgLen";
  public static final String COLUMN_SUFFIX_MAX_LEN = ".maxLen";

  /**
   * The suffix Spark's {@code toMap} uses for a histogram. Lance ANALYZE never persists one (it
   * forces {@code spark.sql.statistics.histogram.enabled} off during computation), so tests use
   * this only to assert the histogram key's absence.
   */
  public static final String COLUMN_SUFFIX_HISTOGRAM = ".histogram";
}
