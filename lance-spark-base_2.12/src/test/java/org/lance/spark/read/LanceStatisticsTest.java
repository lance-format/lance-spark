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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LanceStatisticsTest {

  @Test
  public void testExplicitConstructor() {
    LanceStatistics stats = new LanceStatistics(1000, 50000);
    assertEquals(1000, stats.numRows().getAsLong());
    assertEquals(50000, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimatePostPruningScalesCorrectly() {
    // 10 fragments, 3 survive → 30% of totals
    LanceStatistics stats = LanceStatistics.estimatePostPruning(1000, 10000, 10, 3);
    assertEquals(300, stats.numRows().getAsLong());
    assertEquals(3000, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimatePostPruningSingleSurviveOfMany() {
    // 100 fragments, 1 survives → 1% of totals
    LanceStatistics stats = LanceStatistics.estimatePostPruning(10000, 100000, 100, 1);
    assertEquals(100, stats.numRows().getAsLong());
    assertEquals(1000, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimatePostPruningAllSurvive() {
    // All fragments survive → full-table stats
    LanceStatistics stats = LanceStatistics.estimatePostPruning(1000, 50000, 10, 10);
    assertEquals(1000, stats.numRows().getAsLong());
    assertEquals(50000, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimatePostPruningMoreThanTotalSurvive() {
    // Edge case: surviving > total (shouldn't happen, but be safe) → full-table stats
    LanceStatistics stats = LanceStatistics.estimatePostPruning(1000, 50000, 10, 15);
    assertEquals(1000, stats.numRows().getAsLong());
    assertEquals(50000, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimatePostPruningZeroSurvive() {
    // Zero fragments survive → zero stats
    LanceStatistics stats = LanceStatistics.estimatePostPruning(1000, 50000, 10, 0);
    assertEquals(0, stats.numRows().getAsLong());
    assertEquals(0, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimatePostPruningZeroTotalFragments() {
    // Edge case: zero total fragments → returns full-table stats (guard against division by zero)
    LanceStatistics stats = LanceStatistics.estimatePostPruning(1000, 50000, 0, 0);
    assertEquals(1000, stats.numRows().getAsLong());
    assertEquals(50000, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimateProjectedScalesByColumnWidthRatio() {
    // Full schema has 9 columns; project 3 of equal width → size should scale by 3/9.
    // 24_000_000 full bytes × (3×8)/(9×8) = 8_000_000.
    StructType full =
        new StructType(
            new StructField[] {
              new StructField("c1", DataTypes.LongType, true, null),
              new StructField("c2", DataTypes.LongType, true, null),
              new StructField("c3", DataTypes.LongType, true, null),
              new StructField("c4", DataTypes.LongType, true, null),
              new StructField("c5", DataTypes.LongType, true, null),
              new StructField("c6", DataTypes.LongType, true, null),
              new StructField("c7", DataTypes.LongType, true, null),
              new StructField("c8", DataTypes.LongType, true, null),
              new StructField("c9", DataTypes.LongType, true, null)
            });
    StructType projected =
        new StructType(
            new StructField[] {
              new StructField("c1", DataTypes.LongType, true, null),
              new StructField("c2", DataTypes.LongType, true, null),
              new StructField("c3", DataTypes.LongType, true, null)
            });
    LanceStatistics stats = LanceStatistics.estimateProjected(1000, 24_000_000L, full, projected);
    assertEquals(1000, stats.numRows().getAsLong());
    assertEquals(8_000_000L, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimateProjectedFavorsWideColumns() {
    // Full schema: one String (width 20) + one Int (width 4). Total width 24.
    // Project just the String → ratio 20/24.
    // 24_000 full bytes × 20/24 = 20_000.
    StructType full =
        new StructType(
            new StructField[] {
              new StructField("name", DataTypes.StringType, true, null),
              new StructField("id", DataTypes.IntegerType, true, null)
            });
    StructType projected =
        new StructType(
            new StructField[] {new StructField("name", DataTypes.StringType, true, null)});
    LanceStatistics stats = LanceStatistics.estimateProjected(1000, 24_000L, full, projected);
    assertEquals(20_000L, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimateProjectedAllColumnsUnchanged() {
    // Projection == full schema → no scaling.
    StructType full =
        new StructType(new StructField[] {new StructField("a", DataTypes.LongType, true, null)});
    LanceStatistics stats = LanceStatistics.estimateProjected(1000, 50_000L, full, full);
    assertEquals(50_000L, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimateProjectedEmptyProjectionReturnsFullSize() {
    // Count-only scan: no columns projected. Fall back to full size rather than zero.
    StructType full =
        new StructType(new StructField[] {new StructField("a", DataTypes.LongType, true, null)});
    StructType projected = new StructType(new StructField[] {});
    LanceStatistics stats = LanceStatistics.estimateProjected(1000, 50_000L, full, projected);
    assertEquals(50_000L, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimateProjectedNeverZero() {
    StructType full =
        new StructType(new StructField[] {new StructField("a", DataTypes.LongType, true, null)});
    LanceStatistics stats = LanceStatistics.estimateProjected(0, 0L, full, full);
    assertEquals(1, stats.sizeInBytes().getAsLong());
  }

  @Test
  public void testEstimateProjectedTruncationClampsToOne() {
    // Very narrow projection of a tiny table truncates to 0 after (long) cast.
    // Clamp must restore it to 1 so JoinSelection doesn't read it as empty.
    StructType full =
        new StructType(
            new StructField[] {
              new StructField("key", DataTypes.LongType, true, null),
              new StructField("s1", DataTypes.StringType, true, null),
              new StructField("s2", DataTypes.StringType, true, null),
              new StructField("s3", DataTypes.StringType, true, null),
              new StructField("s4", DataTypes.StringType, true, null),
              new StructField("s5", DataTypes.StringType, true, null)
            });
    StructType projected =
        new StructType(new StructField[] {new StructField("key", DataTypes.LongType, true, null)});
    // fullWidths = 8 + 5*20 = 108; projWidths = 8.
    // 10 bytes × 8 / 108 = 0.74 → (long) cast → 0 → clamp → 1.
    LanceStatistics stats = LanceStatistics.estimateProjected(1000, 10L, full, projected);
    assertEquals(1L, stats.sizeInBytes().getAsLong());
  }
}
