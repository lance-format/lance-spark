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
}
