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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link LanceStatsKeys} key constants and validators. */
class LanceStatsKeysTest {

  @Test
  @DisplayName("validateColumnName accepts plain names")
  void validateColumnNameAccepts() {
    LanceStatsKeys.validateColumnName("foo");
    LanceStatsKeys.validateColumnName("foo_bar");
    LanceStatsKeys.validateColumnName("Mixed-Case");
    LanceStatsKeys.validateColumnName("with spaces");
    // No assertion needed — passing without throwing is the contract.
  }

  @Test
  @DisplayName("validateColumnName rejects dotted names (key ambiguity with nested-leaf paths)")
  void validateColumnNameRejectsDots() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> LanceStatsKeys.validateColumnName("a.b"));
    assertTrue(ex.getMessage().contains("'.'"));
  }

  @Test
  @DisplayName("validateColumnName rejects null")
  void validateColumnNameRejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> LanceStatsKeys.validateColumnName(null));
  }

  @Test
  @DisplayName("validateColumnName rejects empty string (orphan-cleanup ambiguity)")
  void validateColumnNameRejectsEmpty() {
    assertThrows(IllegalArgumentException.class, () -> LanceStatsKeys.validateColumnName(""));
  }

  @Test
  @DisplayName("isStatsCompatibleColumnName: empty / null / dotted all reject")
  void isStatsCompatibleColumnNameRejectsBadInputs() {
    assertFalse(LanceStatsKeys.isStatsCompatibleColumnName(null));
    assertFalse(LanceStatsKeys.isStatsCompatibleColumnName(""));
    assertFalse(LanceStatsKeys.isStatsCompatibleColumnName("a.b"));
    assertTrue(LanceStatsKeys.isStatsCompatibleColumnName("a"));
  }

  @Test
  @DisplayName("Concatenated keys equal top-level constants")
  void keyConstantsConsistency() {
    assertEquals(
        "lance.stats.column.foo.min",
        LanceStatsKeys.COLUMN_PREFIX + "foo" + LanceStatsTestKeys.COLUMN_SUFFIX_MIN);
    assertEquals(
        "lance.stats.column.foo.distinctCount",
        LanceStatsKeys.COLUMN_PREFIX + "foo" + LanceStatsTestKeys.COLUMN_SUFFIX_DISTINCT_COUNT);
  }
}
