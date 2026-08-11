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
package org.lance.spark.update;

import org.apache.spark.sql.execution.datasources.v2.LanceSystemIndex;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for shared Lance index-name helpers. */
public class LanceSystemIndexTest {

  /**
   * Normalization must be locale-independent. Under tr-TR, {@code "IDX".toLowerCase()} yields
   * {@code "ıdx"} (dotless i), which would make CREATE and OPTIMIZE/DROP INDEX disagree on the same
   * name. {@code Locale.ROOT} keeps it {@code "idx"} regardless of the JVM default locale.
   */
  @Test
  public void testNormalizeNameIsLocaleIndependent() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));
      assertEquals("idx_istanbul", LanceSystemIndex.normalizeName("IDX_ISTANBUL"));
      assertEquals("idx", LanceSystemIndex.normalizeName("IDX"));
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  public void testNormalizeNameNull() {
    assertEquals(null, LanceSystemIndex.normalizeName(null));
  }

  @Test
  public void testIsSystemIndex() {
    assertTrue(LanceSystemIndex.isSystemIndex("__lance_frag_reuse"));
    assertTrue(LanceSystemIndex.isSystemIndex("__LANCE_MEM_WAL"));
    assertFalse(LanceSystemIndex.isSystemIndex("idx_user"));
    assertFalse(LanceSystemIndex.isSystemIndex(null));
  }
}
