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
package org.lance.spark.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link ParserUtils#cleanIdentifier}. */
public class ParserUtilsTest {

  @Test
  public void testUnquotedIdentifier() {
    assertEquals("TABLE_NAME", ParserUtils.cleanIdentifier("TABLE_NAME"));
  }

  @Test
  public void testBacktickQuotedIdentifier() {
    assertEquals("my-table", ParserUtils.cleanIdentifier("`my-table`"));
  }

  @Test
  public void testEscapedDoubledBacktick() {
    // ``  inside backtick-quoted identifier → single `
    assertEquals("my`table", ParserUtils.cleanIdentifier("`my``table`"));
  }

  @Test
  public void testMultipleEscapedBackticks() {
    assertEquals("a`b`c", ParserUtils.cleanIdentifier("`a``b``c`"));
  }

  @Test
  public void testEmptyBacktickQuotedIdentifier() {
    // `` (two backticks, empty content) → empty string
    assertEquals("", ParserUtils.cleanIdentifier("``"));
  }

  @Test
  public void testSingleBacktick() {
    // Single backtick is not a valid quoted identifier — returned as-is
    assertEquals("`", ParserUtils.cleanIdentifier("`"));
  }

  @Test
  public void testEmptyString() {
    assertEquals("", ParserUtils.cleanIdentifier(""));
  }

  @Test
  public void testPlainIdentifierWithUnderscore() {
    assertEquals("COL_NAME_123", ParserUtils.cleanIdentifier("COL_NAME_123"));
  }

  @Test
  public void testBacktickQuotedWithSpaces() {
    assertEquals("my table", ParserUtils.cleanIdentifier("`my table`"));
  }

  @Test
  public void testBacktickQuotedWithDots() {
    // Dots inside backtick-quoted identifier are literal, not separators
    assertEquals("schema.table", ParserUtils.cleanIdentifier("`schema.table`"));
  }

  @Test
  public void testNullInput() {
    assertNull(ParserUtils.cleanIdentifier(null));
  }
}
