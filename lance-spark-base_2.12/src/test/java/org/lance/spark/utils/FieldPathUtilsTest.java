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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FieldPathUtilsTest {

  @Test
  public void testCanonicalPathQuotesLiteralDots() {
    assertEquals("parent.child", FieldPathUtils.canonicalPath(List.of("parent", "child")));
    assertEquals(
        "parent.`child.with.dot`",
        FieldPathUtils.canonicalPath(List.of("parent", "child.with.dot")));
  }

  @Test
  public void testCanonicalPathQuotesNonIdentifierCharacters() {
    assertEquals(
        "`meta-data`.`user-id`", FieldPathUtils.canonicalPath(List.of("meta-data", "user-id")));
    assertEquals(
        "parent.`display name`", FieldPathUtils.canonicalPath(List.of("parent", "display name")));
  }

  @Test
  public void testCanonicalPathEscapesBackticks() {
    assertEquals(
        "parent.`child``name`", FieldPathUtils.canonicalPath(List.of("parent", "child`name")));
  }

  @Test
  public void testParseCanonicalPath() {
    assertEquals(
        List.of("parent", "child.with.dot", "leaf"),
        FieldPathUtils.parseCanonicalPath("parent.`child.with.dot`.leaf"));
    assertEquals(
        List.of("parent", "child`name"), FieldPathUtils.parseCanonicalPath("parent.`child``name`"));
  }

  @Test
  public void testParseCanonicalPathRejectsMalformedPath() {
    assertThrows(
        IllegalArgumentException.class, () -> FieldPathUtils.parseCanonicalPath("parent..child"));
    assertThrows(
        IllegalArgumentException.class, () -> FieldPathUtils.parseCanonicalPath("parent.`child"));
    assertThrows(
        IllegalArgumentException.class,
        () -> FieldPathUtils.parseCanonicalPath("parent.`child`tail"));
  }
}
