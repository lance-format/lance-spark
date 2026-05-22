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
package org.lance.spark.index;

import org.lance.index.IndexType;

import org.apache.spark.sql.catalyst.plans.logical.LanceNamedArgument;
import org.apache.spark.sql.execution.datasources.v2.IndexUtils;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexUtilsTest {

  @Test
  public void testBuildIndexTypeKnown() {
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("btree"));
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("fts"));
    assertEquals(IndexType.ZONEMAP, IndexUtils.buildIndexType("zonemap"));
  }

  @Test
  public void testBuildIndexTypeCaseInsensitive() {
    assertEquals(IndexType.ZONEMAP, IndexUtils.buildIndexType("ZoneMap"));
    assertEquals(IndexType.BTREE, IndexUtils.buildIndexType("BTREE"));
    assertEquals(IndexType.INVERTED, IndexUtils.buildIndexType("FTS"));
  }

  @Test
  public void testBuildIndexTypeUnknown() {
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> IndexUtils.buildIndexType("hash"));
    assertTrue(ex.getMessage().contains("Unsupported index method"));
    assertTrue(ex.getMessage().contains("hash"));
  }

  @Test
  public void testBuildScalarIndexParamTypeKnown() {
    assertEquals("btree", IndexUtils.buildScalarIndexParamType("btree"));
    assertEquals("inverted", IndexUtils.buildScalarIndexParamType("fts"));
    assertEquals("zonemap", IndexUtils.buildScalarIndexParamType("zonemap"));
  }

  @Test
  public void testBuildScalarIndexParamTypeCaseInsensitive() {
    assertEquals("zonemap", IndexUtils.buildScalarIndexParamType("ZoneMap"));
    assertEquals("btree", IndexUtils.buildScalarIndexParamType("BTREE"));
    assertEquals("inverted", IndexUtils.buildScalarIndexParamType("FTS"));
  }

  @Test
  public void testBuildScalarIndexParamTypeUnknown() {
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> IndexUtils.buildScalarIndexParamType("hash"));
    assertTrue(ex.getMessage().contains("Unsupported index method"));
    assertTrue(ex.getMessage().contains("hash"));
  }

  @Test
  public void testBuildIndexTypeNullRejected() {
    // null method names must fail fast with a clear error rather than NPE inside
    // `.toLowerCase()` — that path would leak an ambiguous null-pointer message to operators
    // who passed `using <method>` with an unparseable token.
    assertThrows(NullPointerException.class, () -> IndexUtils.buildIndexType(null));
  }

  @Test
  public void testBuildScalarIndexParamTypeNullRejected() {
    assertThrows(NullPointerException.class, () -> IndexUtils.buildScalarIndexParamType(null));
  }

  // ----- args → JSON serialization (IndexUtils.toJson) -------------------------------------
  //
  // These tests pin the with-clause-to-lance-core wire shape. A silent-param-drop regression
  // (the failure mode that already cost us a real bug: `with (zone_size=N)` on a ZONEMAP
  // index was forwarded as-is to lance-core but ignored by serde because the param name
  // there is `rows_per_zone`) is verified at the IndexUtils boundary here — the test does
  // NOT prevent lance-core from ignoring unknown keys, but it DOES guarantee the param name
  // and value the user wrote actually reach lance-core verbatim.

  @Test
  public void testToJsonEmptyArgs() {
    assertEquals("{}", IndexUtils.toJson(toScalaSeq(Collections.emptyList())));
  }

  @Test
  public void testToJsonLongValuePreserved() {
    // The most common path: `with (rows_per_zone=4096)` arrives as Long. Verify the name is
    // preserved verbatim and the value is a JSON number, not a quoted string.
    String json =
        IndexUtils.toJson(
            toScalaSeq(Collections.singletonList(new LanceNamedArgument("rows_per_zone", 4096L))));
    assertEquals("{\"rows_per_zone\":4096}", json);
  }

  @Test
  public void testToJsonStringValueStripsQuotes() {
    // NOTE: in the production SQL parser path, quote-stripping happens earlier at
    // `LanceSqlExtensionsAstBuilder.visitStringLiteral` — by the time the value reaches
    // IndexUtils.toJson, the outer quotes have already been removed. The strip logic in
    // toJson itself is a defensive second layer for programmatic callers (e.g. a future
    // code path that constructs LanceNamedArgument outside the SQL parser, or a test
    // helper passing already-quoted strings).
    //
    // These tests pin the toJson defensive layer's behaviour in isolation. They do NOT
    // reach via SQL parsing (which would already have stripped quotes).
    //
    // Cover the three observable styles separately rather than only one:
    //   single-quoted: `'tantivy'` → `tantivy`
    //   double-quoted: `"tantivy"` → `tantivy`
    //   mixed quotes (the strip pair is applied independently, both pairs removed):
    //       `"'tantivy'"` → `tantivy`
    // A regression that only stripped one quote variety would pass the existing
    // single-quoted case but fail the others.
    String singleQuoted =
        IndexUtils.toJson(
            toScalaSeq(Collections.singletonList(new LanceNamedArgument("kind", "'tantivy'"))));
    assertEquals("{\"kind\":\"tantivy\"}", singleQuoted);

    String doubleQuoted =
        IndexUtils.toJson(
            toScalaSeq(Collections.singletonList(new LanceNamedArgument("kind", "\"tantivy\""))));
    assertEquals("{\"kind\":\"tantivy\"}", doubleQuoted);

    // Mixed: `"'tantivy'"` — outer pair is double, inner is single. The strip-chain at
    // IndexUtils.toJson removes both pairs (outer double, then inner single from the result).
    String mixed =
        IndexUtils.toJson(
            toScalaSeq(Collections.singletonList(new LanceNamedArgument("kind", "\"'tantivy'\""))));
    assertEquals("{\"kind\":\"tantivy\"}", mixed);
  }

  @Test
  public void testToJsonMultipleArgsPreserveOrder() {
    // args is a Seq, not a Map — input order must be preserved through to JSON. A
    // regression that reordered via HashMap would silently break lance-core integrations
    // that depend on declaration order (none exist today, but the contract is the user's
    // SQL clause order).
    String json =
        IndexUtils.toJson(
            toScalaSeq(
                Arrays.asList(
                    new LanceNamedArgument("rows_per_zone", 4096L),
                    new LanceNamedArgument("max_levels", 3))));
    assertEquals("{\"rows_per_zone\":4096,\"max_levels\":3}", json);
  }

  @Test
  public void testToJsonNullValueEmitsNull() {
    String json =
        IndexUtils.toJson(
            toScalaSeq(Collections.singletonList(new LanceNamedArgument("opt", null))));
    assertEquals("{\"opt\":null}", json);
  }

  @Test
  public void testToJsonBooleanValueEmitsNativeBoolean() {
    String json =
        IndexUtils.toJson(
            toScalaSeq(Collections.singletonList(new LanceNamedArgument("flag", true))));
    assertEquals("{\"flag\":true}", json);
  }

  // Helper: convert Java List to Scala immutable.Seq[LanceNamedArgument] for IndexUtils.toJson.
  // Must return immutable.Seq specifically — in Scala 2.13 the default `Seq` type is
  // `scala.collection.immutable.Seq`, so calling `.toSeq()` on a Scala Buffer (which yields
  // `scala.collection.Seq` in 2.12 and `scala.collection.immutable.Seq` in 2.13) is a type
  // mismatch across the cross-Scala-version boundary. `.toList()` yields
  // `scala.collection.immutable.List` which IS a `Seq` in both 2.12 and 2.13.
  private static scala.collection.immutable.Seq<LanceNamedArgument> toScalaSeq(
      java.util.List<LanceNamedArgument> args) {
    return JavaConverters.asScalaBufferConverter(args).asScala().toList();
  }
}
