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

import org.lance.ipc.FullTextQuery;
import org.lance.ipc.FullTextQuery.MatchQuery;
import org.lance.ipc.FullTextQuery.MultiMatchQuery;
import org.lance.ipc.FullTextQuery.Operator;
import org.lance.ipc.FullTextQuery.PhraseQuery;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FullTextQueryUtilsTest {

  // ---- null contract ----

  @Test
  public void testToStringNullReturnsNull() {
    assertNull(FullTextQueryUtils.fullTextQueryToString(null));
  }

  @Test
  public void testFromStringNullReturnsNull() {
    assertNull(FullTextQueryUtils.stringToFullTextQuery(null));
  }

  // ---- MatchQuery round-trip ----

  @Test
  public void testMatchQueryRoundTripSimple() {
    FullTextQuery q = FullTextQuery.match("hello world", "body");
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    assertTrue(FullTextQueryUtils.equals(q, rt));
    assertEquals(FullTextQuery.Type.MATCH, rt.getType());
    MatchQuery mq = (MatchQuery) rt;
    assertEquals("hello world", mq.getQueryText());
    assertEquals("body", mq.getColumn());
  }

  @Test
  public void testMatchQueryRoundTripFuzzinessEmpty() {
    // Optional.empty() = auto-fuzziness default
    FullTextQuery q = FullTextQuery.match("hello", "col");
    MatchQuery mq = (MatchQuery) q;
    assertFalse(mq.getFuzziness().isPresent());
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    MatchQuery rtMq = (MatchQuery) rt;
    assertFalse(rtMq.getFuzziness().isPresent());
  }

  @Test
  public void testMatchQueryRoundTripFuzzinessZero() {
    // Optional.of(0) = exact match — must NOT be conflated with Optional.empty()
    FullTextQuery q = FullTextQuery.match("hello", "col", 1.0f, Optional.of(0), 50, Operator.OR, 0);
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    MatchQuery rtMq = (MatchQuery) rt;
    assertTrue(rtMq.getFuzziness().isPresent());
    assertEquals(0, rtMq.getFuzziness().get().intValue());
  }

  @Test
  public void testMatchQueryRoundTripFuzzinessNonZero() {
    FullTextQuery q = FullTextQuery.match("hello", "col", 1.0f, Optional.of(2), 50, Operator.OR, 0);
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    MatchQuery rtMq = (MatchQuery) rt;
    assertTrue(rtMq.getFuzziness().isPresent());
    assertEquals(2, rtMq.getFuzziness().get().intValue());
  }

  @Test
  public void testMatchQueryRoundTripAllFields() {
    FullTextQuery q =
        FullTextQuery.match("search text", "title", 2.5f, Optional.of(1), 30, Operator.AND, 3);
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    assertTrue(FullTextQueryUtils.equals(q, rt));
    MatchQuery mq = (MatchQuery) rt;
    assertEquals("search text", mq.getQueryText());
    assertEquals("title", mq.getColumn());
    assertEquals(2.5f, mq.getBoost(), 1e-6f);
    assertTrue(mq.getFuzziness().isPresent());
    assertEquals(1, mq.getFuzziness().get().intValue());
    assertEquals(30, mq.getMaxExpansions());
    assertEquals(Operator.AND, mq.getOperator());
    assertEquals(3, mq.getPrefixLength());
  }

  // ---- PhraseQuery round-trip ----

  @Test
  public void testPhraseQueryRoundTrip() {
    FullTextQuery q = FullTextQuery.phrase("quick brown fox", "body");
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    assertTrue(FullTextQueryUtils.equals(q, rt));
    PhraseQuery pq = (PhraseQuery) rt;
    assertEquals("quick brown fox", pq.getQueryText());
    assertEquals("body", pq.getColumn());
    assertEquals(0, pq.getSlop());
  }

  @Test
  public void testPhraseQueryRoundTripWithSlop() {
    FullTextQuery q = FullTextQuery.phrase("quick fox", "body", 2);
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    assertTrue(FullTextQueryUtils.equals(q, rt));
    assertEquals(2, ((PhraseQuery) rt).getSlop());
  }

  // ---- MultiMatchQuery round-trip ----

  @Test
  public void testMultiMatchQueryRoundTripSimple() {
    List<String> cols = Arrays.asList("title", "body");
    FullTextQuery q = FullTextQuery.multiMatch("search", cols);
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    assertTrue(FullTextQueryUtils.equals(q, rt));
    MultiMatchQuery mmq = (MultiMatchQuery) rt;
    assertEquals("search", mmq.getQueryText());
    assertEquals(cols, mmq.getColumns());
  }

  @Test
  public void testMultiMatchQueryRoundTripWithBoostsAndOperator() {
    List<String> cols = Arrays.asList("title", "body", "abstract");
    List<Float> boosts = Arrays.asList(2.0f, 1.0f, 0.5f);
    FullTextQuery q = FullTextQuery.multiMatch("search", cols, boosts, Operator.AND);
    FullTextQuery rt =
        FullTextQueryUtils.stringToFullTextQuery(FullTextQueryUtils.fullTextQueryToString(q));
    assertTrue(FullTextQueryUtils.equals(q, rt));
    MultiMatchQuery mmq = (MultiMatchQuery) rt;
    assertTrue(mmq.getBoosts().isPresent());
    List<Float> rtBoosts = mmq.getBoosts().get();
    assertEquals(3, rtBoosts.size());
    // Float precision must be preserved (not widened to double)
    assertEquals(2.0f, rtBoosts.get(0), 1e-6f);
    assertEquals(1.0f, rtBoosts.get(1), 1e-6f);
    assertEquals(0.5f, rtBoosts.get(2), 1e-6f);
    assertEquals(Operator.AND, mmq.getOperator());
  }

  // ---- equals() coverage ----

  @Test
  public void testEqualsBothNull() {
    assertTrue(FullTextQueryUtils.equals(null, null));
  }

  @Test
  public void testEqualsOneNull() {
    FullTextQuery q = FullTextQuery.match("x", "col");
    assertFalse(FullTextQueryUtils.equals(q, null));
    assertFalse(FullTextQueryUtils.equals(null, q));
  }

  @Test
  public void testEqualsIdenticalMatchQuery() {
    FullTextQuery a = FullTextQuery.match("hello", "col", 1.0f, Optional.of(1), 50, Operator.OR, 0);
    FullTextQuery b = FullTextQuery.match("hello", "col", 1.0f, Optional.of(1), 50, Operator.OR, 0);
    assertTrue(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMatchQueryDiffersInQueryText() {
    FullTextQuery a = FullTextQuery.match("hello", "col");
    FullTextQuery b = FullTextQuery.match("world", "col");
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMatchQueryDiffersInColumn() {
    FullTextQuery a = FullTextQuery.match("hello", "col1");
    FullTextQuery b = FullTextQuery.match("hello", "col2");
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMatchQueryDiffersInBoost() {
    FullTextQuery a =
        FullTextQuery.match("hello", "col", 1.0f, Optional.empty(), 50, Operator.OR, 0);
    FullTextQuery b =
        FullTextQuery.match("hello", "col", 2.0f, Optional.empty(), 50, Operator.OR, 0);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMatchQueryDiffersInFuzziness() {
    FullTextQuery a = FullTextQuery.match("hello", "col", 1.0f, Optional.of(0), 50, Operator.OR, 0);
    FullTextQuery b = FullTextQuery.match("hello", "col", 1.0f, Optional.of(1), 50, Operator.OR, 0);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMatchQueryDiffersInMaxExpansions() {
    FullTextQuery a =
        FullTextQuery.match("hello", "col", 1.0f, Optional.empty(), 30, Operator.OR, 0);
    FullTextQuery b =
        FullTextQuery.match("hello", "col", 1.0f, Optional.empty(), 50, Operator.OR, 0);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMatchQueryDiffersInOperator() {
    FullTextQuery a =
        FullTextQuery.match("hello", "col", 1.0f, Optional.empty(), 50, Operator.AND, 0);
    FullTextQuery b =
        FullTextQuery.match("hello", "col", 1.0f, Optional.empty(), 50, Operator.OR, 0);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMatchQueryDiffersInPrefixLength() {
    FullTextQuery a =
        FullTextQuery.match("hello", "col", 1.0f, Optional.empty(), 50, Operator.OR, 0);
    FullTextQuery b =
        FullTextQuery.match("hello", "col", 1.0f, Optional.empty(), 50, Operator.OR, 2);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsIdenticalPhraseQuery() {
    FullTextQuery a = FullTextQuery.phrase("quick brown fox", "body", 1);
    FullTextQuery b = FullTextQuery.phrase("quick brown fox", "body", 1);
    assertTrue(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsPhraseQueryDiffersInSlop() {
    FullTextQuery a = FullTextQuery.phrase("quick fox", "body", 0);
    FullTextQuery b = FullTextQuery.phrase("quick fox", "body", 1);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsPhraseQueryDiffersInColumn() {
    FullTextQuery a = FullTextQuery.phrase("quick fox", "body");
    FullTextQuery b = FullTextQuery.phrase("quick fox", "title");
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsPhraseQueryDiffersInQueryText() {
    FullTextQuery a = FullTextQuery.phrase("quick fox", "body");
    FullTextQuery b = FullTextQuery.phrase("slow fox", "body");
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsIdenticalMultiMatchQuery() {
    List<String> cols = Arrays.asList("title", "body");
    FullTextQuery a = FullTextQuery.multiMatch("hello", cols);
    FullTextQuery b = FullTextQuery.multiMatch("hello", cols);
    assertTrue(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMultiMatchDiffersInColumns() {
    FullTextQuery a = FullTextQuery.multiMatch("hello", Arrays.asList("col1", "col2"));
    FullTextQuery b = FullTextQuery.multiMatch("hello", Arrays.asList("col1", "col3"));
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMultiMatchDiffersInOperator() {
    List<String> cols = Arrays.asList("col1", "col2");
    FullTextQuery a = FullTextQuery.multiMatch("hello", cols, null, Operator.AND);
    FullTextQuery b = FullTextQuery.multiMatch("hello", cols, null, Operator.OR);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMultiMatchDiffersInQueryText() {
    List<String> cols = Arrays.asList("col1", "col2");
    FullTextQuery a = FullTextQuery.multiMatch("hello", cols);
    FullTextQuery b = FullTextQuery.multiMatch("world", cols);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMultiMatchBoostsPresentVsAbsent() {
    List<String> cols = Arrays.asList("col1", "col2");
    List<Float> boosts = Arrays.asList(1.0f, 2.0f);
    FullTextQuery a = FullTextQuery.multiMatch("hello", cols, boosts, Operator.OR);
    FullTextQuery b = FullTextQuery.multiMatch("hello", cols, null, Operator.OR);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsMultiMatchBoostsDifferentValues() {
    List<String> cols = Arrays.asList("col1", "col2");
    FullTextQuery a =
        FullTextQuery.multiMatch("hello", cols, Arrays.asList(1.0f, 2.0f), Operator.OR);
    FullTextQuery b =
        FullTextQuery.multiMatch("hello", cols, Arrays.asList(1.0f, 3.0f), Operator.OR);
    assertFalse(FullTextQueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDifferentSubtypes() {
    FullTextQuery a = FullTextQuery.match("hello", "col");
    FullTextQuery b = FullTextQuery.phrase("hello", "col");
    assertFalse(FullTextQueryUtils.equals(a, b));
  }
}
