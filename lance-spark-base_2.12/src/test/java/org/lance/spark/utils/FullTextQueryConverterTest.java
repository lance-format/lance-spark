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
import org.lance.ipc.FullTextQuery.Operator;
import org.lance.namespace.model.MatchQuery;
import org.lance.namespace.model.PhraseQuery;
import org.lance.namespace.model.StructuredFtsQuery;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FullTextQueryConverterTest {

  // ---- null contract ----

  @Test
  public void testNullReturnsNull() {
    assertNull(FullTextQueryConverter.toStructuredFtsQuery(null));
  }

  // ---- MatchQuery ----

  @Test
  public void testMatchSimpleUsesDefaults() {
    // FullTextQuery.match(text, col) defaults: boost=1.0, fuzziness=auto, maxExpansions=50,
    // operator=OR, prefixLength=0.
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(FullTextQuery.match("hello world", "body"));
    MatchQuery mq = s.getQuery().getMatch();
    assertEquals("hello world", mq.getTerms());
    assertEquals("body", mq.getColumn());
    assertEquals(1.0f, mq.getBoost(), 1e-6f);
    assertEquals(50, mq.getMaxExpansions().intValue());
    assertEquals("OR", mq.getOperator());
    assertEquals(0, mq.getPrefixLength().intValue());
    // auto fuzziness maps to unset (null), not 0.
    assertNull(mq.getFuzziness());
  }

  @Test
  public void testMatchAllFields() {
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(
            FullTextQuery.match("search text", "title", 2.5f, Optional.of(1), 30, Operator.AND, 3));
    MatchQuery mq = s.getQuery().getMatch();
    assertEquals("search text", mq.getTerms());
    assertEquals("title", mq.getColumn());
    assertEquals(2.5f, mq.getBoost(), 1e-6f);
    assertEquals(1, mq.getFuzziness().intValue());
    assertEquals(30, mq.getMaxExpansions().intValue());
    assertEquals("AND", mq.getOperator());
    assertEquals(3, mq.getPrefixLength().intValue());
  }

  @Test
  public void testMatchFuzzinessZeroPreserved() {
    // Optional.of(0) (exact) must map to 0, distinct from auto (null).
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(
            FullTextQuery.match("hello", "col", 1.0f, Optional.of(0), 50, Operator.OR, 0));
    assertEquals(0, s.getQuery().getMatch().getFuzziness().intValue());
  }

  // ---- PhraseQuery ----

  @Test
  public void testPhraseSimple() {
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(
            FullTextQuery.phrase("quick brown fox", "body"));
    PhraseQuery pq = s.getQuery().getPhrase();
    assertEquals("quick brown fox", pq.getTerms());
    assertEquals("body", pq.getColumn());
    assertEquals(0, pq.getSlop().intValue());
  }

  @Test
  public void testPhraseWithSlop() {
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(FullTextQuery.phrase("quick fox", "body", 2));
    assertEquals(2, s.getQuery().getPhrase().getSlop().intValue());
  }

  // ---- MultiMatchQuery ----

  @Test
  public void testMultiMatchExpandsOneMatchPerColumn() {
    List<String> cols = Arrays.asList("title", "body");
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(FullTextQuery.multiMatch("search", cols));
    List<MatchQuery> matches = s.getQuery().getMultiMatch().getMatchQueries();
    assertEquals(2, matches.size());
    assertEquals("search", matches.get(0).getTerms());
    assertEquals("title", matches.get(0).getColumn());
    assertEquals("search", matches.get(1).getTerms());
    assertEquals("body", matches.get(1).getColumn());
    // default operator OR is applied to each expanded match.
    assertEquals("OR", matches.get(0).getOperator());
    assertEquals("OR", matches.get(1).getOperator());
  }

  @Test
  public void testMultiMatchWithBoostsAndOperator() {
    List<String> cols = Arrays.asList("title", "body", "abstract");
    List<Float> boosts = Arrays.asList(2.0f, 1.0f, 0.5f);
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(
            FullTextQuery.multiMatch("search", cols, boosts, Operator.AND));
    List<MatchQuery> matches = s.getQuery().getMultiMatch().getMatchQueries();
    assertEquals(3, matches.size());
    for (MatchQuery m : matches) {
      assertEquals("search", m.getTerms());
      assertEquals("AND", m.getOperator());
    }
    assertEquals(2.0f, matches.get(0).getBoost(), 1e-6f);
    assertEquals(1.0f, matches.get(1).getBoost(), 1e-6f);
    assertEquals(0.5f, matches.get(2).getBoost(), 1e-6f);
  }

  @Test
  public void testMultiMatchWithoutBoostsLeavesBoostUnset() {
    StructuredFtsQuery s =
        FullTextQueryConverter.toStructuredFtsQuery(
            FullTextQuery.multiMatch("search", Arrays.asList("title", "body"), null, Operator.OR));
    for (MatchQuery m : s.getQuery().getMultiMatch().getMatchQueries()) {
      assertNull(m.getBoost());
    }
  }

  // ---- unsupported subtypes ----

  @Test
  public void testBoostQueryRejected() {
    FullTextQuery boost =
        FullTextQuery.boost(FullTextQuery.match("a", "col"), FullTextQuery.match("b", "col"));
    assertThrows(
        IllegalArgumentException.class, () -> FullTextQueryConverter.toStructuredFtsQuery(boost));
  }
}
