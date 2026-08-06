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
import org.lance.namespace.model.FtsQuery;
import org.lance.namespace.model.MatchQuery;
import org.lance.namespace.model.MultiMatchQuery;
import org.lance.namespace.model.PhraseQuery;
import org.lance.namespace.model.StructuredFtsQuery;

import java.util.List;
import java.util.Optional;

/**
 * Converts the canonical {@link FullTextQuery} (the model both FTS surfaces build) into the Lance
 * namespace structured FTS model ({@link StructuredFtsQuery}) carried by {@code queryTable}. This
 * is the namespace-backend half of the shared FTS front-end; the local backend consumes {@link
 * FullTextQuery} directly via {@code ScanOptions.fullTextQuery(...)}.
 *
 * <p>Only the three in-scope subtypes are supported ({@code MATCH}, {@code MATCH_PHRASE}, {@code
 * MULTI_MATCH}), matching {@link FullTextQueryUtils}. {@code BOOST}/{@code BOOLEAN} throw.
 *
 * <p>Field mapping (verified against lance-namespace 0.7.5):
 *
 * <ul>
 *   <li>{@code MatchQuery(queryText, column, boost, fuzziness, maxExpansions, operator,
 *       prefixLength)} → namespace {@code MatchQuery} (same fields; {@code queryText}→{@code
 *       terms}, {@code Operator}→its {@code name()}).
 *   <li>{@code PhraseQuery(queryText, column, slop)} → namespace {@code PhraseQuery}.
 *   <li>{@code MultiMatchQuery(queryText, columns, boosts, operator)} → namespace {@code
 *       MultiMatchQuery}, expanded to one {@code MatchQuery} per column (terms={@code queryText},
 *       the shared operator, and {@code boosts[i]} when present). The namespace model has no
 *       top-level operator, so it is applied per expanded match.
 * </ul>
 */
public final class FullTextQueryConverter {

  private FullTextQueryConverter() {}

  /**
   * Converts to the namespace structured FTS model. Returns {@code null} for {@code null} input.
   *
   * @throws IllegalArgumentException for out-of-scope subtypes (BOOST, BOOLEAN)
   */
  public static StructuredFtsQuery toStructuredFtsQuery(FullTextQuery query) {
    if (query == null) {
      return null;
    }
    return new StructuredFtsQuery().query(toFtsQuery(query));
  }

  private static FtsQuery toFtsQuery(FullTextQuery query) {
    FullTextQuery.Type type = query.getType();
    switch (type) {
      case MATCH:
        return new FtsQuery().match(toMatchQuery((FullTextQuery.MatchQuery) query));
      case MATCH_PHRASE:
        return new FtsQuery().phrase(toPhraseQuery((FullTextQuery.PhraseQuery) query));
      case MULTI_MATCH:
        return new FtsQuery().multiMatch(toMultiMatchQuery((FullTextQuery.MultiMatchQuery) query));
      default:
        throw new IllegalArgumentException(
            "Unsupported FullTextQuery subtype for namespace conversion: " + type);
    }
  }

  private static MatchQuery toMatchQuery(FullTextQuery.MatchQuery mq) {
    MatchQuery out =
        new MatchQuery()
            .terms(mq.getQueryText())
            .column(mq.getColumn())
            .boost(mq.getBoost())
            .maxExpansions(mq.getMaxExpansions())
            .operator(mq.getOperator().name())
            .prefixLength(mq.getPrefixLength());
    // Optional.empty() = auto fuzziness; leave the namespace field unset (null) to preserve it.
    mq.getFuzziness().ifPresent(out::fuzziness);
    return out;
  }

  private static PhraseQuery toPhraseQuery(FullTextQuery.PhraseQuery pq) {
    return new PhraseQuery().terms(pq.getQueryText()).column(pq.getColumn()).slop(pq.getSlop());
  }

  private static MultiMatchQuery toMultiMatchQuery(FullTextQuery.MultiMatchQuery mmq) {
    String terms = mmq.getQueryText();
    String operator = mmq.getOperator().name();
    List<String> columns = mmq.getColumns();
    Optional<List<Float>> boosts = mmq.getBoosts();
    MultiMatchQuery out = new MultiMatchQuery();
    for (int i = 0; i < columns.size(); i++) {
      MatchQuery match = new MatchQuery().terms(terms).column(columns.get(i)).operator(operator);
      if (boosts.isPresent() && i < boosts.get().size()) {
        match.boost(boosts.get().get(i));
      }
      out.addMatchQueriesItem(match);
    }
    return out;
  }
}
