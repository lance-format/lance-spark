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
import org.lance.ipc.FullTextQuery.Type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Serialization and structural equality utilities for the three in-scope {@link FullTextQuery}
 * subtypes: {@link MatchQuery}, {@link PhraseQuery}, and {@link MultiMatchQuery}.
 *
 * <p>These utilities exist because the concrete subclasses do not override {@code equals()} or
 * {@code hashCode()}, inheriting reference equality from {@code Object}. The FTS optimizer rule
 * introduces a {@code fromOptions()} construction path where queries are re-parsed from JSON into
 * new object references, making reference equality produce incorrect results.
 */
public final class FullTextQueryUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String FIELD_TYPE = "type";
  private static final String FIELD_QUERY_TEXT = "queryText";
  private static final String FIELD_COLUMN = "column";
  private static final String FIELD_BOOST = "boost";
  private static final String FIELD_FUZZINESS = "fuzziness";
  private static final String FIELD_MAX_EXPANSIONS = "maxExpansions";
  private static final String FIELD_OPERATOR = "operator";
  private static final String FIELD_PREFIX_LENGTH = "prefixLength";
  private static final String FIELD_SLOP = "slop";
  private static final String FIELD_COLUMNS = "columns";
  private static final String FIELD_BOOSTS = "boosts";

  private FullTextQueryUtils() {}

  /**
   * Serializes a {@link FullTextQuery} to a JSON string. Returns {@code null} for {@code null}
   * input.
   *
   * @throws IllegalArgumentException for out-of-scope subtypes (BOOST, BOOLEAN)
   */
  public static String fullTextQueryToString(FullTextQuery query) {
    if (query == null) {
      return null;
    }
    try {
      ObjectNode node = MAPPER.createObjectNode();
      Type type = query.getType();
      node.put(FIELD_TYPE, type.name());
      switch (type) {
        case MATCH:
          {
            MatchQuery mq = (MatchQuery) query;
            node.put(FIELD_QUERY_TEXT, mq.getQueryText());
            node.put(FIELD_COLUMN, mq.getColumn());
            node.put(FIELD_BOOST, mq.getBoost());
            if (mq.getFuzziness().isPresent()) {
              node.put(FIELD_FUZZINESS, mq.getFuzziness().get());
            }
            node.put(FIELD_MAX_EXPANSIONS, mq.getMaxExpansions());
            node.put(FIELD_OPERATOR, mq.getOperator().name());
            node.put(FIELD_PREFIX_LENGTH, mq.getPrefixLength());
            break;
          }
        case MATCH_PHRASE:
          {
            PhraseQuery pq = (PhraseQuery) query;
            node.put(FIELD_QUERY_TEXT, pq.getQueryText());
            node.put(FIELD_COLUMN, pq.getColumn());
            node.put(FIELD_SLOP, pq.getSlop());
            break;
          }
        case MULTI_MATCH:
          {
            MultiMatchQuery mmq = (MultiMatchQuery) query;
            node.put(FIELD_QUERY_TEXT, mmq.getQueryText());
            ArrayNode cols = node.putArray(FIELD_COLUMNS);
            for (String col : mmq.getColumns()) {
              cols.add(col);
            }
            if (mmq.getBoosts().isPresent()) {
              ArrayNode boostsArr = node.putArray(FIELD_BOOSTS);
              for (Float boost : mmq.getBoosts().get()) {
                boostsArr.add(boost);
              }
            }
            node.put(FIELD_OPERATOR, mmq.getOperator().name());
            break;
          }
        default:
          throw new IllegalArgumentException(
              "Unsupported FullTextQuery subtype for serialization: " + type);
      }
      return MAPPER.writeValueAsString(node);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize FullTextQuery", e);
    }
  }

  /**
   * Deserializes a {@link FullTextQuery} from a JSON string produced by {@link
   * #fullTextQueryToString}. Returns {@code null} for {@code null} input.
   *
   * <p>Uses public static factory methods ({@code FullTextQuery.match()}, {@code
   * FullTextQuery.phrase()}, {@code FullTextQuery.multiMatch()}) — the inner class constructors are
   * package-private.
   */
  public static FullTextQuery stringToFullTextQuery(String json) {
    if (json == null) {
      return null;
    }
    try {
      JsonNode node = MAPPER.readTree(json);
      String typeName = node.get(FIELD_TYPE).asText();
      Type type = Type.valueOf(typeName);
      switch (type) {
        case MATCH:
          {
            String queryText = node.get(FIELD_QUERY_TEXT).asText();
            String column = node.get(FIELD_COLUMN).asText();
            float boost = (float) node.get(FIELD_BOOST).asDouble();
            Optional<Integer> fuzziness =
                node.has(FIELD_FUZZINESS)
                    ? Optional.of(node.get(FIELD_FUZZINESS).asInt())
                    : Optional.empty();
            int maxExpansions = node.get(FIELD_MAX_EXPANSIONS).asInt();
            Operator operator = Operator.valueOf(node.get(FIELD_OPERATOR).asText());
            int prefixLength = node.get(FIELD_PREFIX_LENGTH).asInt();
            return FullTextQuery.match(
                queryText, column, boost, fuzziness, maxExpansions, operator, prefixLength);
          }
        case MATCH_PHRASE:
          {
            String queryText = node.get(FIELD_QUERY_TEXT).asText();
            String column = node.get(FIELD_COLUMN).asText();
            int slop = node.get(FIELD_SLOP).asInt();
            return FullTextQuery.phrase(queryText, column, slop);
          }
        case MULTI_MATCH:
          {
            String queryText = node.get(FIELD_QUERY_TEXT).asText();
            ArrayNode colsNode = (ArrayNode) node.get(FIELD_COLUMNS);
            List<String> columns = new ArrayList<>();
            for (JsonNode colNode : colsNode) {
              columns.add(colNode.asText());
            }
            Operator operator = Operator.valueOf(node.get(FIELD_OPERATOR).asText());
            if (node.has(FIELD_BOOSTS) && !node.get(FIELD_BOOSTS).isNull()) {
              ArrayNode boostsNode = (ArrayNode) node.get(FIELD_BOOSTS);
              List<Float> boosts = new ArrayList<>();
              for (JsonNode boostNode : boostsNode) {
                boosts.add((float) boostNode.asDouble());
              }
              return FullTextQuery.multiMatch(queryText, columns, boosts, operator);
            } else {
              // Full overload with null boosts preserves the explicit operator.
              return FullTextQuery.multiMatch(queryText, columns, null, operator);
            }
          }
        default:
          throw new IllegalArgumentException(
              "Unsupported FullTextQuery type for deserialization: " + type);
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize FullTextQuery from: " + json, e);
    }
  }

  /**
   * Structural equality for two {@link FullTextQuery} instances. Handles null inputs: returns
   * {@code true} when both are null, {@code false} when exactly one is null.
   *
   * <p>TODO: Remove this method and replace all call sites with {@code Objects.equals(a, b)} once
   * https://github.com/lance-format/lance/pull/6674 is merged and released. That PR adds native
   * {@code equals()}/{@code hashCode()} to {@code FullTextQuery} subtypes; until then reference
   * equality from {@code Object} makes {@code Objects.equals} incorrect for
   * independently-constructed instances.
   */
  public static boolean equals(FullTextQuery a, FullTextQuery b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    if (a.getType() != b.getType()) {
      return false;
    }
    switch (a.getType()) {
      case MATCH:
        {
          MatchQuery matchA = (MatchQuery) a;
          MatchQuery matchB = (MatchQuery) b;
          return Objects.equals(matchA.getQueryText(), matchB.getQueryText())
              && Objects.equals(matchA.getColumn(), matchB.getColumn())
              && Float.compare(matchA.getBoost(), matchB.getBoost()) == 0
              && Objects.equals(matchA.getFuzziness(), matchB.getFuzziness())
              && matchA.getMaxExpansions() == matchB.getMaxExpansions()
              && matchA.getOperator() == matchB.getOperator()
              && matchA.getPrefixLength() == matchB.getPrefixLength();
        }
      case MATCH_PHRASE:
        {
          PhraseQuery phraseA = (PhraseQuery) a;
          PhraseQuery phraseB = (PhraseQuery) b;
          return Objects.equals(phraseA.getQueryText(), phraseB.getQueryText())
              && Objects.equals(phraseA.getColumn(), phraseB.getColumn())
              && phraseA.getSlop() == phraseB.getSlop();
        }
      case MULTI_MATCH:
        {
          MultiMatchQuery multiA = (MultiMatchQuery) a;
          MultiMatchQuery multiB = (MultiMatchQuery) b;
          return Objects.equals(multiA.getQueryText(), multiB.getQueryText())
              && Objects.equals(multiA.getColumns(), multiB.getColumns())
              && multiA.getOperator() == multiB.getOperator()
              && equalsBoostLists(multiA.getBoosts(), multiB.getBoosts());
        }
      default:
        throw new IllegalArgumentException(
            "Unsupported FullTextQuery type for equality: " + a.getType());
    }
  }

  private static boolean equalsBoostLists(
      Optional<List<Float>> boostsA, Optional<List<Float>> boostsB) {
    if (!boostsA.isPresent() && !boostsB.isPresent()) {
      return true;
    }
    if (!boostsA.isPresent() || !boostsB.isPresent()) {
      return false;
    }
    List<Float> listA = boostsA.get();
    List<Float> listB = boostsB.get();
    if (listA.size() != listB.size()) {
      return false;
    }
    for (int i = 0; i < listA.size(); i++) {
      if (Float.compare(listA.get(i), listB.get(i)) != 0) {
        return false;
      }
    }
    return true;
  }
}
