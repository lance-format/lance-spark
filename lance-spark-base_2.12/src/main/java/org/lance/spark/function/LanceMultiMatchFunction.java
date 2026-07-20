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
package org.lance.spark.function;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Sentinel V2 function for multi-column FTS search.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>{@code lance_multi_match(query, col1, col2, ...)} — OR semantics (default)
 *   <li>{@code lance_multi_match(query, 'operator=AND', col1, col2, ...)} — explicit operator
 * </ul>
 *
 * <p>When the second argument is a string literal starting with a recognized option key ({@code
 * operator=}), it is treated as an options string rather than a column name. All remaining
 * arguments are column names. Minimum arity is 3 (query + at least 2 columns without options, or
 * query + options + at least 1 column). Option parsing and column extraction happen in {@code
 * LanceFtsPredicateRule} — {@code bind()} only validates arity and assigns types.
 *
 * <p>Returns {@code BooleanType} so Spark treats it as a valid filter predicate. {@code
 * produceResult} always throws — the predicate must be intercepted by {@code LanceFtsPredicateRule}
 * and pushed down to the scanner before execution.
 */
public final class LanceMultiMatchFunction implements UnboundFunction {

  public static final String NAME = "lance_multi_match";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    int arity = inputType.fields().length;
    if (arity < 3) {
      throw new IllegalArgumentException(
          NAME
              + " requires at least 3 arguments (query, [options,] col1, col2, ...), got "
              + arity);
    }
    DataType[] types = new DataType[arity];
    for (int i = 0; i < arity; i++) {
      types[i] = DataTypes.StringType;
    }
    return new Bound(types);
  }

  @Override
  public String description() {
    return "Multi-column FTS search: "
        + "lance_multi_match(query, col1, col2, ...) -> BOOLEAN, or "
        + "lance_multi_match(query, 'operator=AND|OR', col1, col2, ...) -> BOOLEAN. "
        + "Default operator is OR: a row is returned if any column matches. "
        + "Pushes the predicate to the Lance FTS inverted index via LanceFtsPredicateRule.";
  }

  private static final class Bound implements ScalarFunction<Boolean> {
    private final DataType[] inputTypes;

    private Bound(DataType[] inputTypes) {
      this.inputTypes = inputTypes;
    }

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public DataType[] inputTypes() {
      return inputTypes;
    }

    @Override
    public DataType resultType() {
      return DataTypes.BooleanType;
    }

    @Override
    public String canonicalName() {
      return NAME;
    }

    @Override
    public Boolean produceResult(InternalRow input) {
      throw new UnsupportedOperationException(
          "LanceMultiMatchFunction should never be called at row level — "
              + "register LanceFtsPredicateRule via spark.sql.extensions. "
              + "Ensure the Lance Spark session extensions are configured.");
    }
  }
}
