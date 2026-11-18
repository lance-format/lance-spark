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
 * Sentinel V2 function for FTS keyword search: {@code lance_match(col, query [, opts])}.
 *
 * <p>Returns {@code BooleanType} so Spark treats it as a valid filter predicate. {@code
 * produceResult} always throws — the predicate must be intercepted by {@code LanceFtsPredicateRule}
 * and pushed down to the scanner before execution. If it reaches row-level evaluation, the
 * optimizer rule is missing from the session extensions configuration.
 */
public final class LanceMatchFunction implements UnboundFunction {

  public static final String NAME = "lance_match";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    int arity = inputType.fields().length;
    if (arity == 2) {
      return new Bound(new DataType[] {DataTypes.StringType, DataTypes.StringType});
    } else if (arity == 3) {
      return new Bound(
          new DataType[] {DataTypes.StringType, DataTypes.StringType, DataTypes.StringType});
    } else {
      throw new IllegalArgumentException(
          NAME + " expects 2 or 3 arguments (column, query [, options]), got " + arity);
    }
  }

  @Override
  public String description() {
    return "FTS keyword search: lance_match(column, query [, 'key=value,...']) -> BOOLEAN. "
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
          "LanceMatchFunction should never be called at row level — "
              + "register LanceFtsPredicateRule via spark.sql.extensions. "
              + "Ensure the Lance Spark session extensions are configured.");
    }
  }
}
