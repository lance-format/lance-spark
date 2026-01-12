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

import java.util.concurrent.ThreadLocalRandom;

/**
 * A V2 function used for write distribution: return the fragment id if present, otherwise return a
 * random int to spread insert rows across partitions.
 */
public final class LanceFragmentIdWithDefaultFunction implements UnboundFunction {
  public static final String NAME = "lance_fragment_or_rand";

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Bind the function to an input schema that contains a single INT field representing the fragment
   * id.
   */
  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 1) {
      throw new IllegalArgumentException(NAME + " expects 1 argument");
    }
    DataType argType = inputType.fields()[0].dataType();
    if (!DataTypes.IntegerType.sameType(argType)) {
      throw new IllegalArgumentException(NAME + " expects INT input");
    }
    return new Bound(inputType);
  }

  @Override
  public String description() {
    return "Return fragment id when present, otherwise a random int for distribution.";
  }

  private static final class Bound implements ScalarFunction<Integer> {
    private final DataType[] inputTypes;

    private Bound(StructType inputType) {
      this.inputTypes = new DataType[] {inputType.fields()[0].dataType()};
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
      return DataTypes.IntegerType;
    }

    @Override
    public boolean isResultNullable() {
      return false;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }

    @Override
    public String canonicalName() {
      return NAME;
    }

    /** Return the fragment id when available, otherwise return a random int to avoid skew. */
    @Override
    public Integer produceResult(InternalRow input) {
      if (input.isNullAt(0)) {
        return ThreadLocalRandom.current().nextInt();
      }
      return input.getInt(0);
    }
  }
}
