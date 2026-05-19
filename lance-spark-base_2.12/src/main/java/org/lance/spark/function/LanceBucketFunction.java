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

import org.lance.spark.utils.BucketHashUtil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * V2 bucket function registered in the Lance catalog so that Spark's {@code
 * V2ScanPartitioningAndOrdering} can convert {@code Expressions.bucket(N, col)} into a Catalyst
 * expression.
 *
 * <p>Without this function, Spark silently drops the {@code KeyGroupedPartitioning} because it
 * cannot resolve the "bucket" function, and SPJ falls back to a full shuffle.
 */
public final class LanceBucketFunction implements UnboundFunction {

  public static final String NAME = "bucket";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "bucket(numBuckets, col) — hash-partitions by" + " Murmur3 into N buckets";
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 2) {
      throw new UnsupportedOperationException(
          "bucket requires exactly 2 arguments:" + " numBuckets and column value");
    }
    DataType valueType = inputType.fields()[1].dataType();
    return new BucketScalarFunction(valueType);
  }

  private static final class BucketScalarFunction implements ScalarFunction<Integer> {

    private final DataType valueType;

    BucketScalarFunction(DataType valueType) {
      this.valueType = valueType;
    }

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public String canonicalName() {
      return "iceberg.bucket(" + valueType.catalogString() + ")";
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, valueType};
    }

    @Override
    public Integer produceResult(InternalRow input) {
      int numBuckets = input.getInt(0);
      if (input.isNullAt(1)) {
        return BucketHashUtil.computeBucketIdFromValue(null, numBuckets);
      }
      Object value = input.get(1, valueType);
      return BucketHashUtil.computeBucketIdFromValue(value, numBuckets);
    }
  }
}
