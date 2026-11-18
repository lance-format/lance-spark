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

import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceMatchFunctionTest {

  private final LanceMatchFunction fn = new LanceMatchFunction();

  private static StructType schema(int arity) {
    StructField[] fields = new StructField[arity];
    for (int i = 0; i < arity; i++) {
      fields[i] = DataTypes.createStructField("arg" + i, DataTypes.StringType, false);
    }
    return new StructType(fields);
  }

  @Test
  public void testArity2Accepted() {
    fn.bind(schema(2));
  }

  @Test
  public void testArity3Accepted() {
    fn.bind(schema(3));
  }

  @Test
  public void testArity0Rejected() {
    assertThrows(IllegalArgumentException.class, () -> fn.bind(schema(0)));
  }

  @Test
  public void testArity1Rejected() {
    assertThrows(IllegalArgumentException.class, () -> fn.bind(schema(1)));
  }

  @Test
  public void testArity4Rejected() {
    assertThrows(IllegalArgumentException.class, () -> fn.bind(schema(4)));
  }

  @Test
  public void testName() {
    assertEquals(LanceMatchFunction.NAME, fn.name());
    assertEquals("lance_match", fn.name());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBoundNameAndCanonicalName() {
    // Bound.name() is the hook point used by the slice 02 optimizer rule to match on function name.
    ScalarFunction<Boolean> bound2 = (ScalarFunction<Boolean>) fn.bind(schema(2));
    assertEquals("lance_match", bound2.name());
    assertEquals("lance_match", bound2.canonicalName());

    ScalarFunction<Boolean> bound3 = (ScalarFunction<Boolean>) fn.bind(schema(3));
    assertEquals("lance_match", bound3.name());
    assertEquals("lance_match", bound3.canonicalName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProduceResultThrows() {
    // The sentinel contract: if the optimizer rule is absent, row-level invocation must throw
    // UnsupportedOperationException rather than silently returning false and filtering all rows.
    ScalarFunction<Boolean> bound2 = (ScalarFunction<Boolean>) fn.bind(schema(2));
    assertThrows(UnsupportedOperationException.class, () -> bound2.produceResult(null));

    ScalarFunction<Boolean> bound3 = (ScalarFunction<Boolean>) fn.bind(schema(3));
    assertThrows(UnsupportedOperationException.class, () -> bound3.produceResult(null));
  }
}
