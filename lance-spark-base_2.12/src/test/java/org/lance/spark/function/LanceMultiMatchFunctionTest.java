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

public class LanceMultiMatchFunctionTest {

  private final LanceMultiMatchFunction fn = new LanceMultiMatchFunction();

  private static StructType schema(int arity) {
    StructField[] fields = new StructField[arity];
    for (int i = 0; i < arity; i++) {
      fields[i] = DataTypes.createStructField("arg" + i, DataTypes.StringType, false);
    }
    return new StructType(fields);
  }

  @Test
  public void testArity3Accepted() {
    fn.bind(schema(3));
  }

  @Test
  public void testArity5Accepted() {
    fn.bind(schema(5));
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
  public void testArity2Rejected() {
    assertThrows(IllegalArgumentException.class, () -> fn.bind(schema(2)));
  }

  @Test
  public void testName() {
    assertEquals(LanceMultiMatchFunction.NAME, fn.name());
    assertEquals("lance_multi_match", fn.name());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBoundNameAndCanonicalName() {
    ScalarFunction<Boolean> bound3 = (ScalarFunction<Boolean>) fn.bind(schema(3));
    assertEquals("lance_multi_match", bound3.name());
    assertEquals("lance_multi_match", bound3.canonicalName());

    ScalarFunction<Boolean> bound5 = (ScalarFunction<Boolean>) fn.bind(schema(5));
    assertEquals("lance_multi_match", bound5.name());
    assertEquals("lance_multi_match", bound5.canonicalName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProduceResultThrows() {
    ScalarFunction<Boolean> bound3 = (ScalarFunction<Boolean>) fn.bind(schema(3));
    assertThrows(UnsupportedOperationException.class, () -> bound3.produceResult(null));

    ScalarFunction<Boolean> bound5 = (ScalarFunction<Boolean>) fn.bind(schema(5));
    assertThrows(UnsupportedOperationException.class, () -> bound5.produceResult(null));
  }
}
