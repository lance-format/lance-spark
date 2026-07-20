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

public class LancePhraseFunctionTest {

  private final LancePhraseFunction fn = new LancePhraseFunction();

  private static StructType stringSchema(int arity) {
    StructField[] fields = new StructField[arity];
    for (int i = 0; i < arity; i++) {
      fields[i] = DataTypes.createStructField("arg" + i, DataTypes.StringType, false);
    }
    return new StructType(fields);
  }

  private static StructType phraseSchema3() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("col", DataTypes.StringType, false),
          DataTypes.createStructField("query", DataTypes.StringType, false),
          DataTypes.createStructField("slop", DataTypes.IntegerType, false)
        });
  }

  @Test
  public void testArity2Accepted() {
    fn.bind(stringSchema(2));
  }

  @Test
  public void testArity3WithSlopAccepted() {
    fn.bind(phraseSchema3());
  }

  @Test
  public void testArity0Rejected() {
    assertThrows(IllegalArgumentException.class, () -> fn.bind(stringSchema(0)));
  }

  @Test
  public void testArity1Rejected() {
    assertThrows(IllegalArgumentException.class, () -> fn.bind(stringSchema(1)));
  }

  @Test
  public void testArity4Rejected() {
    assertThrows(IllegalArgumentException.class, () -> fn.bind(stringSchema(4)));
  }

  @Test
  public void testName() {
    assertEquals(LancePhraseFunction.NAME, fn.name());
    assertEquals("lance_match_phrase", fn.name());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBoundNameAndCanonicalName() {
    ScalarFunction<Boolean> bound2 = (ScalarFunction<Boolean>) fn.bind(stringSchema(2));
    assertEquals("lance_match_phrase", bound2.name());
    assertEquals("lance_match_phrase", bound2.canonicalName());

    ScalarFunction<Boolean> bound3 = (ScalarFunction<Boolean>) fn.bind(phraseSchema3());
    assertEquals("lance_match_phrase", bound3.name());
    assertEquals("lance_match_phrase", bound3.canonicalName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProduceResultThrows() {
    ScalarFunction<Boolean> bound2 = (ScalarFunction<Boolean>) fn.bind(stringSchema(2));
    assertThrows(UnsupportedOperationException.class, () -> bound2.produceResult(null));

    ScalarFunction<Boolean> bound3 = (ScalarFunction<Boolean>) fn.bind(phraseSchema3());
    assertThrows(UnsupportedOperationException.class, () -> bound3.produceResult(null));
  }
}
