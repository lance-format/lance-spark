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
package org.lance.spark.read;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class LanceRuntimeFiltering41CompatibilityTest {
  @Test
  public void testReleased41ContractAndPredicateShape() {
    assertTrue(
        Arrays.stream(SupportsRuntimeV2Filtering.class.getMethods())
            .anyMatch(method -> method.getName().equals("filterAttributes")));
    assertTrue(
        Arrays.stream(SupportsRuntimeV2Filtering.class.getMethods())
            .anyMatch(method -> method.getName().equals("filter")));
    assertFalse(
        Arrays.stream(SupportsRuntimeV2Filtering.class.getMethods())
            .anyMatch(method -> method.getName().equals("pushedPredicates")));
    assertFalse(
        Arrays.stream(SupportsRuntimeV2Filtering.class.getMethods())
            .anyMatch(method -> method.getName().equals("supportsIterativePushdown")));

    Predicate predicate = TestPredicates.in("x", 1, 2);
    Expression[] children = predicate.children();
    assertEquals("IN", predicate.name());
    assertInstanceOf(FieldReference.class, children[0]);
    assertInstanceOf(Literal.class, children[1]);
    assertInstanceOf(Literal.class, children[2]);
  }
}
