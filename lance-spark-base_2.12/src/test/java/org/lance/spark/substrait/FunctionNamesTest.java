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
package org.lance.spark.substrait;

import org.lance.spark.substrait.FunctionNames.FunctionRef;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FunctionNamesTest {

  @ParameterizedTest
  @CsvSource({
    // Comparison
    "=,            equal,                comparison",
    "<>,           not_equal,            comparison",
    "<=>,          is_not_distinct_from, comparison",
    ">,            gt,                   comparison",
    ">=,           gte,                  comparison",
    "<,            lt,                   comparison",
    "<=,           lte,                  comparison",
    "IS_NULL,      is_null,              comparison",
    "IS_NOT_NULL,  is_not_null,          comparison",
    // Arithmetic
    "+,            add,                  arithmetic",
    "-,            subtract,             arithmetic",
    "*,            multiply,             arithmetic",
    "/,            divide,               arithmetic",
    // String
    "STARTS_WITH,  starts_with,          string",
    "ENDS_WITH,    ends_with,            string",
    "CONTAINS,     contains,             string",
    "UPPER,        upper,                string",
    "LOWER,        lower,                string",
  })
  void scalarOpsMapToBareNamesAndUris(String sparkOp, String bareName, String uriTag) {
    Optional<FunctionRef> ref = FunctionNames.lookupScalar(sparkOp);
    assertTrue(ref.isPresent(), "expected mapping for " + sparkOp);
    assertEquals(bareName, ref.get().bareName());
    assertEquals(expectedUri(uriTag), ref.get().extensionUri());
  }

  @ParameterizedTest
  @CsvSource({
    // count is in functions_aggregate_generic.yaml; sum/min/max are in functions_arithmetic.yaml
    // (verified against the canonical substrait spec at extensions/functions_*.yaml).
    "count, count, generic",
    "sum,   sum,   arithmetic",
    "min,   min,   arithmetic",
    "max,   max,   arithmetic",
  })
  void aggregateOpsMapToBareNamesAndUris(String sparkAgg, String bareName, String uriTag) {
    Optional<FunctionRef> ref = FunctionNames.lookupAggregate(sparkAgg);
    assertTrue(ref.isPresent(), "expected mapping for " + sparkAgg);
    assertEquals(bareName, ref.get().bareName());
    String expectedUri =
        "generic".equals(uriTag)
            ? FunctionNames.AGGREGATE_GENERIC_URI
            : FunctionNames.ARITHMETIC_URI;
    assertEquals(expectedUri, ref.get().extensionUri());
  }

  @Test
  void unmappedScalarOpsReturnEmpty() {
    // V2 ops we deliberately do not handle in the lookup map (each is special-cased elsewhere
    // or out of scope for Phase 1).
    assertFalse(FunctionNames.lookupScalar("AND").isPresent());
    assertFalse(FunctionNames.lookupScalar("OR").isPresent());
    assertFalse(FunctionNames.lookupScalar("NOT").isPresent());
    assertFalse(FunctionNames.lookupScalar("IN").isPresent());
    assertFalse(FunctionNames.lookupScalar("CAST").isPresent());
    assertFalse(FunctionNames.lookupScalar("ALWAYS_TRUE").isPresent());
    assertFalse(FunctionNames.lookupScalar("ALWAYS_FALSE").isPresent());
    assertFalse(FunctionNames.lookupScalar("BOOLEAN_EXPRESSION").isPresent());
  }

  @Test
  void avgIsExplicitlyUnsupportedAsAggregate() {
    // Lance executes aggregates completely per fragment; averaging averages with unequal
    // partition sizes is wrong, so AggregateEncoder rejects AVG. The lookup must agree.
    assertFalse(FunctionNames.lookupAggregate("avg").isPresent());
  }

  @Test
  void unknownOpReturnsEmpty() {
    assertFalse(FunctionNames.lookupScalar("DOES_NOT_EXIST").isPresent());
    assertFalse(FunctionNames.lookupAggregate("does_not_exist").isPresent());
  }

  private static String expectedUri(String tag) {
    switch (tag) {
      case "comparison":
        return FunctionNames.COMPARISON_URI;
      case "arithmetic":
        return FunctionNames.ARITHMETIC_URI;
      case "string":
        return FunctionNames.STRING_URI;
      case "boolean":
        return FunctionNames.BOOLEAN_URI;
      default:
        throw new IllegalArgumentException("Unknown URI tag: " + tag);
    }
  }
}
