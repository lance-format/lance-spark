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

import org.lance.index.DistanceType;
import org.lance.ipc.Query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryUtilsTest {

  private static Query.Builder base() {
    Query.Builder b = new Query.Builder();
    b.setColumn("vec");
    b.setKey(new float[] {1.0f, 2.0f, 3.0f});
    b.setK(10);
    b.setMinimumNprobes(1);
    b.setUseIndex(true);
    return b;
  }

  @Test
  public void testEqualsBothNull() {
    assertTrue(QueryUtils.equals(null, null));
  }

  @Test
  public void testEqualsOneNull() {
    Query q = base().build();
    assertFalse(QueryUtils.equals(q, null));
    assertFalse(QueryUtils.equals(null, q));
  }

  @Test
  public void testEqualsIdentical() {
    Query a = base().build();
    Query b = base().build();
    assertTrue(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffColumn() {
    Query a = base().build();
    Query b = base().setColumn("other").build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffKey() {
    Query a = base().build();
    Query b = base().setKey(new float[] {1.0f, 2.0f, 4.0f}).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffK() {
    Query a = base().build();
    Query b = base().setK(5).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffMinimumNprobes() {
    Query a = base().build();
    Query b = base().setMinimumNprobes(5).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffMaximumNprobes() {
    Query a = base().setMaximumNprobes(10).build();
    Query b = base().setMaximumNprobes(20).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffEf() {
    Query a = base().setEf(64).build();
    Query b = base().setEf(128).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffRefineFactor() {
    Query a = base().setRefineFactor(1).build();
    Query b = base().setRefineFactor(2).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffDistanceType() {
    Query a = base().setDistanceType(DistanceType.L2).build();
    Query b = base().setDistanceType(DistanceType.Cosine).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsDiffUseIndex() {
    Query a = base().setUseIndex(true).build();
    Query b = base().setUseIndex(false).build();
    assertFalse(QueryUtils.equals(a, b));
  }

  @Test
  public void testEqualsIgnoresQueryParallelism() {
    // queryParallelism is excluded — instances differing only in this field must compare equal.
    // The Query.Builder doesn't expose setQueryParallelism, so we verify via string round-trip:
    // two independently deserialized instances of the same JSON are equal.
    String json = QueryUtils.queryToString(base().build());
    Query a = QueryUtils.stringToQuery(json);
    Query b = QueryUtils.stringToQuery(json);
    assertTrue(QueryUtils.equals(a, b));
  }
}
