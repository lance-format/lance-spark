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

import org.lance.spark.utils.Optional;

import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class FilterPushDownTest {
  @Test
  public void testCompileFiltersToSqlWhereClause() {
    // Test case 1: GreaterThan, LessThanOrEqual, IsNotNull
    Predicate[] filters1 =
        new Predicate[] {
          TestPredicates.gt("age", 30),
          TestPredicates.lte("salary", 100000),
          TestPredicates.isNotNull("name")
        };
    Optional<String> whereClause1 = FilterPushDown.compileFiltersToSqlWhereClause(filters1);
    assertTrue(whereClause1.isPresent());
    assertEquals("(age > 30) AND (salary <= 100000) AND (name IS NOT NULL)", whereClause1.get());

    // Test case 2: GreaterThan, StringContains, LessThan
    Predicate[] filters2 =
        new Predicate[] {
          TestPredicates.gt("age", 30),
          TestPredicates.contains("name", "John"),
          TestPredicates.lt("salary", 50000)
        };
    Optional<String> whereClause2 = FilterPushDown.compileFiltersToSqlWhereClause(filters2);
    assertTrue(whereClause2.isPresent());
    assertEquals("(age > 30) AND (salary < 50000)", whereClause2.get());

    // Test case 3: Empty filters array
    Predicate[] filters3 = new Predicate[] {};
    Optional<String> whereClause3 = FilterPushDown.compileFiltersToSqlWhereClause(filters3);
    assertFalse(whereClause3.isPresent());

    // Test case 4: Mixed supported and unsupported filters
    Predicate[] filters4 =
        new Predicate[] {
          TestPredicates.gt("age", 30),
          TestPredicates.contains("name", "John"),
          TestPredicates.isNull("address"),
          TestPredicates.eq("country", "USA")
        };
    Optional<String> whereClause4 = FilterPushDown.compileFiltersToSqlWhereClause(filters4);
    assertTrue(whereClause4.isPresent());
    assertEquals("(age > 30) AND (address IS NULL) AND (country == 'USA')", whereClause4.get());

    // Test case 5: Not, Or, And combinations
    Predicate[] filters5 =
        new Predicate[] {
          TestPredicates.not(TestPredicates.gt("age", 30)),
          TestPredicates.or(TestPredicates.isNotNull("name"), TestPredicates.isNull("address")),
          TestPredicates.and(
              TestPredicates.lt("salary", 100000), TestPredicates.gte("salary", 50000))
        };
    Optional<String> whereClause5 = FilterPushDown.compileFiltersToSqlWhereClause(filters5);
    assertTrue(whereClause5.isPresent());
    assertEquals(
        "(NOT (age > 30)) AND ((name IS NOT NULL) OR (address IS NULL)) AND ((salary < 100000) AND (salary >= 50000))",
        whereClause5.get());
  }

  @Test
  public void testCompileFiltersToSqlWhereClauseWithEmptyFilters() {
    Predicate[] filters = new Predicate[] {};

    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertFalse(whereClause.isPresent());
  }

  @Test
  public void testIntegerInFilterPushDown() {
    Object[] values = new Object[2];
    values[0] = 500;
    values[1] = 600;
    Predicate[] filters =
        new Predicate[] {TestPredicates.gt("age", 30), TestPredicates.in("salary", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals("(age > 30) AND (salary IN (500,600))", whereClause.get());
  }

  @Test
  public void testStringInFilterPushDown() {
    Object[] values = new Object[2];
    values[0] = "500";
    values[1] = "600";
    Predicate[] filters =
        new Predicate[] {TestPredicates.gt("age", 30), TestPredicates.in("salary", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals("(age > 30) AND (salary IN ('500','600'))", whereClause.get());
  }

  @Test
  public void testStringValueWithSingleQuoteEscaping() {
    Predicate[] filters = new Predicate[] {TestPredicates.eq("name", "O'Brien")};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals("(name == 'O''Brien')", whereClause.get());
  }

  @Test
  public void testStringInFilterWithSingleQuoteEscaping() {
    Object[] values = new Object[] {"O'Brien", "D'Angelo"};
    Predicate[] filters = new Predicate[] {TestPredicates.in("name", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals("(name IN ('O''Brien','D''Angelo'))", whereClause.get());
  }

  @Test
  public void testDecimalFilterPushDown() {
    // Decimal comparisons must use CAST so Lance's DataFusion parser produces Decimal128,
    // not Float64, which would fail type resolution against Decimal columns.
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.gte("net_profit", new BigDecimal("100.00")),
          TestPredicates.lte("net_profit", new BigDecimal("200.00"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(net_profit >= CAST(100.00 AS DECIMAL(5, 2))) AND (net_profit <= CAST(200.00 AS DECIMAL(5, 2)))",
        whereClause.get());
  }

  @Test
  public void testDecimalInFilterPushDown() {
    Object[] values =
        new Object[] {new BigDecimal("100.00"), new BigDecimal("150.00"), new BigDecimal("200.00")};
    Predicate[] filters = new Predicate[] {TestPredicates.in("price", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(price IN (CAST(100.00 AS DECIMAL(5, 2)),CAST(150.00 AS DECIMAL(5, 2)),CAST(200.00 AS DECIMAL(5, 2))))",
        whereClause.get());
  }

  @Test
  public void testDecimalWithVaryingScaleAndPrecision() {
    // Verify precision/scale are taken from the BigDecimal value itself
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.gt("amount", new BigDecimal("1234567.89")),
          TestPredicates.lt("amount", new BigDecimal("0.5"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(amount > CAST(1234567.89 AS DECIMAL(9, 2))) AND (amount < CAST(0.5 AS DECIMAL(1, 1)))",
        whereClause.get());
  }

  @Test
  public void testDecimalZeroValuePrecisionClamped() {
    // Java's BigDecimal returns precision=1 for zero regardless of scale, e.g.
    // new BigDecimal("0.00") has precision=1 and scale=2. Arrow rejects DECIMAL(1,2) because
    // scale > precision is invalid. The fix clamps: precision = max(precision, scale).
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.gt("net_paid", new BigDecimal("0.00")),
          TestPredicates.gt("net_profit", new BigDecimal("1.00"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(net_paid > CAST(0.00 AS DECIMAL(2, 2))) AND (net_profit > CAST(1.00 AS DECIMAL(3, 2)))",
        whereClause.get());
  }

  @Test
  public void testDateFilterPushDown() {
    // Date literals must use the 'date' keyword so Lance's DataFusion parser produces Date32,
    // not Utf8, which would fail type resolution against Date columns.
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.gte("d_date", Date.valueOf("2000-08-23")),
          TestPredicates.lte("d_date", Date.valueOf("2000-09-06"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(d_date >= date '2000-08-23') AND (d_date <= date '2000-09-06')", whereClause.get());
  }

  @Test
  public void testTimestampFilterPushDown() {
    // Timestamp literals must use the 'timestamp' keyword so Lance's DataFusion parser produces
    // Timestamp, not Utf8.
    Predicate[] filters =
        new Predicate[] {
          TestPredicates.eq("created_at", Timestamp.valueOf("2024-01-15 10:30:00.0"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters);
    assertTrue(whereClause.isPresent());
    assertEquals("(created_at == timestamp '2024-01-15 10:30:00.0')", whereClause.get());
  }

  @Test
  public void testTimeTypeFilterRejected() throws Exception {
    // TimeType literals must be rejected from pushdown because Lance's DataFusion planner does
    // not yet support SQLDataType::Time in parse_type(). TimeType exists only in Spark 4.1+,
    // so we construct it via reflection and skip the assertion on older versions.
    org.apache.spark.sql.types.DataType timeType;
    try {
      Class<?> timeTypeClass = Class.forName("org.apache.spark.sql.types.TimeType");
      timeType =
          (org.apache.spark.sql.types.DataType)
              timeTypeClass.getDeclaredConstructor(int.class).newInstance(6);
    } catch (ClassNotFoundException e) {
      return; // Spark < 4.1: nothing to test.
    }

    org.apache.spark.sql.connector.expressions.LiteralValue<Long> timeLit =
        new org.apache.spark.sql.connector.expressions.LiteralValue<>(0L, timeType);
    Predicate timeGt =
        new Predicate(
            ">",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              org.apache.spark.sql.connector.expressions.FieldReference.apply("t"), timeLit
            });
    assertFalse(FilterPushDown.isPredicateSupported(timeGt));

    Predicate intGt = TestPredicates.gt("age", 30);
    assertTrue(FilterPushDown.isPredicateSupported(intGt));
  }

  @Test
  public void testReferencesAnyMatchesTopLevelColumn() {
    Predicate onPayload = TestPredicates.gt("payload", 1L);
    assertTrue(FilterPushDown.referencesAny(onPayload, Collections.singleton("payload")));
    assertFalse(FilterPushDown.referencesAny(onPayload, Collections.singleton("id")));
  }
}
