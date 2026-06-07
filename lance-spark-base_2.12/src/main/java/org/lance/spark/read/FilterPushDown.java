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

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterPushDown {
  /**
   * Create SQL 'where clause' from Spark V2 predicates.
   *
   * @param predicates Supported V2 predicates
   * @return where clause, or Optional.empty() if no predicates compile to SQL
   */
  public static Optional<String> compileFiltersToSqlWhereClause(Predicate[] predicates) {
    if (predicates.length == 0) {
      return Optional.empty();
    }
    List<String> compiled = new ArrayList<>();
    for (Predicate predicate : predicates) {
      compilePredicate(predicate).ifPresent(compiled::add);
    }
    if (compiled.isEmpty()) {
      return Optional.empty();
    }
    String whereClause =
        compiled.stream().map(p -> "(" + p + ")").collect(Collectors.joining(" AND "));
    return Optional.of(whereClause);
  }

  /** Returns true if {@code predicate} references any of the given top-level columns. */
  public static boolean referencesAny(Predicate predicate, Set<String> columns) {
    if (columns.isEmpty()) {
      return false;
    }
    for (NamedReference ref : predicate.references()) {
      String[] names = ref.fieldNames();
      if (names.length > 0 && columns.contains(names[0])) {
        return true;
      }
    }
    return false;
  }

  public static boolean isPredicateSupported(Predicate predicate) {
    if (predicate instanceof And) {
      And and = (And) predicate;
      return isPredicateSupported(and.left()) && isPredicateSupported(and.right());
    }
    if (predicate instanceof Or) {
      Or or = (Or) predicate;
      return isPredicateSupported(or.left()) && isPredicateSupported(or.right());
    }
    if (predicate instanceof Not) {
      Not not = (Not) predicate;
      return isPredicateSupported(not.child());
    }
    switch (predicate.name()) {
      case "=":
      case "<":
      case "<=":
      case ">":
      case ">=":
      case "IS_NULL":
      case "IS_NOT_NULL":
      case "IN":
        return isColumnLiteralShape(predicate);
      default:
        return false;
    }
  }

  private static boolean isColumnLiteralShape(Predicate predicate) {
    Expression[] children = predicate.children();
    if (children.length == 0) {
      return false;
    }
    if (!(children[0] instanceof NamedReference)) {
      return false;
    }
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal)) {
        return false;
      }
      if (isTimeTypeLiteral((Literal<?>) children[i])) {
        // Lance's custom SQL planner (lance-datafusion/src/planner.rs) does not yet support
        // SQLDataType::Time in parse_type(), so TimeType literals can't be pushed down. Reject
        // and let Spark evaluate the predicate post-scan.
        return false;
      }
    }
    return true;
  }

  /**
   * Detects a Spark TimeType literal via class name to stay version-safe — {@code TimeType} only
   * exists in Spark 4.1+, so a direct {@code instanceof TimeType} would not compile against older
   * Spark versions in the base module.
   */
  private static boolean isTimeTypeLiteral(Literal<?> literal) {
    return literal.dataType() != null
        && "org.apache.spark.sql.types.TimeType".equals(literal.dataType().getClass().getName());
  }

  private static Optional<String> compilePredicate(Predicate predicate) {
    if (predicate instanceof And) {
      And and = (And) predicate;
      Optional<String> left = compilePredicate(and.left());
      Optional<String> right = compilePredicate(and.right());
      if (left.isEmpty()) return right;
      if (right.isEmpty()) return left;
      return Optional.of(String.format("(%s) AND (%s)", left.get(), right.get()));
    }
    if (predicate instanceof Or) {
      Or or = (Or) predicate;
      Optional<String> left = compilePredicate(or.left());
      Optional<String> right = compilePredicate(or.right());
      if (left.isEmpty()) return right;
      if (right.isEmpty()) return left;
      return Optional.of(String.format("(%s) OR (%s)", left.get(), right.get()));
    }
    if (predicate instanceof Not) {
      Not not = (Not) predicate;
      Optional<String> child = compilePredicate(not.child());
      if (child.isEmpty()) return child;
      return Optional.of(String.format("NOT (%s)", child.get()));
    }

    Expression[] children = predicate.children();
    switch (predicate.name()) {
      case "=":
        return binaryOp(children, "==");
      case "<":
        return binaryOp(children, "<");
      case "<=":
        return binaryOp(children, "<=");
      case ">":
        return binaryOp(children, ">");
      case ">=":
        return binaryOp(children, ">=");
      case "IS_NULL":
        return Optional.of(String.format("%s IS NULL", columnName(children[0])));
      case "IS_NOT_NULL":
        return Optional.of(String.format("%s IS NOT NULL", columnName(children[0])));
      case "IN":
        String values =
            java.util.Arrays.stream(children)
                .skip(1)
                .map(c -> compileLiteral(((Literal<?>) c)))
                .collect(Collectors.joining(","));
        return Optional.of(String.format("%s IN (%s)", columnName(children[0]), values));
      default:
        return Optional.empty();
    }
  }

  private static Optional<String> binaryOp(Expression[] children, String op) {
    if (children.length != 2) {
      return Optional.empty();
    }
    return Optional.of(
        columnName(children[0]) + " " + op + " " + compileLiteral((Literal<?>) children[1]));
  }

  private static String columnName(Expression expr) {
    return String.join(".", ((NamedReference) expr).fieldNames());
  }

  private static String compileLiteral(Literal<?> literal) {
    Object value = literal.value();
    if (value == null) {
      return "NULL";
    }
    if (literal.dataType() == DataTypes.DateType && value instanceof Integer) {
      return "date '" + java.time.LocalDate.ofEpochDay((Integer) value) + "'";
    }
    if (literal.dataType() == DataTypes.TimestampType && value instanceof Long) {
      long micros = (Long) value;
      java.time.Instant instant =
          java.time.Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1_000);
      return "timestamp '" + Timestamp.from(instant) + "'";
    }
    if (value instanceof Date) {
      return "date '" + value.toString().replace("'", "''") + "'";
    }
    if (value instanceof Timestamp) {
      return "timestamp '" + value.toString().replace("'", "''") + "'";
    }
    if (value instanceof org.apache.spark.unsafe.types.UTF8String) {
      return "'" + value.toString().replace("'", "''") + "'";
    }
    if (value instanceof String) {
      return "'" + ((String) value).replace("'", "''") + "'";
    }
    if (value instanceof org.apache.spark.sql.types.Decimal) {
      return formatDecimalCast(((org.apache.spark.sql.types.Decimal) value).toJavaBigDecimal());
    }
    if (value instanceof BigDecimal) {
      return formatDecimalCast((BigDecimal) value);
    }
    return value.toString();
  }

  private static String formatDecimalCast(BigDecimal bd) {
    int scale = bd.scale();
    int precision = Math.max(bd.precision(), scale);
    return "CAST(" + bd.toPlainString() + " AS DECIMAL(" + precision + ", " + scale + "))";
  }
}
