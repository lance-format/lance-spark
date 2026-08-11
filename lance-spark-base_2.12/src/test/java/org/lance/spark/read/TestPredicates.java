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
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Test-only factory for building canonical V2 {@link Predicate} instances with the same shape Spark
 * produces via {@code SupportsPushDownV2Filters}.
 */
final class TestPredicates {
  private TestPredicates() {}

  static Predicate eq(String column, Object value) {
    return new Predicate("=", new Expression[] {FieldReference.apply(column), literalOf(value)});
  }

  /**
   * Builds an equality predicate whose literal is a raw {@code TimestampType} value carrying {@code
   * micros}-since-epoch UTC, exactly as Spark hands timestamps to V2 pushdown. Unlike {@link
   * #eq(String, Object)} with a {@link Timestamp}, the value is not derived from the JVM default
   * time zone, so it pins time-zone-independent rendering.
   */
  static Predicate eqTimestampMicros(String column, long micros) {
    return cmpTimestampMicros("=", column, micros);
  }

  /**
   * Like {@link #eqTimestampMicros(String, long)} but for any comparison operator ({@code "<"},
   * {@code ">="}, ...), so range/non-equality pushdown of {@code TimestampType} literals can be
   * exercised through the same raw {@code Long}-micros shape Spark produces.
   */
  static Predicate cmpTimestampMicros(String op, String column, long micros) {
    LiteralValue<Long> lit = new LiteralValue<>(micros, DataTypes.TimestampType);
    return new Predicate(op, new Expression[] {FieldReference.apply(column), lit});
  }

  static Predicate lt(String column, Object value) {
    return new Predicate("<", new Expression[] {FieldReference.apply(column), literalOf(value)});
  }

  static Predicate lte(String column, Object value) {
    return new Predicate("<=", new Expression[] {FieldReference.apply(column), literalOf(value)});
  }

  static Predicate gt(String column, Object value) {
    return new Predicate(">", new Expression[] {FieldReference.apply(column), literalOf(value)});
  }

  static Predicate gte(String column, Object value) {
    return new Predicate(">=", new Expression[] {FieldReference.apply(column), literalOf(value)});
  }

  static Predicate isNull(String column) {
    return new Predicate("IS_NULL", new Expression[] {FieldReference.apply(column)});
  }

  static Predicate isNotNull(String column) {
    return new Predicate("IS_NOT_NULL", new Expression[] {FieldReference.apply(column)});
  }

  static Predicate in(String column, Object... values) {
    Expression[] children = new Expression[values.length + 1];
    children[0] = FieldReference.apply(column);
    for (int i = 0; i < values.length; i++) {
      children[i + 1] = literalOf(values[i]);
    }
    return new Predicate("IN", children);
  }

  static Predicate contains(String column, String value) {
    return new Predicate(
        "CONTAINS", new Expression[] {FieldReference.apply(column), literalOf(value)});
  }

  static Predicate and(Predicate left, Predicate right) {
    return new And(left, right);
  }

  static Predicate or(Predicate left, Predicate right) {
    return new Or(left, right);
  }

  static Predicate not(Predicate child) {
    return new Not(child);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static LiteralValue literalOf(Object value) {
    if (value == null) {
      return new LiteralValue(null, DataTypes.NullType);
    }
    DataType type;
    Object normalized = value;
    if (value instanceof Long) {
      type = DataTypes.LongType;
    } else if (value instanceof Integer) {
      type = DataTypes.IntegerType;
    } else if (value instanceof Short) {
      type = DataTypes.ShortType;
    } else if (value instanceof Byte) {
      type = DataTypes.ByteType;
    } else if (value instanceof Double) {
      type = DataTypes.DoubleType;
    } else if (value instanceof Float) {
      type = DataTypes.FloatType;
    } else if (value instanceof Boolean) {
      type = DataTypes.BooleanType;
    } else if (value instanceof String) {
      type = DataTypes.StringType;
      normalized = UTF8String.fromString((String) value);
    } else if (value instanceof UTF8String) {
      type = DataTypes.StringType;
    } else if (value instanceof BigDecimal) {
      BigDecimal bd = (BigDecimal) value;
      int scale = bd.scale();
      int precision = Math.max(bd.precision(), scale);
      type = DataTypes.createDecimalType(precision, scale);
      normalized = Decimal.apply(bd);
    } else if (value instanceof Date) {
      // Spark V2 represents Date as int epoch days
      type = DataTypes.DateType;
      normalized = (int) ((Date) value).toLocalDate().toEpochDay();
    } else if (value instanceof Timestamp) {
      // Spark V2 represents Timestamp as long epoch micros
      type = DataTypes.TimestampType;
      Timestamp ts = (Timestamp) value;
      long seconds = ts.toInstant().getEpochSecond();
      long nanos = ts.toInstant().getNano();
      normalized = seconds * 1_000_000L + nanos / 1_000L;
    } else {
      throw new IllegalArgumentException("Unsupported literal type: " + value.getClass().getName());
    }
    return new LiteralValue(normalized, type);
  }
}
