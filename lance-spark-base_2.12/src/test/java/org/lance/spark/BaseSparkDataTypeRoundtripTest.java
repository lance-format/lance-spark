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
package org.lance.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end write/read round-trip coverage for the Spark {@code DataType}s that previously had no
 * E2E safety net. Each test writes a small DataFrame through the Lance data source and reads it
 * back, asserting value equality including the null case.
 *
 * <p>Tests for known Lance-core gaps are {@code @Disabled} with a pointer to the upstream source.
 * Remove the annotation to verify a future fix.
 */
public abstract class BaseSparkDataTypeRoundtripTest {
  private static SparkSession spark;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    // getOrCreate() reuses any SparkSession already active in this JVM fork, so builder
    // .config(...) calls here would be silently ignored. Individual tests must not depend on SQL
    // session settings such as timezone or ANSI mode.
    spark =
        SparkSession.builder()
            .appName("lance-datatype-roundtrip-test")
            .master("local")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private String outputPath(String name) {
    return tempDir.resolve(name + ".lance").toString();
  }

  private Dataset<Row> writeAndRead(StructType schema, List<Row> rows, String name) {
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.write().format("lance").mode(SaveMode.ErrorIfExists).save(outputPath(name));
    return spark.read().format("lance").load(outputPath(name));
  }

  // ---------------- primitive scalars ----------------

  @Test
  public void testBooleanRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("flag", DataTypes.BooleanType, true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, true), RowFactory.create(1, false), RowFactory.create(2, null));
    List<Row> out = writeAndRead(schema, data, "bool").orderBy("id").collectAsList();
    assertEquals(3, out.size());
    assertTrue(out.get(0).getBoolean(1));
    assertFalse(out.get(1).getBoolean(1));
    assertTrue(out.get(2).isNullAt(1));
  }

  @Test
  public void testByteRoundtrip() {
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("v", DataTypes.ByteType, true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, (byte) 0),
            RowFactory.create(1, Byte.MIN_VALUE),
            RowFactory.create(2, Byte.MAX_VALUE),
            RowFactory.create(3, null));
    List<Row> out = writeAndRead(schema, data, "byte").orderBy("id").collectAsList();
    assertEquals((byte) 0, out.get(0).getByte(1));
    assertEquals(Byte.MIN_VALUE, out.get(1).getByte(1));
    assertEquals(Byte.MAX_VALUE, out.get(2).getByte(1));
    assertTrue(out.get(3).isNullAt(1));
  }

  @Test
  public void testShortRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("v", DataTypes.ShortType, true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, (short) 0),
            RowFactory.create(1, Short.MIN_VALUE),
            RowFactory.create(2, Short.MAX_VALUE),
            RowFactory.create(3, null));
    List<Row> out = writeAndRead(schema, data, "short").orderBy("id").collectAsList();
    assertEquals((short) 0, out.get(0).getShort(1));
    assertEquals(Short.MIN_VALUE, out.get(1).getShort(1));
    assertEquals(Short.MAX_VALUE, out.get(2).getShort(1));
    assertTrue(out.get(3).isNullAt(1));
  }

  @Test
  public void testDecimalSmallRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("v", DataTypes.createDecimalType(10, 2), true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, new BigDecimal("0.00")),
            RowFactory.create(1, new BigDecimal("12345678.90")),
            RowFactory.create(2, new BigDecimal("-42.42")),
            RowFactory.create(3, null));
    List<Row> out = writeAndRead(schema, data, "decimal_small").orderBy("id").collectAsList();
    assertEquals(new BigDecimal("0.00"), out.get(0).getDecimal(1));
    assertEquals(new BigDecimal("12345678.90"), out.get(1).getDecimal(1));
    assertEquals(new BigDecimal("-42.42"), out.get(2).getDecimal(1));
    assertTrue(out.get(3).isNullAt(1));
  }

  @Test
  public void testDecimalSystemDefaultRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("v", DataTypes.createDecimalType(38, 18), true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, new BigDecimal("0.000000000000000000")),
            RowFactory.create(1, new BigDecimal("123456789012345678.901234567890123456")),
            RowFactory.create(2, null));
    List<Row> out = writeAndRead(schema, data, "decimal_default").orderBy("id").collectAsList();
    assertEquals(new BigDecimal("0.000000000000000000"), out.get(0).getDecimal(1));
    assertEquals(new BigDecimal("123456789012345678.901234567890123456"), out.get(1).getDecimal(1));
    assertTrue(out.get(2).isNullAt(1));
  }

  // ---------------- date / time ----------------

  @Test
  public void testDateRoundtrip() {
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("d", DataTypes.DateType, true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, Date.valueOf("2020-01-01")),
            RowFactory.create(1, Date.valueOf("1970-01-01")),
            RowFactory.create(2, Date.valueOf("2100-12-31")),
            RowFactory.create(3, null));
    List<Row> out = writeAndRead(schema, data, "date").orderBy("id").collectAsList();
    assertEquals(Date.valueOf("2020-01-01"), out.get(0).getDate(1));
    assertEquals(Date.valueOf("1970-01-01"), out.get(1).getDate(1));
    assertEquals(Date.valueOf("2100-12-31"), out.get(2).getDate(1));
    assertTrue(out.get(3).isNullAt(1));
  }

  @Test
  public void testTimestampRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("ts", DataTypes.TimestampType, true);
    Timestamp t1 = Timestamp.valueOf("2024-06-15 12:34:56.789");
    Timestamp t2 = Timestamp.valueOf("1970-01-01 00:00:00.0");
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, t1), RowFactory.create(1, t2), RowFactory.create(2, null));
    List<Row> out = writeAndRead(schema, data, "ts").orderBy("id").collectAsList();
    assertEquals(t1, out.get(0).getTimestamp(1));
    assertEquals(t2, out.get(1).getTimestamp(1));
    assertTrue(out.get(2).isNullAt(1));
  }

  @Test
  public void testTimestampNtzRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("ts", DataTypes.TimestampNTZType, true);
    LocalDateTime t1 = LocalDateTime.of(2024, 6, 15, 12, 34, 56, 789_000_000);
    LocalDateTime t2 = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, t1), RowFactory.create(1, t2), RowFactory.create(2, null));
    List<Row> out = writeAndRead(schema, data, "ts_ntz").orderBy("id").collectAsList();
    assertEquals(t1, out.get(0).<LocalDateTime>getAs(1));
    assertEquals(t2, out.get(1).<LocalDateTime>getAs(1));
    assertTrue(out.get(2).isNullAt(1));
  }

  // ---------------- interval ----------------

  /**
   * Lance core's {@code TryFrom<&DataType> for LogicalType} has no branch for any Arrow {@code
   * Interval} variant, so {@code Interval(YearMonth)} falls through to the catch-all error at
   * {@code lance-core/src/datatypes.rs:229-231}. The same gap also blocks {@code
   * CalendarIntervalType} (which maps to {@code Interval(MonthDayNano)}).
   *
   * <p>TODO(lance-core): re-enable once Lance core accepts Arrow {@code Interval(*)}.
   */
  @Disabled("blocked on Lance core: lance-core/src/datatypes.rs rejects Arrow Interval(YearMonth)")
  @Test
  public void testYearMonthIntervalRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("ym", DataTypes.createYearMonthIntervalType(), true);
    // java.time.Period with only years/months (Spark YM interval does not carry days).
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, Period.ofYears(2).plusMonths(3)),
            RowFactory.create(1, Period.ofMonths(-5)),
            RowFactory.create(2, null));
    List<Row> out = writeAndRead(schema, data, "ym_interval").orderBy("id").collectAsList();
    assertEquals(3, out.size());
    assertEquals(Period.ofYears(2).plusMonths(3), out.get(0).get(1));
    assertEquals(Period.ofMonths(-5), out.get(1).get(1));
    assertTrue(out.get(2).isNullAt(1));
  }

  @Test
  public void testDayTimeIntervalRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("dt", DataTypes.createDayTimeIntervalType(), true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, Duration.ofDays(1).plusHours(2).plusMinutes(3)),
            RowFactory.create(1, Duration.ofSeconds(-90)),
            RowFactory.create(2, null));
    List<Row> out = writeAndRead(schema, data, "dt_interval").orderBy("id").collectAsList();
    assertEquals(Duration.ofDays(1).plusHours(2).plusMinutes(3), out.get(0).get(1));
    assertEquals(Duration.ofSeconds(-90), out.get(1).get(1));
    assertTrue(out.get(2).isNullAt(1));
  }

  // ---------------- null ----------------

  @Test
  public void testNullTypeColumnRoundtrip() {
    // Writing a NullType column crashes the JVM fork (SIGABRT / exit 134) in native Lance on
    // Spark 3.4, but works on 3.5 / 4.0 / 4.1. Skip on 3.4 rather than take the fork down.
    //
    // TODO(spark-3.4): fix the native crash and remove this guard. To reproduce, run just this
    // test on the lance-spark-3.4_2.12 module; the fork exits before any assertion runs.
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4"),
        "NullType column crashes the JVM on Spark 3.4 (native Lance bug); verified 3.5+/4.x work");
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("n", DataTypes.NullType, true);
    List<Row> data =
        Arrays.asList(RowFactory.create(0, (Object) null), RowFactory.create(1, (Object) null));
    List<Row> out = writeAndRead(schema, data, "null_col").orderBy("id").collectAsList();
    assertEquals(2, out.size());
    assertTrue(out.get(0).isNullAt(1));
    assertTrue(out.get(1).isNullAt(1));
  }

  // ---------------- array ----------------

  @Test
  public void testArrayOfStringRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("tags", DataTypes.createArrayType(DataTypes.StringType, true), true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, Arrays.asList("a", "b", "c")),
            RowFactory.create(1, Collections.emptyList()),
            RowFactory.create(2, Arrays.asList("only-one")),
            RowFactory.create(3, null));
    List<Row> out = writeAndRead(schema, data, "arr_str").orderBy("id").collectAsList();
    assertEquals(Arrays.asList("a", "b", "c"), out.get(0).getList(1));
    assertTrue(out.get(1).getList(1).isEmpty());
    assertEquals(Arrays.asList("only-one"), out.get(2).getList(1));
    assertTrue(out.get(3).isNullAt(1));
  }

  @Test
  public void testNestedArrayOfIntRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add(
                "matrix",
                DataTypes.createArrayType(
                    DataTypes.createArrayType(DataTypes.IntegerType, true), true),
                true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))),
            RowFactory.create(1, null));
    List<Row> out = writeAndRead(schema, data, "arr_nested").orderBy("id").collectAsList();
    assertEquals(2, out.size());
    // Row.getList converts only the outer level; inner elements remain Scala Seqs and must be
    // normalised via toJavaList before comparing to java.util.List.
    List<?> row0 = out.get(0).getList(1);
    assertEquals(2, row0.size());
    assertEquals(Arrays.asList(1, 2, 3), toJavaList(row0.get(0)));
    assertEquals(Arrays.asList(4, 5, 6), toJavaList(row0.get(1)));
    assertTrue(out.get(1).isNullAt(1));
  }

  /**
   * Convert a Scala {@code Seq} (as returned for inner elements of a nested Spark array) into a
   * {@code java.util.List} via reflection, so this base-test stays Scala-version agnostic. Tries
   * {@code WrappedArray.array()} first, then falls back to {@code Seq.iterator()} for Scala 2.13
   * {@code ArraySeq} which has no {@code array()} accessor.
   */
  @SuppressWarnings("unchecked")
  private static List<Object> toJavaList(Object sparkSeq) {
    if (sparkSeq instanceof List) {
      return (List<Object>) sparkSeq;
    }
    try {
      Object arr = sparkSeq.getClass().getMethod("array").invoke(sparkSeq);
      int len = java.lang.reflect.Array.getLength(arr);
      java.util.ArrayList<Object> out = new java.util.ArrayList<>(len);
      for (int i = 0; i < len; i++) {
        out.add(java.lang.reflect.Array.get(arr, i));
      }
      return out;
    } catch (ReflectiveOperationException arrayAttempt) {
      try {
        Object iter = sparkSeq.getClass().getMethod("iterator").invoke(sparkSeq);
        java.util.ArrayList<Object> out = new java.util.ArrayList<>();
        java.lang.reflect.Method hasNext = iter.getClass().getMethod("hasNext");
        java.lang.reflect.Method next = iter.getClass().getMethod("next");
        while ((boolean) hasNext.invoke(iter)) {
          out.add(next.invoke(iter));
        }
        return out;
      } catch (ReflectiveOperationException iteratorAttempt) {
        throw new AssertionError(
            "Cannot convert Spark nested array element of class "
                + sparkSeq.getClass().getName()
                + " (array() attempt: "
                + arrayAttempt
                + ")",
            iteratorAttempt);
      }
    }
  }

  // ---------------- binary ----------------

  @Test
  public void testBinaryRoundtrip() {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("data", DataTypes.BinaryType, true);
    byte[] b0 = new byte[] {1, 2, 3, 4};
    byte[] b1 = new byte[] {};
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, b0), RowFactory.create(1, b1), RowFactory.create(2, null));
    List<Row> out = writeAndRead(schema, data, "bin").orderBy("id").collectAsList();
    assertArrayEquals(b0, (byte[]) out.get(0).get(1));
    assertArrayEquals(b1, (byte[]) out.get(1).get(1));
    assertTrue(out.get(2).isNullAt(1));
  }
}
