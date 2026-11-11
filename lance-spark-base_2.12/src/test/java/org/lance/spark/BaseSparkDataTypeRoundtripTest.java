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

import org.lance.spark.arrow.LanceArrowWriter;
import org.lance.spark.utils.Float16Utils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.linalg.MatrixUDT;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  public void testDecimalFilterPushDownEndToEnd() {
    // Regression test for the Decimal CAST wrapper that gets pushed to Lance's DataFusion parser.
    // Spark V2 LiteralValue stores decimals as Catalyst's internal Decimal (not BigDecimal), so
    // without an explicit CAST, bare numeric literals are inferred as Float64 and fail type
    // resolution against Decimal128 columns. This drives a real Spark predicate pushdown — unlike
    // FilterPushDownTest, which builds synthetic predicates via TestPredicates.
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("v", DataTypes.createDecimalType(10, 2), true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, new BigDecimal("50.00")),
            RowFactory.create(1, new BigDecimal("100.00")),
            RowFactory.create(2, new BigDecimal("250.50")),
            RowFactory.create(3, null));
    List<Row> out =
        writeAndRead(schema, data, "decimal_pushdown")
            .filter("v >= 100.00")
            .orderBy("id")
            .collectAsList();
    assertEquals(2, out.size());
    assertEquals(1, out.get(0).getInt(0));
    assertEquals(new BigDecimal("100.00"), out.get(0).getDecimal(1));
    assertEquals(2, out.get(1).getInt(0));
    assertEquals(new BigDecimal("250.50"), out.get(1).getDecimal(1));
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

  // ---------------- UDT (UserDefinedType) ----------------

  /**
   * VectorUDT is MLlib's {@link org.apache.spark.ml.linalg.VectorUDT} — the most common UDT in the
   * Spark ecosystem (used for ML feature vectors). Its {@code sqlType} is a 4-field StructType:
   * {@code (type: ByteType, size: IntegerType, indices: ArrayType(IntegerType), values:
   * ArrayType(DoubleType))}.
   *
   * <p>Arrow has no UDT concept, so the read-back schema loses the UDT wrapper and returns a plain
   * {@code StructType}. Data values are intact; {@link VectorUDT#deserialize(Object)} expects an
   * {@code InternalRow}, so in a test context (where Spark hands back public {@link Row}s) the
   * helper reconstructs vectors manually.
   *
   * <p>Null parent rows are covered separately in {@link #testVectorUDTNullRowRoundtrip}.
   */
  @Test
  public void testVectorUDTRoundtrip() {
    VectorUDT vectorUDT = new VectorUDT();
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("vec", vectorUDT, true);

    Vector dense = new DenseVector(new double[] {1.0, 2.0, 3.0});
    Vector sparse = new SparseVector(3, new int[] {0, 2}, new double[] {1.0, 3.0});

    List<Row> data = Arrays.asList(RowFactory.create(0, dense), RowFactory.create(1, sparse));

    Dataset<Row> result = writeAndRead(schema, data, "vector_udt");

    // Write stamps the UDT FQN onto the Arrow field's `__udt` metadata; read consults it and
    // rewraps the column back to VectorUDT. End-to-end the UDT survives without any opt-in.
    assertInstanceOf(
        VectorUDT.class,
        result.schema().apply("vec").dataType(),
        "read-back column must carry VectorUDT (preserved via __udt field metadata)");

    // The `__udt` marker is consumed by the schema converter and stripped from the user-visible
    // StructField metadata — surfacing it would be noise once the dataType is already the UDT.
    assertFalse(
        result.schema().apply("vec").metadata().contains("__udt"),
        "the __udt marker must not leak into user-visible StructField metadata");

    List<Row> out = result.orderBy("id").collectAsList();
    assertEquals(2, out.size());
    // Spark materializes UDT columns as the user-facing class — Vector here.
    assertEquals(dense, out.get(0).get(1));
    assertEquals(sparse, out.get(1).get(1));
  }

  /**
   * Round-trip for a null VectorUDT row. Used to fail because VectorUDT's sqlType marks the inner
   * {@code type} field as non-nullable; see {@code LanceArrowUtils.toArrowField} for the schema
   * relaxation that fixes it.
   */
  @Test
  public void testVectorUDTNullRowRoundtrip() {
    VectorUDT vectorUDT = new VectorUDT();
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("vec", vectorUDT, true);

    Vector dense = new DenseVector(new double[] {1.0, 2.0, 3.0});
    List<Row> data =
        Arrays.asList(RowFactory.create(0, dense), RowFactory.create(1, (Object) null));

    List<Row> out = writeAndRead(schema, data, "vector_udt_null_row").orderBy("id").collectAsList();
    assertEquals(2, out.size());
    assertEquals(dense, out.get(0).get(1));
    assertTrue(out.get(1).isNullAt(1), "row 1 should round-trip as null");
  }

  /**
   * Idempotency: reading a UDT and writing it back must yield a dataset whose schema and rows are
   * indistinguishable from the original. Guards against subtle metadata duplication or loss in the
   * __udt round-trip — e.g., if `buildFieldMetadata` leaked `__udt` into Spark metadata, the second
   * write would write it twice (once via metadata, once via `udt.getClass.getName`).
   */
  @Test
  public void testVectorUDTReWriteRoundtrip() {
    VectorUDT vectorUDT = new VectorUDT();
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("vec", vectorUDT, true);
    Vector dense = new DenseVector(new double[] {1.0, 2.0, 3.0});
    List<Row> data = Arrays.asList(RowFactory.create(0, dense));

    Dataset<Row> firstRead = writeAndRead(schema, data, "vector_udt_rewrite_v1");
    String rewritePath = outputPath("vector_udt_rewrite_v2");
    firstRead.write().format("lance").save(rewritePath);

    Dataset<Row> secondRead = spark.read().format("lance").load(rewritePath);
    assertInstanceOf(
        VectorUDT.class,
        secondRead.schema().apply("vec").dataType(),
        "UDT must survive a read+write+read cycle (idempotency)");
    assertFalse(
        secondRead.schema().apply("vec").metadata().contains("__udt"),
        "second-read StructField metadata must still be clean");
    assertEquals(dense, secondRead.orderBy("id").collectAsList().get(0).get(1));
  }

  /**
   * Generality check: the round-trip works for any {@code UserDefinedType}, not only {@code
   * VectorUDT}. {@code MatrixUDT} has a different sqlType (7-field struct vs. 4-field) and a
   * different user-facing class, so it exercises the same code path with different shape.
   */
  @Test
  public void testMatrixUDTRoundtrip() {
    MatrixUDT matrixUDT = new MatrixUDT();
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("mat", matrixUDT, true);

    Matrix dense = new DenseMatrix(2, 2, new double[] {1.0, 2.0, 3.0, 4.0});
    List<Row> data = Arrays.asList(RowFactory.create(0, dense));

    Dataset<Row> result = writeAndRead(schema, data, "matrix_udt");

    assertInstanceOf(
        MatrixUDT.class,
        result.schema().apply("mat").dataType(),
        "read-back column must carry MatrixUDT (preserved via __udt field metadata)");

    List<Row> out = result.orderBy("id").collectAsList();
    assertEquals(1, out.size());
    assertEquals(dense, out.get(0).get(1));
  }

  /**
   * UDT nested inside a StructType also round-trips. Exercises the recursive UDT branch in {@code
   * toArrowField} (the top-level case is covered by {@code testVectorUDTRoundtrip}).
   */
  @Test
  public void testNestedUDTInStructRoundtrip() {
    VectorUDT vectorUDT = new VectorUDT();
    StructType nested =
        new StructType().add("label", DataTypes.StringType, true).add("vec", vectorUDT, true);
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("payload", nested, true);

    Vector dense = new DenseVector(new double[] {1.0, 2.0});
    List<Row> data = Arrays.asList(RowFactory.create(0, RowFactory.create("hello", dense)));

    Dataset<Row> result = writeAndRead(schema, data, "nested_udt_struct");

    StructType payloadType = (StructType) result.schema().apply("payload").dataType();
    assertInstanceOf(
        VectorUDT.class,
        payloadType.apply("vec").dataType(),
        "nested vec field must carry VectorUDT after round-trip");

    List<Row> out = result.orderBy("id").collectAsList();
    Row payload = out.get(0).getStruct(1);
    assertEquals("hello", payload.getString(0));
    assertEquals(dense, payload.get(1));
  }

  /**
   * UDT used as an array element also round-trips. Exercises the recursive UDT branch through
   * {@code toArrowField}'s ArrayType handling.
   */
  @Test
  public void testUDTAsArrayElementRoundtrip() {
    VectorUDT vectorUDT = new VectorUDT();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("vecs", DataTypes.createArrayType(vectorUDT, true), true);

    Vector v1 = new DenseVector(new double[] {1.0, 2.0});
    Vector v2 = new DenseVector(new double[] {3.0, 4.0});
    List<Row> data = Arrays.asList(RowFactory.create(0, Arrays.asList(v1, v2)));

    Dataset<Row> result = writeAndRead(schema, data, "udt_array_element");

    org.apache.spark.sql.types.ArrayType arr =
        (org.apache.spark.sql.types.ArrayType) result.schema().apply("vecs").dataType();
    assertInstanceOf(
        VectorUDT.class,
        arr.elementType(),
        "array element type must carry VectorUDT after round-trip");

    List<Row> out = result.orderBy("id").collectAsList();
    List<Object> got = out.get(0).getList(1);
    assertEquals(2, got.size());
    assertEquals(v1, got.get(0));
    assertEquals(v2, got.get(1));
  }

  // ---------------- fixed-size-list with null ----------------

  @Test
  public void testOrdinaryInt8ArrayRemainsVariableLength() {
    assertEquals(
        ArrowType.List.INSTANCE,
        LanceArrowUtils.toArrowField(
                "values", DataTypes.createArrayType(DataTypes.ByteType), true, "UTC", null, false)
            .getType());
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("values", DataTypes.createArrayType(DataTypes.ByteType), true);
    List<Row> input =
        Arrays.asList(
            RowFactory.create(0, Collections.emptyList()),
            RowFactory.create(1, Arrays.asList((byte) 1, (byte) 2)),
            RowFactory.create(2, null));
    List<Row> rows = writeAndRead(schema, input, "variable_int8").orderBy("id").collectAsList();
    assertEquals(Collections.emptyList(), rows.get(0).getList(1));
    assertEquals(Arrays.asList((byte) 1, (byte) 2), rows.get(1).getList(1));
    assertTrue(rows.get(2).isNullAt(1));
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri(outputPath("variable_int8")).build();
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(options).build()) {
      assertEquals(ArrowType.List.INSTANCE, dataset.getSchema().findField("values").getType());
    }
  }

  static Stream<Arguments> fixedSizeNumericTypes() {
    return Stream.of(
        Arguments.of(
            "int8",
            new ArrowType.Int(8, true),
            "TINYINT",
            Arrays.asList(Byte.MIN_VALUE, null, (byte) 0, Byte.MAX_VALUE)),
        Arguments.of(
            "uint8",
            new ArrowType.Int(8, false),
            "SMALLINT",
            Arrays.asList((short) 0, null, (short) 128, (short) 255)),
        Arguments.of(
            "int16",
            new ArrowType.Int(16, true),
            "SMALLINT",
            Arrays.asList(Short.MIN_VALUE, null, (short) 0, Short.MAX_VALUE)),
        Arguments.of(
            "uint16", new ArrowType.Int(16, false), "INT", Arrays.asList(0, null, 32768, 65535)),
        Arguments.of(
            "int32",
            new ArrowType.Int(32, true),
            "INT",
            Arrays.asList(Integer.MIN_VALUE, null, 0, Integer.MAX_VALUE)),
        Arguments.of(
            "uint32",
            new ArrowType.Int(32, false),
            "BIGINT",
            Arrays.asList(0L, null, 2147483648L, 4294967295L)),
        Arguments.of(
            "int64",
            new ArrowType.Int(64, true),
            "BIGINT",
            Arrays.asList(Long.MIN_VALUE, null, 0L, Long.MAX_VALUE)),
        Arguments.of(
            "uint64",
            new ArrowType.Int(64, false),
            "BIGINT",
            Arrays.asList(0L, null, Long.MIN_VALUE, -1L)),
        Arguments.of(
            "float16",
            new ArrowType.FloatingPoint(FloatingPointPrecision.HALF),
            "FLOAT",
            Arrays.asList(-1.5f, null, 0.0f, 1.5f)),
        Arguments.of(
            "float32",
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
            "FLOAT",
            Arrays.asList(-1.5f, null, 0.0f, 1.5f)),
        Arguments.of(
            "float64",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
            "DOUBLE",
            Arrays.asList(-1.5d, null, 0.0d, 1.5d)),
        Arguments.of(
            "decimal128",
            new ArrowType.Decimal(10, 2, 128),
            "DECIMAL(10,2)",
            Arrays.asList(
                new BigDecimal("-99999999.99"),
                null,
                new BigDecimal("0.00"),
                new BigDecimal("99999999.99"))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("fixedSizeNumericTypes")
  public void testExistingFixedSizeNumericListWriteback(
      String name, ArrowType elementType, String sqlType, List<?> expected) throws Exception {
    if (name.equals("float16")) {
      Assumptions.assumeTrue(
          Float16Utils.isFloat2VectorAvailable(), "Float16 requires Arrow 18+ (Spark 4.0+)");
    }
    String uri = outputPath("fixed_" + name);
    Schema original =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "values",
                    FieldType.nullable(new ArrowType.FixedSizeList(4)),
                    Collections.singletonList(
                        new Field("item", FieldType.nullable(elementType), null)))));
    org.lance.Dataset.write()
        .allocator(LanceRuntime.allocator())
        .uri(uri)
        .schema(original)
        .execute()
        .close();
    spark
        .conf()
        .set("spark.sql.catalog.fixed_numeric", "org.lance.spark.LanceNamespaceSparkCatalog");
    String table = "fixed_numeric.`" + uri + "`";

    String elements =
        expected.stream()
            .map(value -> "CAST(" + (value == null ? "NULL" : value) + " AS " + sqlType + ")")
            .collect(Collectors.joining(", "));
    spark.sql(
        "INSERT INTO "
            + table
            + " VALUES (0, CAST(NULL AS ARRAY<"
            + sqlType
            + ">)), (1, array("
            + elements
            + "))");
    List<Row> rows = spark.table(table).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertTrue(rows.get(0).isNullAt(1));
    assertEquals(expected, rows.get(1).getList(1));

    spark.table(table).selectExpr("id + 2 AS id", "values").writeTo(table).append();
    List<Row> copied = spark.table(table).orderBy("id").collectAsList();
    assertEquals(4, copied.size());
    assertTrue(copied.get(2).isNullAt(1));
    assertEquals(rows.get(1).getList(1), copied.get(3).getList(1));

    LanceSparkReadOptions options = LanceSparkReadOptions.builder().datasetUri(uri).build();
    long version;
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(options).build()) {
      assertEquals(original, dataset.getSchema());
      assertEquals(4L, dataset.countRows());
      assertTrue(dataset.getFragments().size() >= 2);
      version = dataset.version();
    }
    for (String invalid : Arrays.asList("array()", "array(1, 2, 3)", "array(1, 2, 3, 4, 5)")) {
      Exception error =
          assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "INSERT INTO "
                          + table
                          + " SELECT 4, CAST("
                          + invalid
                          + " AS ARRAY<"
                          + sqlType
                          + ">)"));
      Throwable cause = error;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(
          cause.getMessage().contains("does not match FixedSizeList size 4"), cause.toString());
      try (org.lance.Dataset dataset = Utils.openDatasetBuilder(options).build()) {
        assertEquals(version, dataset.version());
        assertEquals(4L, dataset.countRows());
        assertEquals(original, dataset.getSchema());
      }
    }
  }

  static Stream<Arguments> fixedSizeNonNumericTypes() {
    Stream<Arguments> scalars =
        Stream.of(
            Arguments.of(
                "bool",
                new Field("item", FieldType.nullable(ArrowType.Bool.INSTANCE), null),
                "BOOLEAN",
                "false, NULL, true, false"),
            Arguments.of(
                "fixed_binary",
                new Field("item", FieldType.nullable(new ArrowType.FixedSizeBinary(2)), null),
                "BINARY",
                "X'00FF', NULL, X'0000', X'0102'"),
            Arguments.of(
                "date_day",
                new Field("item", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                "DATE",
                "DATE'1969-12-31', NULL, DATE'1970-01-01', DATE'2024-02-29'"),
            Arguments.of(
                "date_milli",
                new Field(
                    "item", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null),
                "DATE",
                "DATE'1969-12-31', NULL, DATE'1970-01-01', DATE'2024-02-29'"),
            Arguments.of(
                "duration",
                new Field(
                    "item", FieldType.nullable(new ArrowType.Duration(TimeUnit.MICROSECOND)), null),
                "INTERVAL DAY TO SECOND",
                "INTERVAL '-1' SECOND, NULL, INTERVAL '0' SECOND, INTERVAL '1 02:03:04.123456' DAY TO SECOND"));
    Stream<Arguments> timestamps =
        Arrays.stream(TimeUnit.values())
            .flatMap(
                unit ->
                    Stream.of(false, true)
                        .map(
                            withTimezone -> {
                              String sqlType = withTimezone ? "TIMESTAMP" : "TIMESTAMP_NTZ";
                              String fraction =
                                  unit == TimeUnit.SECOND
                                      ? ""
                                      : unit == TimeUnit.MILLISECOND ? ".123" : ".123456";
                              return Arguments.of(
                                  "timestamp_" + unit.name() + "_" + withTimezone,
                                  new Field(
                                      "item",
                                      FieldType.nullable(
                                          new ArrowType.Timestamp(
                                              unit, withTimezone ? "UTC" : null)),
                                      null),
                                  sqlType,
                                  "CAST('1969-12-31 23:59:59"
                                      + fraction
                                      + "' AS "
                                      + sqlType
                                      + "), NULL, "
                                      + "CAST('1970-01-01 00:00:00' AS "
                                      + sqlType
                                      + "), "
                                      + "CAST('2024-01-02 03:04:05"
                                      + fraction
                                      + "' AS "
                                      + sqlType
                                      + ")");
                            }));
    return Stream.concat(scalars, timestamps);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("fixedSizeNonNumericTypes")
  public void testExistingFixedSizeNonNumericListWriteback(
      String name, Field elementField, String sqlType, String elements) throws Exception {
    String uri = outputPath("fixed_" + name);
    Schema original =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(
                    "values",
                    FieldType.nullable(new ArrowType.FixedSizeList(4)),
                    Collections.singletonList(elementField))));
    org.lance.Dataset.write()
        .allocator(LanceRuntime.allocator())
        .uri(uri)
        .schema(original)
        .execute()
        .close();
    spark
        .conf()
        .set("spark.sql.catalog.fixed_nonnumeric", "org.lance.spark.LanceNamespaceSparkCatalog");
    String table = "fixed_nonnumeric.`" + uri + "`";
    String input =
        "SELECT id, CAST(values AS ARRAY<"
            + sqlType
            + ">) AS values FROM VALUES "
            + "(0, NULL), (1, array("
            + elements
            + ")), (2, array(NULL, NULL, NULL, NULL)), "
            + "(3, array("
            + elements
            + ")), (4, NULL) AS data(id, values)";
    Dataset<Row> expected = spark.sql(input);
    spark.sql("INSERT INTO " + table + " " + input);
    Dataset<Row> actual = spark.table(table);
    assertTrue(expected.exceptAll(actual).isEmpty(), "Missing or changed rows after insert");
    assertTrue(actual.exceptAll(expected).isEmpty(), "Unexpected rows after insert");

    // Small batches put null rows on either side of batch boundaries.
    spark
        .table(table)
        .selectExpr("id + 5 AS id", "values")
        .coalesce(1)
        .writeTo(table)
        .option("batch_size", "2")
        .append();
    Dataset<Row> copied = expected.union(expected.selectExpr("id + 5 AS id", "values"));
    Dataset<Row> appended = spark.table(table);
    assertTrue(copied.exceptAll(appended).isEmpty(), "Missing or changed rows after append");
    assertTrue(appended.exceptAll(copied).isEmpty(), "Unexpected rows after append");
    LanceSparkReadOptions options = LanceSparkReadOptions.builder().datasetUri(uri).build();
    long version;
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(options).build()) {
      assertEquals(original, dataset.getSchema());
      assertEquals(10L, dataset.countRows());
      assertTrue(dataset.getFragments().size() >= 2);
      version = dataset.version();
    }
    List<Map.Entry<String, String>> invalidArrays = new ArrayList<>();
    for (String invalid :
        Arrays.asList(
            "slice(values, 1, 0)", "slice(values, 1, 3)", "concat(values, slice(values, 1, 1))")) {
      invalidArrays.add(Map.entry(invalid, "does not match FixedSizeList size 4"));
    }
    if (elementField.getType() instanceof ArrowType.FixedSizeBinary) {
      for (String bytes : Arrays.asList("", "00", "000102")) {
        invalidArrays.add(
            Map.entry(
                "array(X'" + bytes + "', X'00FF', NULL, X'0000')",
                "expects 2 bytes per row, got " + bytes.length() / 2));
      }
    }
    for (Map.Entry<String, String> invalid : invalidArrays) {
      Exception error =
          assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "INSERT INTO "
                          + table
                          + " SELECT 10, "
                          + invalid.getKey()
                          + " FROM (SELECT CAST(array("
                          + elements
                          + ") AS ARRAY<"
                          + sqlType
                          + ">) AS values)"));
      Throwable cause = error;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(cause.getMessage().contains(invalid.getValue()), cause.toString());
      try (org.lance.Dataset dataset = Utils.openDatasetBuilder(options).build()) {
        assertEquals(version, dataset.version());
        assertEquals(10L, dataset.countRows());
        assertEquals(original, dataset.getSchema());
      }
    }
  }

  static Stream<Field> unsupportedFixedSizeListElements() {
    return Stream.of(
        new Field("item", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
        new Field("item", FieldType.nullable(ArrowType.LargeUtf8.INSTANCE), null),
        new Field("item", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
        new Field("item", FieldType.nullable(ArrowType.LargeBinary.INSTANCE), null),
        new Field(
            "item",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(
                new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null))),
        new Field(
            "item",
            FieldType.nullable(new ArrowType.FixedSizeList(2)),
            Collections.singletonList(
                new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unsupportedFixedSizeListElements")
  public void testUnsupportedFixedSizeListElementsRejected(Field element) {
    Schema schema =
        new Schema(
            Collections.singletonList(
                new Field(
                    "values",
                    FieldType.nullable(new ArrowType.FixedSizeList(4)),
                    Collections.singletonList(element))));
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, LanceRuntime.allocator())) {
      UnsupportedOperationException error =
          assertThrows(
              UnsupportedOperationException.class,
              () -> LanceArrowWriter.create(root, LanceArrowUtils.fromArrowSchema(schema)));
      assertTrue(error.getMessage().contains("Unsupported data type: ArrayType("));
    }
  }

  @Test
  public void testFixedSizeListNullMixed() {
    org.apache.spark.sql.types.Metadata meta =
        new org.apache.spark.sql.types.MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 4L)
            .build();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add(
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "vec", DataTypes.createArrayType(DataTypes.FloatType, false), true, meta));
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f)),
            RowFactory.create(1, null),
            RowFactory.create(2, null),
            RowFactory.create(3, Arrays.asList(5.0f, 6.0f, 7.0f, 8.0f)));
    List<Row> out = writeAndRead(schema, data, "fsl_null").orderBy("id").collectAsList();
    assertEquals(4, out.size());
    // Row 0: non-null
    assertFalse(out.get(0).isNullAt(1));
    assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f), out.get(0).getList(1));
    // Row 1: null
    assertTrue(out.get(1).isNullAt(1));
    // Row 2: null
    assertTrue(out.get(2).isNullAt(1));
    // Row 3: non-null AFTER nulls - this is the critical assertion
    assertFalse(out.get(3).isNullAt(1), "Row after null should not be null");
    assertEquals(Arrays.asList(5.0f, 6.0f, 7.0f, 8.0f), out.get(3).getList(1));
  }

  @Test
  public void testStructWithNullFslField() {
    // Non-null struct row where the FSL field inside is null.
    org.apache.spark.sql.types.Metadata fslMeta =
        new org.apache.spark.sql.types.MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 3L)
            .build();
    StructType inner =
        new StructType()
            .add("label", DataTypes.StringType, true)
            .add(
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "vec", DataTypes.createArrayType(DataTypes.FloatType, false), true, fslMeta));
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("payload", inner, true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, RowFactory.create("a", Arrays.asList(1.0f, 2.0f, 3.0f))),
            RowFactory.create(1, RowFactory.create("b", null)),
            RowFactory.create(2, RowFactory.create("c", Arrays.asList(4.0f, 5.0f, 6.0f))));
    List<Row> out = writeAndRead(schema, data, "struct_fsl_null").orderBy("id").collectAsList();
    assertEquals(3, out.size());
    // Row 0
    Row s0 = out.get(0).getStruct(1);
    assertEquals("a", s0.getString(0));
    assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f), s0.getList(1));
    // Row 1: struct non-null but vec field is null
    Row s1 = out.get(1).getStruct(1);
    assertEquals("b", s1.getString(0));
    assertTrue(s1.isNullAt(1));
    // Row 2: after the null FSL, values should be correct
    Row s2 = out.get(2).getStruct(1);
    assertEquals("c", s2.getString(0));
    assertEquals(Arrays.asList(4.0f, 5.0f, 6.0f), s2.getList(1));
  }

  @Test
  public void testStructAllNullRowWithFsl() {
    // Entire struct row is null — exercisesStructWriter.setNull() cascading to FSL child.
    org.apache.spark.sql.types.Metadata fslMeta =
        new org.apache.spark.sql.types.MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 3L)
            .build();
    StructType inner =
        new StructType()
            .add("label", DataTypes.StringType, true)
            .add(
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "vec", DataTypes.createArrayType(DataTypes.FloatType, false), true, fslMeta));
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType, false).add("payload", inner, true);
    List<Row> data =
        Arrays.asList(
            RowFactory.create(0, RowFactory.create("a", Arrays.asList(1.0f, 2.0f, 3.0f))),
            RowFactory.create(1, null),
            RowFactory.create(2, null),
            RowFactory.create(3, RowFactory.create("d", Arrays.asList(7.0f, 8.0f, 9.0f))));
    List<Row> out = writeAndRead(schema, data, "struct_all_null_fsl").orderBy("id").collectAsList();
    assertEquals(4, out.size());
    // Row 0
    Row s0 = out.get(0).getStruct(1);
    assertEquals("a", s0.getString(0));
    assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f), s0.getList(1));
    // Rows 1,2: entire struct is null
    assertTrue(out.get(1).isNullAt(1));
    assertTrue(out.get(2).isNullAt(1));
    // Row 3: after null struct rows, values must be correct
    Row s3 = out.get(3).getStruct(1);
    assertEquals("d", s3.getString(0));
    assertEquals(Arrays.asList(7.0f, 8.0f, 9.0f), s3.getList(1));
  }

  @Test
  public void testArrayOfFslWithNullElements() {
    // ARRAY<ARRAY<FLOAT>> where the inner array has FSL metadata and outer elements can be null.
    org.apache.spark.sql.types.Metadata fslMeta =
        new org.apache.spark.sql.types.MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 2L)
            .build();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add(
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "vecs",
                    DataTypes.createArrayType(
                        DataTypes.createArrayType(DataTypes.FloatType, false), true),
                    true,
                    new org.apache.spark.sql.types.MetadataBuilder()
                        .putString(LanceArrowUtils.LANCE_ELEMENT_METADATA_KEY(), fslMeta.json())
                        .build()));
    List<Row> data =
        Arrays.asList(
            RowFactory.create(
                0, Arrays.asList(Arrays.asList(1.0f, 2.0f), null, Arrays.asList(3.0f, 4.0f))),
            RowFactory.create(1, Arrays.asList(Arrays.asList(5.0f, 6.0f))));
    List<Row> out = writeAndRead(schema, data, "array_fsl_null_elem").orderBy("id").collectAsList();
    assertEquals(2, out.size());
    // Row 0: [vec, null, vec]
    List<Seq<Float>> row0list = out.get(0).getList(1);
    assertEquals(3, row0list.size());
    assertEquals(Arrays.asList(1.0f, 2.0f), JavaConverters.seqAsJavaList(row0list.get(0)));
    assertNull(row0list.get(1));
    assertEquals(Arrays.asList(3.0f, 4.0f), JavaConverters.seqAsJavaList(row0list.get(2)));
    // Row 1: single element
    List<Seq<Float>> row1list = out.get(1).getList(1);
    assertEquals(1, row1list.size());
    assertEquals(Arrays.asList(5.0f, 6.0f), JavaConverters.seqAsJavaList(row1list.get(0)));
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri(outputPath("array_fsl_null_elem")).build();
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(options).build()) {
      Field field = dataset.getSchema().findField("vecs");
      assertEquals(ArrowType.List.INSTANCE, field.getType());
      assertEquals(new ArrowType.FixedSizeList(2), field.getChildren().get(0).getType());
    }
  }

  @Test
  public void testMapWithFslValue() {
    // MAP<INT, ARRAY<FLOAT>> where the value array has FSL metadata.
    // Map requires Lance file format 2.2+.
    org.apache.spark.sql.types.Metadata fslMeta =
        new org.apache.spark.sql.types.MetadataBuilder()
            .putLong("arrow.fixed-size-list.size", 2L)
            .build();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add(
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "m",
                    DataTypes.createMapType(
                        DataTypes.IntegerType,
                        DataTypes.createArrayType(DataTypes.FloatType, false),
                        true),
                    true,
                    fslMeta));
    java.util.Map<Integer, List<Float>> m0 = new java.util.HashMap<>();
    m0.put(1, Arrays.asList(1.0f, 2.0f));
    m0.put(2, null);
    java.util.Map<Integer, List<Float>> m1 = new java.util.HashMap<>();
    m1.put(3, Arrays.asList(3.0f, 4.0f));
    List<Row> data = Arrays.asList(RowFactory.create(0, m0), RowFactory.create(1, m1));
    String path = outputPath("map_fsl_value");
    spark
        .createDataFrame(data, schema)
        .write()
        .format("lance")
        .option("file_format_version", "2.2")
        .mode(SaveMode.ErrorIfExists)
        .save(path);
    List<Row> out = spark.read().format("lance").load(path).orderBy("id").collectAsList();
    assertEquals(2, out.size());
    // Row 0: map with a null value
    assertFalse(out.get(0).isNullAt(1));
    // Row 1: map without null
    assertFalse(out.get(1).isNullAt(1));
  }
}
