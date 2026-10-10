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
package org.lance.spark.arrow

import org.apache.arrow.vector.{IntVector, TimeStampVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.LanceArrowUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.lance.{Dataset, WriteParams}
import org.lance.spark.{LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.utils.Utils
import org.lance.spark.vectorized.LanceArrowColumnVector
import org.lance.spark.write.SingleBatchArrowReader

import java.nio.file.Path
import java.time.{LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import scala.collection.JavaConverters._

abstract class BaseTimestampUnitWriteTest {
  @TempDir var tempDir: Path = _

  private val units = Seq(
    (TimeUnit.SECOND, "seconds", ChronoUnit.SECONDS),
    (TimeUnit.MILLISECOND, "millis", ChronoUnit.MILLIS),
    (TimeUnit.MICROSECOND, "micros", ChronoUnit.MICROS),
    (TimeUnit.NANOSECOND, "nanos", ChronoUnit.NANOS))
  private val epoch = LocalDateTime.of(1970, 1, 1, 0, 0)

  private def field(name: String, dataType: ArrowType, children: Field*): Field =
    new Field(name, FieldType.nullable(dataType), children.asJava)

  private def timestamps(selectedUnits: Seq[TimeUnit] = units.map(_._1)): Seq[Field] =
    units.filter(u => selectedUnits.contains(u._1)).flatMap { case (unit, name, _) =>
      Seq(
        field(name + "_ntz", new ArrowType.Timestamp(unit, null)),
        field(name + "_tz", new ArrowType.Timestamp(unit, "Asia/Shanghai")))
    }

  private def schema(fields: Seq[Field]): Schema =
    new Schema((Seq(field("id", new ArrowType.Int(32, true))) ++ fields).asJava)

  private def withDataset[T](uri: String)(body: Dataset => T): T = {
    val options = LanceSparkReadOptions.builder().datasetUri(uri).build()
    val dataset = Utils.openDatasetBuilder(options).build()
    try body(dataset)
    finally dataset.close()
  }

  private def withTable(original: Schema)(body: (SparkSession, String, String) => Unit): Unit = {
    val uri = tempDir.resolve("timestamps.lance").toString
    Dataset.write()
      .allocator(LanceRuntime.allocator())
      .uri(uri)
      .schema(original)
      .mode(WriteParams.WriteMode.CREATE)
      .execute().close()
    val spark = SparkSession.builder()
      .appName("timestamp-unit-write-test")
      .master("local[1]")
      .config("spark.sql.catalog.timestamp_write", "org.lance.spark.LanceNamespaceSparkCatalog")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    // Deliberately differ from the original Arrow timezone, even if a session is reused.
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try body(spark, s"timestamp_write.`$uri`", uri)
    finally spark.stop()
  }

  private def sqlValue(f: Field, value: String): String = {
    val timezone = f.getType.asInstanceOf[ArrowType.Timestamp].getTimezone
    val sparkType = if (timezone == null) "TIMESTAMP_NTZ" else "TIMESTAMP"
    val literal = if (value == null) "NULL"
    else if (timezone == null) s"'$value'"
    else s"'$value+00:00'"
    s"CAST($literal AS $sparkType)"
  }

  private def atPrecision(value: String, f: Field): LocalDateTime = {
    val unit = f.getType.asInstanceOf[ArrowType.Timestamp].getUnit
    LocalDateTime.parse(value.replace(' ', 'T')).truncatedTo(units.find(_._1 == unit).get._3)
  }

  private def assertTimestamp(row: Row, ordinal: Int, f: Field, expected: LocalDateTime): Unit = {
    if (f.getType.asInstanceOf[ArrowType.Timestamp].getTimezone == null)
      assertEquals(expected, row.getAs[LocalDateTime](ordinal), f.getName)
    else assertEquals(
      expected.toInstant(ZoneOffset.UTC),
      row.getTimestamp(ordinal).toInstant,
      f.getName)
  }

  // Inspect native values as well as Spark values so inverse reader/writer mistakes cannot cancel.
  private def assertStored(uri: String, original: Schema, expected: Map[Int, String]): Unit =
    withDataset(uri) { dataset =>
      assertEquals(original, dataset.getSchema)
      assertEquals(expected.size.toLong, dataset.countRows())
      val scanner = dataset.newScan()
      val reader = scanner.scanBatches()
      try {
        var seen = Set.empty[Int]
        while (reader.loadNextBatch()) {
          val root = reader.getVectorSchemaRoot
          (0 until root.getRowCount).foreach { i =>
            val id = root.getVector("id").asInstanceOf[IntVector].get(i)
            seen += id
            original.getFields.asScala.tail.foreach { f =>
              val vector = root.getVector(f.getName).asInstanceOf[TimeStampVector]
              if (expected(id) == null) assertTrue(vector.isNull(i))
              else {
                val unit = f.getType.asInstanceOf[ArrowType.Timestamp].getUnit
                val chrono = units.find(_._1 == unit).get._3
                assertEquals(
                  chrono.between(epoch, atPrecision(expected(id), f)),
                  vector.get(i),
                  f.getName)
              }
            }
          }
        }
        assertEquals(expected.keySet, seen)
      } finally {
        reader.close()
        scanner.close()
      }
    }

  @Test
  def originalUnitsAndTimezonesSurviveSchemaRoundtrip(): Unit = {
    val fields = timestamps()
    val lists = fields.map(f => field("array_" + f.getName, ArrowType.List.INSTANCE, f))
    val maps = fields.map { f =>
      val key = new Field(
        "key",
        new FieldType(false, ArrowType.Utf8.INSTANCE, null),
        Seq.empty[Field].asJava)
      val entries = new Field(
        "entries",
        new FieldType(false, ArrowType.Struct.INSTANCE, null),
        Seq(key, new Field("value", f.getFieldType, f.getChildren)).asJava)
      field("map_" + f.getName, new ArrowType.Map(false), entries)
    }
    Seq(
      schema(fields :+ field("empty_timezone", new ArrowType.Timestamp(TimeUnit.SECOND, ""))),
      schema(
        Seq(field("payload", ArrowType.Struct.INSTANCE, fields: _*)) ++ lists ++ maps)).foreach {
      original =>
        assertEquals(
          original,
          LanceArrowUtils.toArrowSchema(LanceArrowUtils.fromArrowSchema(original), "UTC", false))
    }
    val ordinary = new StructType().add("ntz", TimestampNTZType).add("tz", TimestampType)
    val arrow = LanceArrowUtils.toArrowSchema(ordinary, "UTC", false)
    assertEquals(
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
      arrow.findField("ntz").getType)
    assertEquals(
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
      arrow.findField("tz").getType)
  }

  @Test
  def sqlInsertPreservesUnitsNullsAndPrecisionBoundaries(): Unit = {
    val fields = timestamps()
    val original = schema(fields)
    withTable(original) { (spark, table, uri) =>
      val values = Seq(
        "1970-01-01 00:00:00.000999",
        "1970-01-01 00:00:00.001000",
        "1970-01-01 00:00:00.001001",
        "1970-01-01 00:00:00.999999",
        "1970-01-01 00:00:01.000001",
        "1969-12-31 23:59:59.999999",
        "1969-12-31 23:59:59.999000",
        "1969-12-31 23:59:59.998999",
        "2024-06-15 12:34:56.123456")
      val inputs = Seq(null) ++ values
      val rowsToInsert = inputs.zipWithIndex.map { case (value, id) =>
        (Seq(id.toString) ++ fields.map(sqlValue(_, value))).mkString("(", ",", ")")
      }
      spark.sql(s"INSERT INTO $table VALUES " + rowsToInsert.mkString(","))
      // Reload the existing target for a second append.
      spark.sql(s"INSERT INTO $table SELECT " + (Seq(inputs.size.toString) ++ fields.map(sqlValue(
        _,
        null))).mkString(","))
      val expected = (inputs :+ null).zipWithIndex.map { case (value, id) => id -> value }.toMap
      val rows = spark.table(table).orderBy("id").collect()
      assertEquals(expected.size, rows.length)
      rows.foreach { row =>
        fields.zipWithIndex.foreach { case (f, index) =>
          if (expected(row.getInt(0)) == null) assertTrue(row.isNullAt(index + 1))
          else assertTimestamp(row, index + 1, f, atPrecision(expected(row.getInt(0)), f))
        }
      }
      assertStored(uri, original, expected)
    }
  }

  @Test
  def readTransformAndAppendPreservesOriginalUnits(): Unit = {
    val fields = timestamps()
    val original = schema(fields)
    withTable(original) { (spark, table, uri) =>
      val input = "2000-01-01 00:00:00.123456"
      spark.sql(
        s"INSERT INTO $table SELECT " + (Seq("0") ++ fields.map(sqlValue(_, input))).mkString(","))
      val transformed = spark.table(table).selectExpr((Seq("id + 1 AS id") ++ fields.map { f =>
        s"${f.getName} + INTERVAL 999 MICROSECONDS AS ${f.getName}"
      }): _*)
      transformed.writeTo(table).append()
      val rows = spark.table(table).orderBy("id").collect()
      assertEquals(2, rows.length)
      fields.zipWithIndex.foreach { case (f, index) =>
        assertTimestamp(rows(0), index + 1, f, atPrecision(input, f))
        val unit = f.getType.asInstanceOf[ArrowType.Timestamp].getUnit
        val expected = unit match {
          case TimeUnit.SECOND => "2000-01-01T00:00:00"
          case TimeUnit.MILLISECOND => "2000-01-01T00:00:00.123"
          case _ => "2000-01-01T00:00:00.124455"
        }
        assertTimestamp(rows(1), index + 1, f, LocalDateTime.parse(expected))
      }
      withDataset(uri)(dataset => assertEquals(original, dataset.getSchema))
    }
  }

  @Test
  def sqlInsertPreservesNestedUnitsAndNullAlignment(): Unit = {
    val fields = timestamps()
    val lists = fields.map(f => field("array_" + f.getName, ArrowType.List.INSTANCE, f))
    val original = schema(Seq(field("payload", ArrowType.Struct.INSTANCE, fields: _*)) ++ lists)
    withTable(original) { (spark, table, uri) =>
      val structType = fields.map(f =>
        s"${f.getName}: " +
          (if (f.getType.asInstanceOf[ArrowType.Timestamp].getTimezone == null) "TIMESTAMP_NTZ"
           else "TIMESTAMP")).mkString("STRUCT<", ",", ">")
      val nullLists = fields.map(f =>
        s"CAST(NULL AS ARRAY<${if (f.getType.asInstanceOf[ArrowType.Timestamp].getTimezone == null) "TIMESTAMP_NTZ"
          else "TIMESTAMP"}>)")
      val payload = fields.flatMap(f =>
        Seq(s"'${f.getName}'", sqlValue(f, "1969-12-31 23:59:59.999999"))).mkString(
        "named_struct(",
        ",",
        ")")
      val arrays = fields.map(f =>
        s"array(${sqlValue(f, "1970-01-01 00:00:00.001001")}, ${sqlValue(f, null)})")
      spark.sql(s"INSERT INTO $table VALUES " +
        (Seq("0", s"CAST(NULL AS $structType)") ++ nullLists).mkString("(", ",", "),") +
        (Seq("1", payload) ++ arrays).mkString("(", ",", ")"))
      val selections = Seq("id") ++ fields.map(f => s"payload.${f.getName}") ++
        fields.map(f => s"array_${f.getName}[0]") ++ fields.map(f => s"array_${f.getName}[1]")
      val rows = spark.table(table).selectExpr(selections: _*).orderBy("id").collect()
      assertEquals(2, rows.length)
      (1 until selections.size).foreach(i => assertTrue(rows(0).isNullAt(i)))
      fields.zipWithIndex.foreach { case (f, index) =>
        assertTimestamp(rows(1), index + 1, f, atPrecision("1969-12-31 23:59:59.999999", f))
        assertTimestamp(
          rows(1),
          index + 1 + fields.size,
          f,
          atPrecision("1970-01-01 00:00:00.001001", f))
        assertTrue(rows(1).isNullAt(index + 1 + fields.size * 2))
      }
      withDataset(uri)(dataset => assertEquals(original, dataset.getSchema))
    }
  }

  @Test
  def nativeNanosecondsReadAndAppendAtSparkMicrosecondPrecision(): Unit = {
    val fields = timestamps(Seq(TimeUnit.NANOSECOND))
    val original = schema(fields)
    withTable(original) { (spark, table, uri) =>
      val samples = Seq(
        (-1001L, -2L),
        (-1000L, -1L),
        (-999L, -1L),
        (-1L, -1L),
        (0L, 0L),
        (1L, 0L),
        (999L, 0L),
        (1000L, 1L),
        (1001L, 1L))
      val root = VectorSchemaRoot.create(original, LanceRuntime.allocator())
      root.allocateNew()
      val ids = root.getVector("id").asInstanceOf[IntVector]
      samples.zipWithIndex.foreach { case ((raw, _), index) =>
        ids.setSafe(index, index)
        fields.foreach(f =>
          root.getVector(f.getName).asInstanceOf[TimeStampVector].setSafe(index, raw))
      }
      ids.setSafe(samples.size, samples.size)
      root.setRowCount(samples.size + 1)
      val reader = new SingleBatchArrowReader(LanceRuntime.allocator(), root)
      try Dataset.write().allocator(LanceRuntime.allocator()).reader(reader).uri(uri)
          .mode(WriteParams.WriteMode.APPEND).execute().close()
      finally {
        reader.close()
        root.close()
      }
      val rows = spark.table(table).orderBy("id").collect()
      assertEquals(samples.size + 1, rows.length)
      samples.zipWithIndex.foreach { case ((_, micros), index) =>
        fields.zipWithIndex.foreach { case (f, column) =>
          assertTimestamp(rows(index), column + 1, f, epoch.plusNanos(micros * 1000L))
        }
      }
      fields.indices.foreach(i => assertTrue(rows.last.isNullAt(i + 1)))
      spark.table(table).selectExpr(
        (Seq(s"id + ${rows.length} AS id") ++ fields.map(_.getName)): _*)
        .writeTo(table).append()
      val copied = spark.table(table).orderBy("id").collect().drop(rows.length)
      assertEquals(rows.toSeq.map(_.toSeq.tail), copied.toSeq.map(_.toSeq.tail))
      // Native originals keep their nanoseconds; Spark copies have only microsecond precision.
      withDataset(uri) { dataset =>
        assertEquals(original, dataset.getSchema)
        val scanner = dataset.newScan()
        val batches = scanner.scanBatches()
        try while (batches.loadNextBatch()) {
            val batch = batches.getVectorSchemaRoot
            (0 until batch.getRowCount).foreach { i =>
              val id = batch.getVector("id").asInstanceOf[IntVector].get(i)
              fields.foreach { f =>
                val v = batch.getVector(f.getName).asInstanceOf[TimeStampVector]
                val sample = id % rows.length
                if (sample == samples.size) assertTrue(v.isNull(i))
                else assertEquals(
                  if (id < rows.length) samples(sample)._1 else samples(sample)._2 * 1000L,
                  v.get(i))
              }
            }
          }
        finally {
          batches.close()
          scanner.close()
        }
      }
    }
  }

  @Test
  def secondAndMillisecondReadsRejectOverflow(): Unit = {
    timestamps(Seq(TimeUnit.SECOND, TimeUnit.MILLISECOND)).foreach { f =>
      val vector = f.createVector(LanceRuntime.allocator()).asInstanceOf[TimeStampVector]
      vector.allocateNew()
      val seconds = f.getType.asInstanceOf[ArrowType.Timestamp].getUnit == TimeUnit.SECOND
      val limit = if (seconds) 9223372036854L else 9223372036854775L
      val expected = if (seconds) 9223372036854000000L else 9223372036854775000L
      Seq(limit, -limit, limit + 1, -limit - 1).zipWithIndex.foreach {
        case (value, index) => vector.setSafe(index, value)
      }
      vector.setValueCount(4)
      val column = new LanceArrowColumnVector(vector)
      try {
        assertEquals(expected, column.getLong(0))
        assertEquals(-expected, column.getLong(1))
        Seq(2, 3).foreach { index =>
          assertThrows(classOf[ArithmeticException], () => { column.getLong(index); () })
        }
      } finally column.close()
    }
  }

  @Test
  def nanosecondOverflowFailsWithoutCommitting(): Unit = {
    val fields = timestamps(Seq(TimeUnit.NANOSECOND))
    val original = schema(fields)
    withTable(original) { (spark, table, uri) =>
      val valid = Seq("1677-09-21 00:12:43.145225", "2262-04-11 23:47:16.854775")
      valid.zipWithIndex.foreach { case (value, index) =>
        spark.sql(s"INSERT INTO $table SELECT " + (Seq(index.toString) ++ fields.map(sqlValue(
          _,
          value))).mkString(","))
      }
      assertStored(uri, original, valid.zipWithIndex.map { case (value, id) => id -> value }.toMap)
      val initialVersion = withDataset(uri)(_.version())
      Seq("1677-09-21 00:12:43.145224", "2262-04-11 23:47:16.854776").foreach { value =>
        val error = assertThrows(
          classOf[Exception],
          () => {
            spark.sql(s"INSERT INTO $table SELECT " + (Seq("2") ++ fields.map(
              sqlValue(_, value))).mkString(","))
            ()
          })
        assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
          .exists(_.isInstanceOf[ArithmeticException]))
        withDataset(uri) { dataset =>
          assertEquals(initialVersion, dataset.version())
          assertEquals(2L, dataset.countRows())
          assertEquals(original, dataset.getSchema)
        }
      }
    }
  }
}
