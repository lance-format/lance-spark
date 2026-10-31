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

import org.apache.arrow.vector.{DurationVector, IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.types.{ArrayType, DataTypes, DayTimeIntervalType, StructType}
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
import java.time.Duration

import scala.collection.JavaConverters._

/**
 * Lance tables that other writers create, such as pandas through PyArrow (`duration[ns]`), hold
 * Arrow durations in any unit. Spark reads them as `INTERVAL DAY TO SECOND`, in microseconds.
 */
abstract class BaseDurationUnitTest {
  @TempDir var tempDir: Path = _

  private val units = Seq(
    TimeUnit.SECOND -> "seconds",
    TimeUnit.MILLISECOND -> "millis",
    TimeUnit.MICROSECOND -> "micros",
    TimeUnit.NANOSECOND -> "nanos")

  // Raw values written natively, and the microseconds Spark reads for each unit: seconds and
  // milliseconds scale up, nanoseconds round down.
  private val raws = Seq(1L, -1L, 1999L)
  private val micros = Map(
    TimeUnit.SECOND -> Seq(1000000L, -1000000L, 1999000000L),
    TimeUnit.MILLISECOND -> Seq(1000L, -1000L, 1999000L),
    TimeUnit.MICROSECOND -> Seq(1L, -1L, 1999L),
    TimeUnit.NANOSECOND -> Seq(0L, -1L, 1L))

  private def field(name: String, dataType: ArrowType, children: Field*): Field =
    new Field(name, FieldType.nullable(dataType), children.asJava)

  private def nanos(name: String): Field = field(name, new ArrowType.Duration(TimeUnit.NANOSECOND))

  private def mapOf(name: String, value: Field): Field = {
    val key =
      new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), Seq.empty[Field].asJava)
    val entries =
      new Field(
        "entries",
        new FieldType(false, ArrowType.Struct.INSTANCE, null),
        Seq(key, value).asJava)
    field(name, new ArrowType.Map(false), entries)
  }

  /** id, a duration column per unit, and a nanosecond duration in a struct, a list and a map. */
  private val schema = new Schema((Seq(field("id", new ArrowType.Int(32, true))) ++
    units.map { case (unit, name) => field(name, new ArrowType.Duration(unit)) } ++ Seq(
      field("payload", ArrowType.Struct.INSTANCE, nanos("nanos")),
      field("list", ArrowType.List.INSTANCE, nanos("item")),
      mapOf("map", nanos("value")))).asJava)

  private def us(value: Long): Duration = Duration.ofNanos(Math.multiplyExact(value, 1000L))

  private def withDataset[T](uri: String)(body: Dataset => T): T = {
    val options = LanceSparkReadOptions.builder().datasetUri(uri).build()
    val dataset = Utils.openDatasetBuilder(options).build()
    try body(dataset)
    finally dataset.close()
  }

  private def withSpark(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .appName("duration-unit-test")
      .master("local[1]")
      .config("spark.sql.catalog.duration_test", "org.lance.spark.LanceNamespaceSparkCatalog")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try body(spark)
    finally spark.stop()
  }

  /** Creates an empty table with {@code schema} and passes its catalog name and URI to body. */
  private def withTable(body: (SparkSession, String, String) => Unit): Unit = {
    val uri = tempDir.resolve("durations.lance").toString
    Dataset.write()
      .allocator(LanceRuntime.allocator())
      .uri(uri)
      .schema(schema)
      .mode(WriteParams.WriteMode.CREATE)
      .execute().close()
    withSpark(spark => body(spark, s"duration_test.`$uri`", uri))
  }

  /** Appends a row per raw value, then a row of nulls, with Lance's own writer. */
  private def appendNative(uri: String): Unit = {
    val root = VectorSchemaRoot.create(schema, LanceRuntime.allocator())
    root.allocateNew()
    val payload = root.getVector("payload").asInstanceOf[StructVector]
    val list = root.getVector("list").asInstanceOf[ListVector]
    val map = root.getVector("map").asInstanceOf[MapVector]
    val entries = map.getDataVector.asInstanceOf[StructVector]
    raws.zipWithIndex.foreach { case (raw, i) =>
      units.foreach { case (_, name) =>
        root.getVector(name).asInstanceOf[DurationVector].setSafe(i, raw)
      }
      payload.setIndexDefined(i)
      payload.getChild("nanos").asInstanceOf[DurationVector].setSafe(i, raw)
      val item = list.startNewValue(i)
      list.getDataVector.asInstanceOf[DurationVector].setSafe(item, raw)
      list.endValue(i, 1)
      val entry = map.startNewValue(i)
      entries.setIndexDefined(entry)
      entries.getChild("key").asInstanceOf[VarCharVector].setSafe(entry, "k".getBytes("UTF-8"))
      entries.getChild("value").asInstanceOf[DurationVector].setSafe(entry, raw)
      map.endValue(i, 1)
    }
    (0 to raws.size).foreach(i => root.getVector("id").asInstanceOf[IntVector].setSafe(i, i))
    root.setRowCount(raws.size + 1)
    val reader = new SingleBatchArrowReader(LanceRuntime.allocator(), root)
    try Dataset.write().allocator(LanceRuntime.allocator()).reader(reader).uri(uri)
        .mode(WriteParams.WriteMode.APPEND).execute().close()
    finally {
      reader.close()
      root.close()
    }
  }

  private def ids(df: DataFrame): Seq[Int] =
    df.select("id").orderBy("id").collect().map(_.getInt(0)).toSeq

  private def pushed(df: DataFrame): String =
    df.queryExecution.executedPlan.collectFirst {
      case exec: BatchScanExec if exec.scan.isInstanceOf[SupportsMetadata] =>
        exec.scan.asInstanceOf[SupportsMetadata].getMetaData()("whereConditions")
    }.get

  @Test
  def schemaRoundtripKeepsEachUnit(): Unit = {
    val durations = units.map { case (unit, name) => field(name, new ArrowType.Duration(unit)) }
    val original = new Schema((durations ++ Seq(
      field("struct", ArrowType.Struct.INSTANCE, durations: _*),
      field(
        "list",
        ArrowType.List.INSTANCE,
        field("item", new ArrowType.Duration(TimeUnit.SECOND))),
      mapOf("map", nanos("value")))).asJava)

    val sparkSchema = LanceArrowUtils.fromArrowSchema(original)
    units.foreach { case (_, name) =>
      assertEquals(DayTimeIntervalType(), sparkSchema(name).dataType, name)
    }
    assertEquals(ArrayType(DayTimeIntervalType()), sparkSchema("list").dataType)
    assertEquals(original, LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false))
    // A new interval column is still written in microseconds.
    assertEquals(
      new ArrowType.Duration(TimeUnit.MICROSECOND),
      LanceArrowUtils.toArrowSchema(new StructType().add("d", DayTimeIntervalType()), "UTC", false)
        .findField("d").getType)
  }

  @Test
  def nativeDurationsReadInEveryUnit(): Unit = withTable { (spark, table, uri) =>
    appendNative(uri)
    val df = spark.table(table)
    units.foreach { case (_, name) =>
      assertEquals(DayTimeIntervalType(), df.schema(name).dataType, name)
    }
    val rows = df.orderBy("id").collect()
    units.zipWithIndex.foreach { case ((unit, name), column) =>
      raws.indices.foreach { row =>
        assertEquals(us(micros(unit)(row)), rows(row).get(column + 1), s"$name row $row")
      }
      assertTrue(rows(raws.size).isNullAt(column + 1), name)
    }
    raws.indices.foreach { row =>
      val expected = us(micros(TimeUnit.NANOSECOND)(row))
      assertEquals(expected, rows(row).getStruct(5).get(0), s"payload row $row")
      assertEquals(Seq(expected), rows(row).getSeq[Duration](6), s"list row $row")
      assertEquals(Map("k" -> expected), rows(row).getMap[String, Duration](7), s"map row $row")
    }
    (5 to 7).foreach(column => assertTrue(rows(raws.size).isNullAt(column), s"column $column"))

    // Spark compares the microseconds it reads: 1 and 1999 nanoseconds are 0 and 1 microsecond.
    val filtered = df.filter("nanos >= INTERVAL '0.000001' SECOND")
    assertEquals(Seq(2), ids(filtered))
    assertFalse(pushed(filtered).contains(">="), pushed(filtered))
  }

  @Test
  def writesKeepEachUnit(): Unit = withTable { (spark, table, uri) =>
    Seq(
      "INTERVAL '0 00:00:01.999999' DAY TO SECOND",
      "INTERVAL '-0 00:00:00.000001' DAY TO SECOND").zipWithIndex.foreach {
      case (value, id) =>
        spark.sql(s"INSERT INTO $table VALUES ($id, " +
          Seq.fill(units.size)(value).mkString(", ") +
          s", named_struct('nanos', $value), array($value), map('k', $value))")
    }
    // Copying the rows through Spark writes the same values back.
    spark.table(table)
      .selectExpr((Seq("id + 2 AS id") ++ units.map(_._2) ++ Seq("payload", "list", "map")): _*)
      .writeTo(table).append()
    // Spark writes microseconds: seconds and milliseconds round down, nanoseconds scale up.
    val stored = Map(
      TimeUnit.SECOND -> Seq(1L, -1L),
      TimeUnit.MILLISECOND -> Seq(1999L, -1L),
      TimeUnit.MICROSECOND -> Seq(1999999L, -1L),
      TimeUnit.NANOSECOND -> Seq(1999999000L, -1000L))
    withDataset(uri) { dataset =>
      assertEquals(schema, dataset.getSchema)
      val scanner = dataset.newScan()
      val batches = scanner.scanBatches()
      try {
        var seen = 0
        while (batches.loadNextBatch()) {
          val batch = batches.getVectorSchemaRoot
          (0 until batch.getRowCount).foreach { i =>
            val id = batch.getVector("id").asInstanceOf[IntVector].get(i)
            val payload = batch.getVector("payload").asInstanceOf[StructVector]
            val list = batch.getVector("list").asInstanceOf[ListVector]
            val map = batch.getVector("map").asInstanceOf[MapVector]
            val values = units.map { case (unit, name) =>
              (name, unit, batch.getVector(name), i)
            } ++ Seq(
              ("payload.nanos", TimeUnit.NANOSECOND, payload.getChild("nanos"), i),
              ("list", TimeUnit.NANOSECOND, list.getDataVector, list.getElementStartIndex(i)),
              (
                "map",
                TimeUnit.NANOSECOND,
                map.getDataVector.asInstanceOf[StructVector].getChild("value"),
                map.getElementStartIndex(i)))
            values.foreach { case (name, unit, vector, index) =>
              assertEquals(
                stored(unit)(id % 2),
                DurationVector.get(vector.asInstanceOf[DurationVector].getDataBuffer, index),
                s"$name id $id")
            }
            seen += 1
          }
        }
        assertEquals(4, seen)
      } finally {
        batches.close()
        scanner.close()
      }
    }
  }

  /** Spark writes `INTERVAL DAY TO SECOND` columns as Arrow `duration[us]`. */
  @Test
  def intervalFiltersRunInSpark(): Unit = withSpark { spark =>
    val path = tempDir.resolve("intervals.lance").toString
    val sparkSchema = new StructType()
      .add("id", DataTypes.IntegerType)
      .add("d", DayTimeIntervalType())
    val rows = Seq(Row(0, Duration.ofSeconds(1)), Row(1, Duration.ofSeconds(3)))
    spark.createDataFrame(rows.asJava, sparkSchema).write.format("lance").save(path)
    val filtered = spark.read.format("lance").load(path).filter("d > INTERVAL '2' SECOND")
    assertEquals(Seq(1), ids(filtered))
    assertFalse(pushed(filtered).contains(" > "), pushed(filtered))
  }

  @Test
  def secondAndMillisecondReadsRejectOverflow(): Unit = {
    Seq(TimeUnit.SECOND -> 1000000L, TimeUnit.MILLISECOND -> 1000L).foreach { case (unit, scale) =>
      val limit = Long.MaxValue / scale
      val vector = field("d", new ArrowType.Duration(unit))
        .createVector(LanceRuntime.allocator()).asInstanceOf[DurationVector]
      vector.allocateNew()
      Seq(limit, limit + 1, -limit - 1).zipWithIndex.foreach { case (value, i) =>
        vector.setSafe(i, value)
      }
      vector.setValueCount(3)
      val column = new LanceArrowColumnVector(vector)
      try {
        assertEquals(DayTimeIntervalType(), column.dataType)
        assertEquals(limit * scale, column.getLong(0))
        Seq(1, 2).foreach { i =>
          assertThrows(classOf[ArithmeticException], () => { column.getLong(i); () })
        }
      } finally column.close()
    }
  }

  @Test
  def nanosecondWriteOverflowFailsWithoutCommitting(): Unit = withTable { (spark, table, uri) =>
    val version = withDataset(uri)(_.version())
    // One microsecond more than a nanosecond duration holds.
    val value = "INTERVAL '106751 23:47:16.854776' DAY TO SECOND"
    val error = assertThrows(
      classOf[Exception],
      () => {
        spark.sql(s"INSERT INTO $table VALUES (0, NULL, NULL, NULL, $value, NULL, NULL, NULL)"); ()
      })
    assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
      .exists(_.isInstanceOf[ArithmeticException]))
    assertEquals(version, withDataset(uri)(_.version()))
  }
}
