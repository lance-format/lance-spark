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

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, TimestampNTZType}
import org.apache.spark.sql.util.LanceArrowUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.lance.{Dataset, WriteParams}
import org.lance.spark.{LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.utils.Utils

import java.nio.file.Path
import java.time.LocalDateTime

import scala.collection.JavaConverters._

abstract class BaseTimestampMilliWriteTest {
  @TempDir var tempDir: Path = _

  private def field(name: String, dataType: ArrowType, children: Field*): Field =
    new Field(name, FieldType.nullable(dataType), children.asJava)

  private def timestamp(name: String, unit: TimeUnit = TimeUnit.MILLISECOND): Field =
    field(name, new ArrowType.Timestamp(unit, null))

  private def primitiveSchema: Schema = new Schema(Seq(
    field("id", new ArrowType.Int(32, true)),
    timestamp("ts"),
    timestamp("micros", TimeUnit.MICROSECOND)).asJava)

  private def nestedSchema: Schema = new Schema(Seq(
    field("id", new ArrowType.Int(32, true)),
    field("payload", ArrowType.Struct.INSTANCE, timestamp("ts")),
    field("times", ArrowType.List.INSTANCE, timestamp("item"))).asJava)

  private def withDataset[T](uri: String)(body: Dataset => T): T = {
    val options = LanceSparkReadOptions.builder().datasetUri(uri).build()
    val dataset = Utils.openDatasetBuilder(options).build()
    try body(dataset)
    finally dataset.close()
  }

  private def withTable(schema: Schema)(body: (SparkSession, String, String) => Unit): Unit = {
    val uri = tempDir.resolve("timestamps.lance").toString
    val dataset =
      Dataset.create(LanceRuntime.allocator(), uri, schema, new WriteParams.Builder().build())
    dataset.close()
    val spark = SparkSession.builder()
      .appName("timestamp-milli-write-test")
      .master("local[1]")
      .config("spark.sql.catalog.timestamp_write", "org.lance.spark.LanceNamespaceSparkCatalog")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try body(spark, s"timestamp_write.`$uri`", uri)
    finally spark.stop()
  }

  @Test
  def originalUnitsSurviveSchemaRoundtrip(): Unit = {
    val key =
      new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), Seq.empty[Field].asJava)
    val entries = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null),
      Seq(key, timestamp("value")).asJava)
    val mapSchema = new Schema(Seq(field("times", new ArrowType.Map(false), entries)).asJava)
    Seq(primitiveSchema, nestedSchema, mapSchema).foreach { original =>
      val sparkSchema = LanceArrowUtils.fromArrowSchema(original)
      assertEquals(original, LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false))
    }
    val ordinary = new StructType().add("ts", TimestampNTZType)
    assertEquals(
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
      LanceArrowUtils.toArrowSchema(ordinary, "UTC", false).findField("ts").getType)
  }

  @Test
  def sqlInsertPreservesUnitsNullsAndPrecisionBoundaries(): Unit = withTable(primitiveSchema) {
    (spark, table, uri) =>
      val values = Seq(
        ("1970-01-01 00:00:00.000999", "1970-01-01T00:00:00"),
        ("1970-01-01 00:00:00.001000", "1970-01-01T00:00:00.001"),
        ("1970-01-01 00:00:00.001001", "1970-01-01T00:00:00.001"),
        ("1969-12-31 23:59:59.999999", "1969-12-31T23:59:59.999"),
        ("1969-12-31 23:59:59.999000", "1969-12-31T23:59:59.999"),
        ("1969-12-31 23:59:59.998999", "1969-12-31T23:59:59.998"),
        ("2024-06-15 12:34:56.123456", "2024-06-15T12:34:56.123"))
      val nonNullRows = values.zipWithIndex.map { case ((input, _), index) =>
        s"(${index + 1}, CAST('$input' AS TIMESTAMP_NTZ), CAST('$input' AS TIMESTAMP_NTZ))"
      }
      spark.sql(s"INSERT INTO $table VALUES " +
        (Seq(
          "(0, CAST(NULL AS TIMESTAMP_NTZ), CAST(NULL AS TIMESTAMP_NTZ))") ++ nonNullRows).mkString(
          ","))
      // Reload the existing target for a second append.
      spark.sql(
        s"INSERT INTO $table SELECT 8, CAST(NULL AS TIMESTAMP_NTZ), CAST(NULL AS TIMESTAMP_NTZ)")
      val rows = spark.sql(s"SELECT id, ts, micros FROM $table ORDER BY id").collect()
      assertEquals(9, rows.length)
      Seq(rows.head, rows.last).foreach { row =>
        assertTrue(row.isNullAt(1))
        assertTrue(row.isNullAt(2))
      }
      values.zipWithIndex.foreach { case ((input, expected), index) =>
        assertEquals(LocalDateTime.parse(expected), rows(index + 1).getAs[LocalDateTime](1))
        assertEquals(
          LocalDateTime.parse(input.replace(' ', 'T')),
          rows(index + 1).getAs[LocalDateTime](2))
      }
      withDataset(uri) { dataset =>
        assertEquals(9L, dataset.countRows())
        assertEquals(primitiveSchema, dataset.getSchema)
      }
  }

  @Test
  def readTransformAndAppendPreservesOriginalUnit(): Unit = withTable(primitiveSchema) {
    (spark, table, uri) =>
      spark.sql(s"INSERT INTO $table VALUES (0, CAST('2000-01-01 00:00:00.123456' AS TIMESTAMP_NTZ), CAST(NULL AS TIMESTAMP_NTZ))")
      val transformed = spark.table(table).selectExpr(
        "id + 1 AS id",
        "ts + INTERVAL 999 MICROSECONDS AS ts",
        "micros")
      transformed.writeTo(table).append()
      val rows = spark.table(table).orderBy("id").collect()
      assertEquals(2, rows.length)
      rows.foreach { row =>
        assertEquals(LocalDateTime.parse("2000-01-01T00:00:00.123"), row.getAs[LocalDateTime](1))
        assertTrue(row.isNullAt(2))
      }
      withDataset(uri)(dataset => assertEquals(primitiveSchema, dataset.getSchema))
  }

  @Test
  def sqlInsertPreservesNestedUnitsAndNullAlignment(): Unit = withTable(nestedSchema) {
    (spark, table, uri) =>
      spark.sql(s"""INSERT INTO $table VALUES
        (0, CAST(NULL AS STRUCT<ts: TIMESTAMP_NTZ>), CAST(NULL AS ARRAY<TIMESTAMP_NTZ>)),
        (1, named_struct('ts', CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP_NTZ)),
            array(CAST('1970-01-01 00:00:00.001001' AS TIMESTAMP_NTZ), CAST(NULL AS TIMESTAMP_NTZ)))""")
      val rows =
        spark.sql(s"SELECT id, payload.ts, times[0], times[1] FROM $table ORDER BY id").collect()
      assertEquals(2, rows.length)
      (1 until 4).foreach(i => assertTrue(rows(0).isNullAt(i)))
      assertEquals(LocalDateTime.parse("1969-12-31T23:59:59.999"), rows(1).getAs[LocalDateTime](1))
      assertEquals(LocalDateTime.parse("1970-01-01T00:00:00.001"), rows(1).getAs[LocalDateTime](2))
      assertTrue(rows(1).isNullAt(3))
      withDataset(uri)(dataset => assertEquals(nestedSchema, dataset.getSchema))
  }
}
