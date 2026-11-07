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
package org.lance.spark

import org.apache.arrow.vector.{IntVector, LargeVarBinaryVector, VectorSchemaRoot}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.util.LanceArrowUtils
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}
import org.lance.{Dataset => LanceTable, Fragment}
import org.lance.spark.utils.{BlobReference, BlobUtils, Utils}
import org.lance.spark.write.{LanceBatchWrite, StagedCommit, StagedCommitOptions}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.Collections

import scala.collection.JavaConverters._

/** Released fixtures must remain readable after every operation that publishes a new manifest. */
abstract class BaseBlobV1In22FilesTest extends BlobV1In22FilesFixtureTest {

  @ParameterizedTest(name = "{0}: {1}")
  @CsvSource(Array(
    "top_level, append",
    "top_level, append22",
    "top_level, appendPath",
    "top_level, delete",
    "top_level, createIndex",
    "top_level, createUntrainedIndex",
    "top_level, refreshIndex",
    "top_level, optimizeIndex",
    "top_level, dropIndex",
    "top_level, properties",
    "top_level, unsetProperties",
    "top_level, primaryKey",
    "top_level, addColumns",
    "top_level, updateColumns",
    "top_level, compact",
    "spark_append_22, append",
    "spark_append_22, delete",
    "spark_append_22, createIndex",
    "spark_append_22, properties",
    "nested, append",
    "nested, append22",
    "nested, delete",
    "nested, createIndex",
    "nested, properties",
    "nested, compact"))
  def preservesBlobV1Payloads(fixture: String, operation: String): Unit = {
    val uri = copyFixture(fixture)
    val table = tableAt(uri)
    val original = fixtureRows(fixture)
    val column = if (fixture == "nested") "payload.data" else "data"
    assertPayloads(uri, original, column)
    val before = version(uri)
    var expected = original
    operation match {
      case "append" =>
        val value = if (fixture == "nested") "named_struct('data', X'666F7572')" else "X'666F7572'"
        spark.sql(s"INSERT INTO $table VALUES (4, $value)")
        expected :+= 4 -> bytes("four")
      case "append22" =>
        val value = if (fixture == "nested") "named_struct('data', X'666F7572') AS payload"
        else "X'666F7572' AS data"
        spark.sql(s"SELECT 4 AS id, $value")
          .writeTo(table).option("file_format_version", "2.2").append()
        expected :+= 4 -> bytes("four")
      case "appendPath" =>
        spark.sql("SELECT 4 AS id, X'666F7572' AS data")
          .write.format("lance").mode("append").save(uri)
        expected :+= 4 -> bytes("four")
      case "delete" =>
        spark.sql(s"DELETE FROM $table WHERE id = 1")
        expected = original.filterNot(_._1 == 1)
      case "createIndex" =>
        spark.sql(s"ALTER TABLE $table CREATE INDEX fresh_idx USING btree (id)")
        assertTrue(indexNames(table).contains("fresh_idx"))
      case "createUntrainedIndex" =>
        spark.sql(
          s"ALTER TABLE $table CREATE INDEX fresh_idx USING zonemap (id) WITH (train = false)")
        assertTrue(indexNames(table).contains("fresh_idx"))
      case "refreshIndex" | "optimizeIndex" =>
        val index = spark.sql(s"SHOW INDEXES IN $table").first()
        assertTrue(index.getAs[Long]("num_unindexed_fragments") > 0)
        val command = if (operation == "refreshIndex") "REFRESH" else "OPTIMIZE"
        spark.sql(s"ALTER TABLE $table $command INDEX id_idx")
        assertEquals(
          0L,
          spark.sql(s"SHOW INDEXES IN $table").first().getAs[Long]("num_unindexed_fragments"))
      case "dropIndex" =>
        assertTrue(indexNames(table).contains("id_idx"))
        spark.sql(s"ALTER TABLE $table DROP INDEX id_idx")
        assertFalse(indexNames(table).contains("id_idx"))
      case "properties" =>
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('compatibility' = 'checked')")
        assertEquals(
          "checked",
          spark.sql(s"SHOW TBLPROPERTIES $table ('compatibility')").first().getString(1))
      case "unsetProperties" =>
        spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('compatibility' = 'checked')")
        assertPayloads(uri, original, column)
        spark.sql(s"ALTER TABLE $table UNSET TBLPROPERTIES ('compatibility')")
        withDataset(uri)(dataset => assertFalse(dataset.getConfig.containsKey("compatibility")))
      case "primaryKey" =>
        spark.sql(s"ALTER TABLE $table SET UNENFORCED PRIMARY KEY (id)")
        withDataset(uri) { dataset =>
          val id = dataset.getLanceSchema.fields().asScala.find(_.getName == "id").get
          assertEquals("1", id.getMetadata.get("lance-schema:unenforced-primary-key:position"))
        }
      case "addColumns" =>
        spark.sql(s"CREATE OR REPLACE TEMP VIEW column_values AS SELECT _rowaddr, _fragid, id * 2 AS id2 FROM $table")
        spark.sql(s"ALTER TABLE $table ADD COLUMNS id2 FROM column_values")
        assertEquals(
          original.map(_._1 * 2),
          spark.sql(s"SELECT id2 FROM $table ORDER BY id").collect().map(_.getInt(0)).toVector)
      case "updateColumns" =>
        spark.sql(s"CREATE OR REPLACE TEMP VIEW column_values AS SELECT _rowaddr, _fragid, id + 10 AS id FROM $table")
        spark.sql(s"ALTER TABLE $table UPDATE COLUMNS id FROM column_values")
        expected = original.map { case (id, data) => (id + 10, data) }
      case "compact" =>
        // An existing index can split compaction groups. Remove it before measuring actual work.
        if (indexNames(table).contains("id_idx")) {
          spark.sql(s"ALTER TABLE $table DROP INDEX id_idx")
          assertPayloads(uri, original, column)
        }
        val result = spark.sql(s"OPTIMIZE $table WITH (target_rows_per_fragment = 1000)").first()
        assertTrue(result.getAs[Long]("fragments_removed") > 0)
        assertTrue(result.getAs[Long]("fragments_added") > 0)
      case other => fail(s"Unknown operation: $other")
    }
    assertTrue(version(uri) > before, s"$operation must publish a manifest")
    assertPayloads(uri, expected, column)
    if (operation.toLowerCase.contains("index")) {
      assertEquals(
        expected.filter(_._1 == 2).map(_._1),
        spark.sql(s"SELECT id FROM $table WHERE id = 2").collect().map(_.getInt(0)).toVector)
      assertEquals(
        expected.filter(_._1 >= 2).map(_._1),
        spark.sql(s"SELECT id FROM $table WHERE id >= 2 ORDER BY id")
          .collect().map(_.getInt(0)).toVector)
    }
    // A readable commit is insufficient if its promoted schema prevents the next write.
    val value = if (fixture == "nested") "named_struct('data', X'6E657874')" else "X'6E657874'"
    val field = if (fixture == "nested") "payload" else "data"
    val extraColumn = if (operation == "addColumns") ", id2" else ""
    val extraValue = if (operation == "addColumns") ", 18" else ""
    spark.sql(s"INSERT INTO $table (id, $field$extraColumn) VALUES (9, $value$extraValue)")
    assertPayloads(uri, (expected :+ (9 -> bytes("next"))).sortBy(_._1), column)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("2.1", "2.2"))
  def rewriteAndSubsequentAppendPreserveBytes(fileVersion: String): Unit = {
    val uri = copyFixture("top_level")
    val table = tableAt(uri)
    val before = version(uri)
    spark.table(table)
      .writeTo(table).option("file_format_version", fileVersion).overwrite(lit(true))
    assertTrue(version(uri) > before)
    assertEquals(Set(fileVersion), physicalFileVersions(uri))
    val expected = fixtureRows("top_level")
    assertPayloads(uri, expected)
    spark.sql(s"INSERT INTO $table VALUES (11, X'6E657874')")
    assertPayloads(uri, expected :+ (11 -> bytes("next")))
  }

  @Test
  def tagsBranchesAndVacuumPreserveTheCurrentVersion(): Unit = {
    val uri = copyFixture("top_level")
    val table = tableAt(uri)
    val before = version(uri)
    spark.sql(s"ALTER TABLE $table CREATE TAG fixture_tag")
    spark.sql(s"ALTER TABLE $table CREATE BRANCH fixture_branch")
    spark.sql(s"VACUUM $table")
    assertEquals(before, version(uri))
    assertPayloads(uri, fixtureRows("top_level"))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("2.2", "stable"))
  def stagedCreateHonorsTheWriteTimeVersion(fileVersion: String): Unit = {
    val uri = tempDir.resolve("staged.lance").toString
    val table = tableAt(uri)
    spark.sql("SELECT 1 AS id, X'6F6E65' AS data")
      .writeTo(table).using("lance")
      .tableProperty("data.lance.encoding", "blob")
      .tableProperty("file_format_version", "2.1")
      .option("file_format_version", fileVersion).create()
    val versions = physicalFileVersions(uri)
    if (fileVersion == "stable") {
      assertEquals(1, versions.size)
      assertTrue(versions.forall(BlobUtils.fileFormatSupportsBlobV2))
    } else {
      assertEquals(Set(fileVersion), versions)
    }
    assertPayloads(uri, Vector(1 -> bytes("one")))
    spark.sql(s"INSERT INTO $table VALUES (2, X'74776F')")
    assertPayloads(uri, Vector(1 -> bytes("one"), 2 -> bytes("two")))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("catalog", "path"))
  def clean21TableAccepts22Append(writer: String): Unit = {
    val uri = tempDir.resolve("clean21.lance").toString
    val table = tableAt(uri)
    spark.sql(s"""CREATE TABLE $table (id INT NOT NULL, data BINARY) USING lance
                 |TBLPROPERTIES ('data.lance.encoding' = 'blob',
                 |'file_format_version' = '2.1')""".stripMargin)
    spark.sql(s"INSERT INTO $table VALUES (1, X'6F6E65')")
    assertEquals(Set("2.1"), physicalFileVersions(uri))
    assertPayloads(uri, Vector(1 -> bytes("one")))
    val appended = spark.sql("SELECT 2 AS id, X'74776F' AS data")
    if (writer == "catalog") {
      appended.writeTo(table).option("file_format_version", "2.2").append()
    } else {
      appended.write.format("lance").mode("append")
        .option("file_format_version", "2.2").save(uri)
    }
    assertEquals(Set("2.1", "2.2"), physicalFileVersions(uri))
    val expected = Vector(1 -> bytes("one"), 2 -> bytes("two"))
    assertPayloads(uri, expected)
    assertSubsequentAppend(uri, expected)
  }

  @Test
  def nestedBlobCreateAndAppendPreserveStoredBytes(): Unit = {
    val source = copyFixture("nested")
    val schema = withDataset(source)(dataset => LanceArrowUtils.fromArrowSchema(dataset.getSchema))
    val uri = tempDir.resolve("nested-created.lance").toString
    val table = tableAt(uri)
    spark.createDataFrame(Collections.singletonList(Row(1, Row(bytes("one")))), schema)
      .writeTo(table).using("lance").create()
    assertPayloads(uri, Vector(1 -> bytes("one")), "payload.data")
    spark.sql(s"INSERT INTO $table VALUES (2, named_struct('data', X'74776F'))")
    assertPayloads(uri, Vector(1 -> bytes("one"), 2 -> bytes("two")), "payload.data")
  }

  @Test
  def sparkFixtureTableVersionDoesNotDescribeItsFiles(): Unit = {
    val uri = copyFixture("spark_append_22")
    withDataset(uri) { dataset =>
      assertEquals("2.1", dataset.getLanceFileFormatVersion)
      val files = dataset.getFragments.asScala.flatMap(_.metadata().getFiles.asScala)
      assertFalse(files.isEmpty)
      files.foreach { file =>
        assertEquals(2, file.getFileMajorVersion)
        assertEquals(2, file.getFileMinorVersion)
      }
    }
    assertPayloads(uri, fixtureRows("spark_append_22"))
  }

  private def indexNames(table: String): Set[String] =
    spark.sql(s"SHOW INDEXES IN $table").collect().map(_.getAs[String]("name")).toSet

  @ParameterizedTest
  @ValueSource(strings = Array("append", "overwrite", "stagedReplace", "stagedCreate"))
  def externalFragmentsRetainBlobBytes(mode: String): Unit = {
    val existing = copyFixture("top_level")
    val schema = withDataset(existing)(_.getSchema)
    val uri = if (mode == "stagedCreate") tempDir.resolve("created.lance").toString else existing
    val stageOptions =
      StagedCommitOptions.pathBased(Collections.emptyMap[String, String](), false, "2.1")
    val stage = mode match {
      case "stagedCreate" => StagedCommit.forNewTable(schema, uri, stageOptions)
      case "stagedReplace" =>
        StagedCommit.forExistingTable(
          Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(uri).build()).build(),
          schema,
          stageOptions)
      case _ => null
    }
    val options = LanceSparkWriteOptions.from(uri).toBuilder.fileFormatVersion("2.2").build()
    val batch = new LanceBatchWrite(
      LanceArrowUtils.fromArrowSchema(schema),
      options,
      mode == "overwrite",
      null,
      null,
      null,
      null,
      false,
      stage)
    val before = if (mode == "stagedCreate") 0L else version(uri)
    try {
      val root = VectorSchemaRoot.create(schema, LanceRuntime.allocator())
      val fragments =
        try {
          root.getVector("id").asInstanceOf[IntVector].setSafe(0, 4)
          root.getVector("data").asInstanceOf[LargeVarBinaryVector].setSafe(0, bytes("four"))
          root.setRowCount(1)
          Fragment.write().datasetUri(uri).allocator(LanceRuntime.allocator()).data(root)
            .writeParams(batch.getFragmentWriteSpec.toWriteParams()).execute()
        } finally root.close()
      batch.commit(Array[WriterCommitMessage](LanceBatchWrite.taskCommit(fragments)))
      if (stage != null) {
        if (mode == "stagedReplace") {
          assertEquals(before, version(uri), "Batch commit must wait for the staged commit")
          assertPayloads(uri, fixtureRows("top_level"))
        }
        stage.commit()
      }
    } finally if (stage != null) stage.close()
    assertTrue(version(uri) > before)
    val expected = if (mode == "append") fixtureRows("top_level") :+ (4 -> bytes("four"))
    else Vector(4 -> bytes("four"))
    assertPayloads(uri, expected)
    assertSubsequentAppend(uri, expected)
  }
}

/** Shared by the all-version suite and the Spark 3.5+ row-level suite. */
abstract class BlobV1In22FilesFixtureTest {
  protected var spark: SparkSession = _
  @TempDir var tempDir: Path = _
  private val catalogName = "blob_v1"

  @BeforeEach
  def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("blob-v1-in-22-files")
      .master("local[2]")
      .config("spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
      .config(s"spark.sql.catalog.$catalogName", "org.lance.spark.LanceNamespaceSparkCatalog")
      .config(s"spark.sql.catalog.$catalogName.impl", "dir")
      .config(s"spark.sql.catalog.$catalogName.root", tempDir.toString)
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  @AfterEach
  def teardown(): Unit = if (spark != null) spark.stop()

  protected def bytes(value: String): Array[Byte] = value.getBytes(UTF_8)

  protected def fixtureRows(name: String): Vector[(Int, Array[Byte])] = name match {
    case "top_level" => Vector(1 -> bytes("one"), 2 -> bytes("two"), 3 -> bytes("three"))
    case "nested" => Vector(1 -> bytes("one"), 2 -> bytes("two"))
    case "spark_append_22" => Vector(1 -> Array[Byte](1), 2 -> Array[Byte](2))
  }

  protected def copyFixture(name: String): String = {
    val source = Paths.get(getClass.getResource(s"/blob_v1_in_22_files/$name.lance").toURI)
    val target = tempDir.resolve(s"$name.lance")
    val paths = Files.walk(source)
    try paths.iterator().asScala.foreach(path =>
        Files.copy(path, target.resolve(source.relativize(path))))
    finally paths.close()
    target.toString
  }

  protected def tableAt(uri: String): String = s"$catalogName.`$uri`"

  protected def withDataset[T](uri: String)(body: LanceTable => T): T = {
    val options = LanceSparkReadOptions.builder().datasetUri(uri).build()
    val dataset = Utils.openDatasetBuilder(options).build()
    try body(dataset)
    finally dataset.close()
  }

  protected def version(uri: String): Long = withDataset(uri)(_.version())

  protected def physicalFileVersions(uri: String): Set[String] = withDataset(uri) { dataset =>
    dataset.getFragments.asScala.flatMap(_.metadata().getFiles.asScala)
      .map(file => s"${file.getFileMajorVersion}.${file.getFileMinorVersion}").toSet
  }

  protected def assertSubsequentAppend(
      uri: String,
      expected: Vector[(Int, Array[Byte])]): Unit = {
    val before = version(uri)
    spark.sql(s"INSERT INTO ${tableAt(uri)} VALUES (9, X'6E657874')")
    assertTrue(version(uri) > before)
    assertPayloads(uri, (expected :+ (9 -> bytes("next"))).sortBy(_._1))
  }

  // Force Spark to decode top-level blob references/descriptors, then verify stored bytes through
  // Lance's blob API. Nested Spark materialization is tracked separately in #852.
  protected def assertPayloads(
      uri: String,
      expected: Vector[(Int, Array[Byte])],
      column: String = "data"): Unit = {
    val rows = spark.sql(s"SELECT id, _rowaddr FROM ${tableAt(uri)} ORDER BY id").collect()
    assertEquals(expected.map(_._1), rows.map(_.getInt(0)).toVector)
    if (column == "data") {
      val projected = spark.sql(s"SELECT id, _rowaddr, data FROM ${tableAt(uri)} ORDER BY id")
      val blobField = projected.schema("data")
      val blobRows = projected.collect()
      assertEquals(expected.map(_._1), blobRows.map(_.getInt(0)).toVector)
      blobRows.zip(expected).foreach { case (row, (id, data)) =>
        assertFalse(row.isNullAt(2), s"Spark blob for id=$id")
        if (blobField.dataType == BinaryType) {
          val reference = BlobReference.deserialize(row.getAs[Array[Byte]](2))
          assertEquals(uri, reference.getDatasetUri)
          assertEquals(column, reference.getColumnName)
          assertEquals(row.getLong(1), reference.getRowAddress)
          assertEquals(data.length.toLong, reference.getSize)
        } else {
          assertEquals(BlobUtils.BLOB_DESCRIPTOR_STRUCT, blobField.dataType)
          assertEquals(data.length.toLong, row.getStruct(2).getAs[Long]("size"))
        }
      }
    }
    val addresses = rows.map(row => Long.box(row.getLong(1))).toList.asJava
    withDataset(uri) { dataset =>
      val blobs = dataset.takeBlobs(addresses, column).asScala
      try {
        assertEquals(expected.size, blobs.size)
        blobs.zip(expected).foreach { case (blob, (id, data)) =>
          assertArrayEquals(data, blob.read(), s"$column bytes for id=$id")
        }
      } finally blobs.foreach(_.close())
    }
  }
}
