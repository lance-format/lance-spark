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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{BinaryType, DoubleType, IntegerType, MetadataBuilder, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.spark.{LanceConstant, LanceDataset, LanceSparkReadOptions}
import org.lance.spark.utils.BlobUtils

import scala.collection.JavaConverters._

/**
 * Unit tests for [[LanceBlobSourceContextRule]]'s application logic, exercised directly on logical
 * plans without a SparkSession. Covers the no-op edge cases (non-Lance target, blob-free source,
 * already-annotated) alongside the positive annotation path.
 *
 * Written with JUnit 5 (not ScalaTest) so surefire actually executes them.
 */
class LanceBlobSourceContextRuleTest {

  private val key = LanceConstant.BLOB_SOURCE_CONTEXTS_KEY
  private val rule = LanceBlobSourceContextRule()

  private val blobMetadata = new MetadataBuilder()
    .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
    .build()

  private def blobSchema: StructType = new StructType()
    .add("id", IntegerType, nullable = false)
    .add("data", BinaryType, nullable = true, blobMetadata)

  private def plainSchema: StructType = new StructType()
    .add("id", IntegerType, nullable = false)
    .add("score", DoubleType, nullable = true)

  private def lanceTable(uri: String, schema: StructType): LanceDataset =
    new LanceDataset(
      LanceSparkReadOptions.from(uri),
      schema,
      java.util.Collections.emptyMap[String, String](),
      null, // namespaceImpl
      java.util.Collections.emptyMap[String, String](), // namespaceProperties
      false, // managedVersioning
      null
    ) // fileFormatVersion

  private def relation(table: Table): DataSourceV2Relation =
    DataSourceV2Relation.create(table, None, None)

  /** A minimal non-Lance table so the rule's isLanceTarget guard sees a foreign target. */
  private class NonLanceTable(tableName: String, tableSchema: StructType) extends Table {
    override def name(): String = tableName
    override def schema(): StructType = tableSchema
    override def capabilities(): java.util.Set[TableCapability] =
      java.util.Collections.emptySet[TableCapability]()
  }

  private def append(
      target: Table,
      source: Table,
      writeOptions: Map[String, String] = Map.empty): AppendData =
    AppendData.byName(relation(target), relation(source), writeOptions)

  @Test
  def doesNotAnnotateNonLanceTargets(): Unit = {
    val plan = AppendData.byName(
      relation(new NonLanceTable("foreign", plainSchema)),
      relation(lanceTable("file:///src.lance", blobSchema)),
      Map.empty[String, String])
    val result = rule(plan).asInstanceOf[AppendData]
    assertFalse(result.writeOptions.contains(key))
  }

  @Test
  def doesNotAnnotateWhenSourceHasNoBlobColumns(): Unit = {
    val plan = append(
      lanceTable("file:///target.lance", blobSchema),
      lanceTable("file:///src.lance", plainSchema))
    val result = rule(plan).asInstanceOf[AppendData]
    assertFalse(result.writeOptions.contains(key))
  }

  @Test
  def annotatesLanceTargetWithBlobSource(): Unit = {
    val plan = append(
      lanceTable("file:///target.lance", blobSchema),
      lanceTable("file:///src.lance", blobSchema))
    val result = rule(plan).asInstanceOf[AppendData]
    assertTrue(result.writeOptions.contains(key))
    assertTrue(result.writeOptions(key).nonEmpty)
  }

  @Test
  def doesNotOverwriteExistingContextsKey(): Unit = {
    val plan = append(
      lanceTable("file:///target.lance", blobSchema),
      lanceTable("file:///src.lance", blobSchema),
      Map(key -> "preexisting"))
    val result = rule(plan).asInstanceOf[AppendData]
    assertEquals("preexisting", result.writeOptions(key))
  }

  @Test
  def isIdempotentAcrossRepeatedApplications(): Unit = {
    val plan = append(
      lanceTable("file:///target.lance", blobSchema),
      lanceTable("file:///src.lance", blobSchema))
    val once = rule(plan).asInstanceOf[AppendData]
    val twice = rule(once).asInstanceOf[AppendData]
    assertTrue(once.writeOptions.contains(key))
    assertEquals(once, twice)
  }

  @Test
  def annotationPreservesV2WritesMergeInvariant(): Unit = {
    // V2Writes.mergeOptions (Spark 4.x) asserts commandOptions == dsOptions or one side empty,
    // reflecting the three writer APIs: SQL puts options on the relation, DataFrameWriterV2 on
    // the command, and the V1 save() path on both, equal. Annotation must keep the invariant in
    // every (command, relation) configuration.
    val target = lanceTable("file:///target.lance", blobSchema)
    val source = lanceTable("file:///src.lance", blobSchema)
    val carried = Map("dataset_uri" -> "file:///target.lance")
    val configs = Seq(
      (Map.empty[String, String], Map.empty[String, String]), // SQL / writeTo without options
      (carried, carried), // V1 save(): both channels, equal
      (carried, Map.empty[String, String]), // DataFrameWriterV2 with command options
      (Map.empty[String, String], carried)
    ) // relation as the sole carrier
    configs.foreach { case (commandOptions, relationOptions) =>
      val targetRelation = DataSourceV2Relation.create(
        target,
        None,
        None,
        new CaseInsensitiveStringMap(relationOptions.asJava))
      val plan = AppendData.byName(targetRelation, relation(source), commandOptions)
      val result = rule(plan).asInstanceOf[AppendData]
      val command = result.writeOptions
      val ds = result.table.asInstanceOf[DataSourceV2Relation].options.asScala.toMap
      assertTrue(
        command.contains(key) || ds.contains(key),
        s"context not attached for command=$commandOptions relation=$relationOptions")
      assertTrue(
        command == ds || command.isEmpty || ds.isEmpty,
        s"V2Writes.mergeOptions invariant violated: command=$command ds=$ds")
    }
  }

  @Test
  def annotatesExactlyTheSupportedWriteCommands(): Unit = {
    // The blob-write command matrix (single source of truth: BlobPlanUtils.V2BlobWrite):
    //   AppendData                 -> context here, copy rewrite in LanceBlobV2CopyThroughRule
    //   OverwriteByExpression      -> context here, copy rewrite in LanceBlobV2CopyThroughRule
    //   CTAS / RTAS                -> both handled inside LanceBlobV2CopyThroughRule
    //   OverwritePartitionsDynamic -> rejected at the capability check before any rule runs,
    //                                 so this rule must not pretend to support it
    val target = relation(lanceTable("file:///target.lance", blobSchema))
    val source = relation(lanceTable("file:///src.lance", blobSchema))

    val append = rule(AppendData.byName(target, source, Map.empty)).asInstanceOf[AppendData]
    assertTrue(append.writeOptions.contains(key))

    val overwrite = rule(
      OverwriteByExpression.byName(target, source, Literal.TrueLiteral, Map.empty))
      .asInstanceOf[OverwriteByExpression]
    assertTrue(overwrite.writeOptions.contains(key))

    val dynamic = OverwritePartitionsDynamic.byName(target, source, Map.empty)
    assertEquals(dynamic, rule(dynamic))
  }
}
