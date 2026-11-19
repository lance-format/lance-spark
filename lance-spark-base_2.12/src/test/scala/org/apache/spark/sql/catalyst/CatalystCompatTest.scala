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
package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.CatalystCompat.TreeNodeArgs
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Stand-in for a Catalyst node whose runtime class has more fields than the Spark build Lance was
 * compiled against: a field inserted before `writeOptions` and one appended at the end, both of
 * the same type as a field Lance modifies.
 */
case class VendorExtendedWrite(
    query: LogicalPlan,
    vendorOptions: Map[String, String],
    writeOptions: Map[String, String],
    vendorFlag: Boolean,
    vendorTrailingOptions: Map[String, String])
  extends UnaryNode {
  override def child: LogicalPlan = query
  override def output: Seq[Attribute] = Nil
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(query = newChild)
}

case class VendorExtendedLeaf(name: String, vendorHint: Option[String] = None) extends LeafNode {
  override def output: Seq[Attribute] = Nil
}

class CatalystCompatTest {

  private val query = LocalRelation(AttributeReference("id", IntegerType)())

  private def vendorWrite(
      vendorOptions: Map[String, String] = Map.empty,
      writeOptions: Map[String, String] = Map.empty,
      vendorTrailingOptions: Map[String, String] = Map.empty): VendorExtendedWrite =
    VendorExtendedWrite(
      query,
      vendorOptions,
      writeOptions,
      vendorFlag = true,
      vendorTrailingOptions)

  @Test
  def withArgReplacesOnlyTheArgumentReadByTheAccessor(): Unit = {
    val vendorOptions = Map("vendor" -> "a")
    val trailingOptions = Map("trailing" -> "b")
    val original = vendorWrite(vendorOptions, Map("k" -> "old"), trailingOptions)

    val updated = original.withArg(_.writeOptions)(Map("k" -> "new"))

    assertEquals(Map("k" -> "new"), updated.writeOptions)
    assertSame(vendorOptions, updated.vendorOptions)
    assertSame(trailingOptions, updated.vendorTrailingOptions)
    assertTrue(updated.vendorFlag)
    assertSame(query, updated.query)
  }

  @Test
  def withArgDisambiguatesArgumentsSharingTheSameInstance(): Unit = {
    val original = vendorWrite()
    assertSame(original.vendorOptions, original.writeOptions)

    val updated = original.withArg(_.writeOptions)(Map("k" -> "v"))

    assertEquals(Map("k" -> "v"), updated.writeOptions)
    assertTrue(updated.vendorOptions.isEmpty)
    assertTrue(updated.vendorTrailingOptions.isEmpty)
  }

  @Test
  def withArgSelectsTheCompatibleConstructorArgumentWhenFieldsShareAnInstance(): Unit = {
    val sharedRelation = UnresolvedRelation(Seq("target"))
    val original =
      CatalystCompat.appendDataByPosition(sharedRelation, sharedRelation, Map.empty[String, String])
    val replacementQuery = Project(query.output, query)

    val updated = original.withArg(_.query)(replacementQuery)

    assertSame(sharedRelation, updated.table)
    assertSame(replacementQuery, updated.query)
  }

  @Test
  def withArgReturnsTheSameNodeWhenTheValueIsUnchanged(): Unit = {
    val original = vendorWrite(writeOptions = Map("k" -> "v"))

    assertSame(original, original.withArg(_.writeOptions)(original.writeOptions))
  }

  @Test
  def withArgPreservesArgumentsOutsideProductIterator(): Unit = {
    val metadata = new MetadataBuilder().putString("marker", "preserved").build()
    val original = Alias(query.output.head, "before")(
      qualifier = Seq("source"),
      explicitMetadata = Some(metadata))

    val updated = original.withArg(_.name)("after")

    assertEquals("after", updated.name)
    assertSame(original.child, updated.child)
    assertEquals(original.exprId, updated.exprId)
    assertEquals(original.qualifier, updated.qualifier)
    assertEquals(original.explicitMetadata, updated.explicitMetadata)
  }

  @Test
  def withArgRejectsAccessorsThatAreNotConstructorArguments(): Unit = {
    val original = vendorWrite()

    assertThrows(
      classOf[IllegalStateException],
      () => original.withArg(_.output)(Seq(AttributeReference("other", IntegerType)())))
  }

  @Test
  def sortBuildsAGlobalSort(): Unit = {
    val order = Seq(SortOrder(query.output.head, Ascending))

    val sort = CatalystCompat.sort(order, global = true, query)

    assertEquals(order, sort.order)
    assertTrue(sort.global)
    assertSame(query, sort.child)
  }

  @Test
  def appendDataByPositionBuildsAByPositionAppend(): Unit = {
    val table = UnresolvedRelation(Seq("target"))
    val options = Map("k" -> "v")

    val append = CatalystCompat.appendDataByPosition(table, query, options)

    assertSame(table, append.table)
    assertSame(query, append.query)
    assertEquals(options, append.writeOptions)
    assertFalse(append.isByName)
  }

  @Test
  def companionFactoryFillsTrailingParametersWithTheirDefaults(): Unit = {
    val leaf =
      CatalystCompat.invokeCompanionFactory(classOf[VendorExtendedLeaf], "apply", "name")

    assertEquals(VendorExtendedLeaf("name", None), leaf)
  }

  @Test
  def companionFactoryRejectsArgumentsNoOverloadAccepts(): Unit = {
    assertThrows(
      classOf[IllegalStateException],
      () =>
        CatalystCompat.invokeCompanionFactory(
          classOf[VendorExtendedLeaf],
          "apply",
          Integer.valueOf(1)))
  }
}
