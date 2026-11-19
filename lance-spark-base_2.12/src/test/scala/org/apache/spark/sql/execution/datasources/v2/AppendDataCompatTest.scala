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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LocalRelation, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util

class AppendDataCompatTest {
  private val relation = DataSourceV2Relation.create(
    new Table {
      override def name(): String = "compatibility_test"
      override def schema(): StructType = new StructType()
      override def capabilities(): util.Set[TableCapability] = util.Collections.emptySet()
    },
    None,
    None)
  private val query = LocalRelation()
  private val options = Map("update_columns" -> "test_column")

  class OssCompanion {
    def byPosition(
        table: NamedRelation,
        source: LogicalPlan,
        writeOptions: Map[String, String]): AppendData = {
      assertSame(relation, table)
      assertSame(query, source)
      assertEquals(options, writeOptions)
      AppendData.byPosition(table, source, writeOptions)
    }
  }

  class DatabricksCompanion {
    def byPosition(
        table: NamedRelation,
        source: LogicalPlan,
        writeOptions: Map[String, String],
        runtimeDefault: Boolean): AppendData = {
      assertTrue(runtimeDefault)
      assertSame(relation, table)
      assertSame(query, source)
      assertEquals(options, writeOptions)
      AppendData.byPosition(table, source, writeOptions)
    }

    def byPosition$default$4: Boolean = true
  }

  @Test
  def usesOssSignature(): Unit = {
    val append = AppendDataCompat.byPosition(new OssCompanion, relation, query, options)
    assertSame(relation, append.table)
    assertSame(query, append.query)
    assertEquals(options, append.writeOptions)
    assertFalse(append.isByName)
  }

  @Test
  def usesDatabricksSignatureAndRuntimeDefault(): Unit = {
    val append = AppendDataCompat.byPosition(new DatabricksCompanion, relation, query, options)
    assertSame(relation, append.table)
    assertSame(query, append.query)
    assertEquals(options, append.writeOptions)
    assertFalse(append.isByName)
  }

  @Test
  def propagatesOriginalRuntimeError(): Unit = {
    val failure = new NoSuchMethodError("runtime failure")
    val companion = new OssCompanion {
      override def byPosition(
          table: NamedRelation,
          source: LogicalPlan,
          writeOptions: Map[String, String]): AppendData = throw failure
    }
    val thrown = assertThrows(
      classOf[NoSuchMethodError],
      () => AppendDataCompat.byPosition(companion, relation, query, options))
    assertSame(failure, thrown)
  }

  @Test
  def rejectsUnsupportedSignature(): Unit = {
    assertThrows(
      classOf[NoSuchMethodException],
      () => AppendDataCompat.byPosition(new Object, relation, query, options))
  }
}
