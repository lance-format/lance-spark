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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** Logical plan for maintaining an existing named Lance index. */
case class LanceOptimizeIndex(
    table: LogicalPlan,
    indexName: String,
    args: Seq[LanceNamedArgument]) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = LanceOptimizeIndexOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = s"LanceOptimizeIndex($indexName)"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan])
      : LanceOptimizeIndex = {
    copy(table = newChildren(0))
  }
}

object LanceOptimizeIndexOutputType {
  val SCHEMA: Seq[Attribute] = StructType(
    Array(
      StructField("index_name", DataTypes.StringType, nullable = false),
      StructField("fragments_indexed", DataTypes.LongType, nullable = false),
      StructField("segments_before", DataTypes.LongType, nullable = false),
      StructField("segments_after", DataTypes.LongType, nullable = false)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
