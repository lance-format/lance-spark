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

/**
 * AddIndex logical plan representing distributed index creation on a Lance dataset.
 *
 * This command builds a scalar index per fragment and then commits the index metadata.
 */
case class AddIndex(
    table: LogicalPlan,
    indexName: String,
    method: String,
    columns: Seq[String],
    args: Seq[LanceNamedArgument]) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = AddIndexOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = {
    s"AddIndex(${indexName} on ${columns.mkString(",")})"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): AddIndex = {
    copy(newChildren(0), this.indexName, this.method, this.columns, this.args)
  }
}

object AddIndexOutputType {
  val SCHEMA = StructType(
    Array(
      StructField("fragments_indexed", DataTypes.LongType, nullable = true),
      StructField("index_name", DataTypes.StringType, nullable = true)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
