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
 * ShowIndexes logical plan representing listing all indexes on a Lance dataset.
 */
case class ShowIndexes(table: LogicalPlan) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = ShowIndexesOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = {
    "ShowIndexesOnLanceDataset"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan])
      : ShowIndexes = {
    copy(newChildren(0))
  }
}

object ShowIndexesOutputType {
  val SCHEMA: Seq[Attribute] = StructType(
    Array(
      StructField("name", DataTypes.StringType, nullable = true),
      StructField(
        "fields",
        DataTypes.createArrayType(DataTypes.StringType, true),
        nullable = true),
      StructField("index_type", DataTypes.StringType, nullable = true),
      StructField("num_indexed_fragments", DataTypes.LongType, nullable = true),
      StructField("num_indexed_rows", DataTypes.LongType, nullable = true),
      StructField("num_unindexed_fragments", DataTypes.LongType, nullable = true),
      StructField("num_unindexed_rows", DataTypes.LongType, nullable = true)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
