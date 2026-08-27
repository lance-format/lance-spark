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
 * OptimizeIndex logical plan representing incremental index maintenance on a Lance dataset.
 *
 * Unlike CREATE INDEX (a full distributed rebuild across all fragments), this merges only the
 * unindexed (newly-appended) fragments into existing indexes via lance-core's optimizeIndices
 * API. When indexName is empty, all indexes are optimized.
 */
case class OptimizeIndex(
    table: LogicalPlan,
    indexName: Option[String],
    args: Seq[LanceNamedArgument]) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = OptimizeIndexOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = {
    s"OptimizeIndex(${indexName.getOrElse("<all>")})"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan])
      : OptimizeIndex = {
    copy(newChildren(0), this.indexName, this.args)
  }
}

object OptimizeIndexOutputType {
  val SCHEMA: Seq[Attribute] = StructType(
    Array(
      StructField("index_name", DataTypes.StringType, nullable = true),
      StructField("status", DataTypes.StringType, nullable = true)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
