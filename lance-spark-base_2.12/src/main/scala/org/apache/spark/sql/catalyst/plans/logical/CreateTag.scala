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
 * CreateTag logical plan representing tag creation on a Lance dataset.
 *
 * This command creates a named tag for a specific version (or latest if not specified).
 * The version can be specified as either:
 * - A Long representing the numeric version number
 * - A String representing a tag name that will be resolved to a version
 */
case class CreateTag(
    table: LogicalPlan,
    tagName: String,
    version: Option[Any]) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = CreateTagOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = {
    version match {
      case Some(v) => s"CreateTag($tagName for version $v)"
      case None => s"CreateTag($tagName for latest)"
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan])
      : CreateTag = {
    copy(table = newChildren(0))
  }
}

object CreateTagOutputType {
  val SCHEMA = StructType(
    Array(
      StructField("tag_name", DataTypes.StringType, nullable = false),
      StructField("version", DataTypes.LongType, nullable = false)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
