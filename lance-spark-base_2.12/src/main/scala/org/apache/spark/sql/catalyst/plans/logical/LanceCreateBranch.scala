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
import org.lance.Ref

case class LanceCreateBranch(
    table: LogicalPlan,
    branchName: String,
    ref: Ref,
    ifNotExists: Boolean = false) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = LanceCreateBranchOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = {
    s"CreateBranch(${branchName}, ${ref}, ifNotExists=${ifNotExists})"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan])
      : LanceCreateBranch = {
    copy(newChildren(0), this.branchName, this.ref, this.ifNotExists)
  }
}

object LanceCreateBranchOutputType {
  val SCHEMA = StructType(
    Array(
      StructField("name", DataTypes.StringType, nullable = false)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
