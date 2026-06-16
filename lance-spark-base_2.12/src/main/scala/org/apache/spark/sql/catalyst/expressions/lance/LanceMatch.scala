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
package org.apache.spark.sql.catalyst.expressions.lance

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Catalyst expression for the `lance_match(column, query)` SQL function.
 *
 * On a Lance table, this expression is extracted by [[LanceFtsPushdownRule]] and translated into a
 * native FTS scan via `ScanOptions.fullTextQuery`. When the expression cannot be pushed down
 * (e.g. non-Lance scan, query shape the rule doesn't recognize), this fallback evaluation runs —
 * a plain substring match with no tokenizer awareness. Correct but slow; proper relevance and
 * tokenizer semantics only apply through pushdown.
 */
case class LanceMatch(left: Expression, right: Expression)
  extends BinaryExpression
  with Predicate
  with ExpectsInputTypes
  with CodegenFallback {

  override def prettyName: String = "lance_match"

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def nullSafeEval(colValue: Any, queryValue: Any): Any = {
    val col = colValue.asInstanceOf[UTF8String]
    val query = queryValue.asInstanceOf[UTF8String]
    col.contains(query)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): LanceMatch =
    copy(left = newLeft, right = newRight)
}
