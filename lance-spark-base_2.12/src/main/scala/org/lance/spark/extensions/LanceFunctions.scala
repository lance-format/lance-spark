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
package org.lance.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.expressions.lance.LanceMatch

/** Shared Lance-specific function registrations for every version-specific extensions module. */
object LanceFunctions {

  def register(extensions: SparkSessionExtensions): Unit = {
    registerLanceMatch(extensions)
  }

  private def registerLanceMatch(extensions: SparkSessionExtensions): Unit = {
    val id = FunctionIdentifier("lance_match")
    val info = new ExpressionInfo(classOf[LanceMatch].getCanonicalName, "lance_match")
    extensions.injectFunction(
      (
        id,
        info,
        (children: Seq[org.apache.spark.sql.catalyst.expressions.Expression]) => {
          if (children.length != 2) {
            throw new IllegalArgumentException(
              s"lance_match requires exactly 2 arguments (column, query), got ${children.length}")
          }
          LanceMatch(children(0), children(1))
        }))
  }
}
