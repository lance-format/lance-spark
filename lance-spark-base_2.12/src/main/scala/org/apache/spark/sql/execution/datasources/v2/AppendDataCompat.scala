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
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}

import java.lang.reflect.{InvocationTargetException, Method}

private[v2] object AppendDataCompat {
  def byPosition(
      relation: NamedRelation,
      query: LogicalPlan,
      options: Map[String, String]): AppendData =
    byPosition(AppendData, relation, query, options)

  private[v2] def byPosition(
      companion: AnyRef,
      relation: NamedRelation,
      query: LogicalPlan,
      options: Map[String, String]): AppendData = {
    val parameters = Array[Class[_]](
      classOf[NamedRelation],
      classOf[LogicalPlan],
      classOf[Map[String, String]])
    val method =
      try {
        companion.getClass.getMethod("byPosition", parameters: _*)
      } catch {
        // DBR 16.4 adds a fourth argument; use its default to retain the runtime's write semantics.
        case _: NoSuchMethodException =>
          companion.getClass.getMethod("byPosition", (parameters :+ java.lang.Boolean.TYPE): _*)
      }
    val arguments = Array[AnyRef](relation, query, options)
    val actualArguments = if (method.getParameterCount == 4) {
      arguments :+ invoke(companion.getClass.getMethod("byPosition$default$4"), companion)
    } else {
      arguments
    }
    classOf[AppendData].cast(invoke(method, companion, actualArguments: _*))
  }

  private def invoke(method: Method, receiver: AnyRef, arguments: AnyRef*): AnyRef =
    try {
      method.invoke(receiver, arguments: _*)
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
}
