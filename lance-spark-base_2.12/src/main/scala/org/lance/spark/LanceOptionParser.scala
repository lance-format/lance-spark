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
package org.lance.spark

import java.util.{HashMap, Map => JMap, TreeMap}
import java.util.function.Function

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Resolves each parameter's declared sources: operation, table, catalog, then default. */
private[spark] class LanceOptionParser(
    defaults: JMap[String, String],
    tableProperties: JMap[String, String],
    options: JMap[String, String]) {
  private val catalog = new TreeMap[String, String](String.CASE_INSENSITIVE_ORDER)
  catalog.putAll(defaults)
  private val operation = new TreeMap[String, String](String.CASE_INSENSITIVE_ORDER)
  operation.putAll(options)
  private val resolved = new TreeMap[String, String](String.CASE_INSENSITIVE_ORDER)
  resolved.putAll(catalog)
  resolved.putAll(operation)

  def resolvedOptions(): JMap[String, String] = new HashMap[String, String](resolved)

  def option(key: String): Parameter = new Parameter(key)

  class Parameter(key: String) {
    private var property: Option[String] = None

    def tableProperty(name: String): Parameter = {
      property = Some(name)
      this
    }

    def parse[T](defaultValue: T, convert: Function[String, T]): T = {
      val tableValue = if (operation.containsKey(key)) {
        None
      } else {
        property.flatMap { name =>
          val matches = tableProperties.asScala.filter { case (k, _) =>
            k.equalsIgnoreCase(name)
          }
          require(
            matches.values.toSeq.distinct.size <= 1,
            s"Conflicting table defaults for $name: ${matches.keys.mkString(", ")}; UNSET one spelling")
          matches.headOption.map(_._2)
        }
      }
      val value = if (operation.containsKey(key)) {
        Some(operation.get(key))
      } else {
        tableValue.orElse(Option(catalog.get(key)))
      }
      value match {
        case None => defaultValue
        case Some(raw) =>
          try {
            val parsed = convert.apply(raw)
            resolved.put(key, raw)
            parsed
          } catch {
            case NonFatal(e) =>
              throw new IllegalArgumentException(
                s"Invalid value for $key: '$raw': ${e.getMessage}",
                e)
          }
      }
    }

    def intValue(defaultValue: java.lang.Integer): java.lang.Integer =
      parse(defaultValue, (raw: String) => java.lang.Integer.valueOf(raw))

    def longValue(defaultValue: java.lang.Long): java.lang.Long =
      parse(defaultValue, (raw: String) => java.lang.Long.valueOf(raw))

    def positiveIntValue(defaultValue: java.lang.Integer): java.lang.Integer =
      parse(
        defaultValue,
        (raw: String) => {
          val value = java.lang.Integer.valueOf(raw)
          require(value > 0, s"$key must be positive")
          value
        })

    def positiveLongValue(defaultValue: java.lang.Long): java.lang.Long =
      parse(
        defaultValue,
        (raw: String) => {
          val value = java.lang.Long.valueOf(raw)
          require(value > 0, s"$key must be positive")
          value
        })

    def booleanValue(defaultValue: java.lang.Boolean): java.lang.Boolean =
      parse(defaultValue, (raw: String) => java.lang.Boolean.valueOf(raw))

    def stringValue(defaultValue: String): String =
      parse(defaultValue, (raw: String) => raw)
  }
}
