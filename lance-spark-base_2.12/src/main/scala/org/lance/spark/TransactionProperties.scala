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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

import java.util.{Collections, LinkedHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object TransactionProperties {
  val option: String = LanceSparkWriteOptions.CONFIG_TRANSACTION_PROPERTIES

  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

  def snapshot(properties: JMap[String, String]): JMap[String, String] = {
    require(properties != null, "Transaction properties must be a non-null string map")
    val copied = new LinkedHashMap[String, String]()
    properties.asInstanceOf[JMap[_, _]].asScala.foreach {
      case (key: String, value: String) => copied.put(key, value)
      case _ => throw new IllegalArgumentException(
          "Transaction property keys and values must be non-null strings")
    }
    Collections.unmodifiableMap(copied)
  }

  def decode(encoded: String): JMap[String, String] = {
    try {
      val root = mapper.readTree(encoded)
      require(root != null && root.isObject, "Expected a JSON object")
      val properties = new LinkedHashMap[String, String]()
      root.fields().asScala.foreach { field =>
        require(field.getValue.isTextual, "Expected a string value")
        properties.put(field.getKey, field.getValue.textValue())
      }
      snapshot(properties)
    } catch {
      case NonFatal(_) => throw new IllegalArgumentException(
          "transaction_properties must be a JSON object with unique keys and string values")
    }
  }

  def encode(properties: JMap[String, String]): String =
    mapper.writeValueAsString(snapshot(properties))
}
