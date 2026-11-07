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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

/** UPDATE/MERGE are only wired into the Spark 3.5+ modules. */
abstract class BaseBlobV1In22FilesRowLevelTest extends BlobV1In22FilesFixtureTest {
  @ParameterizedTest(name = "{0}: {1}")
  @CsvSource(Array(
    "top_level, update",
    "top_level, merge",
    "spark_append_22, update",
    "spark_append_22, merge"))
  def rowLevelWritesPreserveBlobV1Payloads(fixture: String, operation: String): Unit = {
    val uri = copyFixture(fixture)
    val table = tableAt(uri)
    val original = fixtureRows(fixture)
    assertPayloads(uri, original)
    val before = version(uri)
    val expected = operation match {
      case "update" =>
        spark.sql(s"UPDATE $table SET id = id + 10 WHERE id = 1")
        original.map { case (id, data) => (if (id == 1) 11 else id, data) }.sortBy(_._1)
      case "merge" =>
        spark.sql(s"""MERGE INTO $table t USING
                     |(SELECT 1 AS id, X'7265706C61636564' AS data
                     | UNION ALL SELECT 4 AS id, X'666F7572' AS data) s ON t.id = s.id
                     |WHEN MATCHED THEN UPDATE SET t.data = s.data
                     |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        original.map { case (id, data) =>
          (id, if (id == 1) bytes("replaced") else data)
        } :+
          (4 -> bytes("four"))
    }
    assertTrue(version(uri) > before)
    assertPayloads(uri, expected)
    assertSubsequentAppend(uri, expected)
  }
}
