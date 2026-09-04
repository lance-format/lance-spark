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

import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.api.Test
import org.lance.index.Index

class ShowIndexesExecTest {

  @Test
  def totalSizeBytes_sumsAllSegments(): Unit = {
    val indexes = Seq(indexWithSize(10L), indexWithSize(20L))

    assertEquals(java.lang.Long.valueOf(30L), ShowIndexesExec.totalSizeBytes(indexes))
  }

  @Test
  def totalSizeBytes_returnsNullWhenAnySegmentSizeIsMissing(): Unit = {
    val indexes = Seq(indexWithSize(10L), Index.builder().build())

    assertNull(ShowIndexesExec.totalSizeBytes(indexes))
  }

  private def indexWithSize(sizeBytes: Long): Index =
    Index.builder().sizeBytes(java.lang.Long.valueOf(sizeBytes)).build()
}
