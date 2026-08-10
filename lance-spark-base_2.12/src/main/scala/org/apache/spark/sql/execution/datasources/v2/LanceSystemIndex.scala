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

/**
 * Lance system (internal) index names that are not user-manageable. These are maintained by
 * lance-core itself and are filtered out of user-facing index operations such as SHOW INDEXES and
 * OPTIMIZE INDEX.
 */
object LanceSystemIndex {
  val SystemIndexNames: Set[String] = Set("__lance_frag_reuse", "__lance_mem_wal")

  def isSystemIndex(indexName: String): Boolean =
    indexName != null && SystemIndexNames.exists(_.equalsIgnoreCase(indexName))
}
