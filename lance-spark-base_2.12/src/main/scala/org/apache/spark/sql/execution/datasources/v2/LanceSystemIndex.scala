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

import java.util.Locale

/**
 * Helpers for Lance index names shared across the index DDL commands.
 *
 * Holds the set of system (internal) index names that are not user-manageable — maintained by
 * lance-core itself and filtered out of user-facing index operations such as SHOW INDEXES and
 * OPTIMIZE INDEX — and the locale-independent name normalization used to give the SQL commands a
 * consistent, case-insensitive contract.
 */
object LanceSystemIndex {
  val SystemIndexNames: Set[String] = Set("__lance_frag_reuse", "__lance_mem_wal")

  def isSystemIndex(indexName: String): Boolean =
    indexName != null && SystemIndexNames.exists(_.equalsIgnoreCase(indexName))

  /**
   * Lower-cases an index name for the case-insensitive SQL contract using [[Locale.ROOT]], so
   * normalization does not depend on the JVM default locale (e.g. under tr-TR a naive
   * {@code toLowerCase} maps 'I' to a dotless 'ı', which would make CREATE and OPTIMIZE/DROP INDEX
   * disagree on the same name).
   */
  def normalizeName(indexName: String): String =
    if (indexName == null) null else indexName.toLowerCase(Locale.ROOT)
}
