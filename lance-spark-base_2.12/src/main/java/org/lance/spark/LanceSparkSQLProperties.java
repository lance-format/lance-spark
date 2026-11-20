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
package org.lance.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import scala.Option;

public final class LanceSparkSQLProperties {

  public static final String SEARCH_DISTRIBUTED_ENABLED =
      "spark.sql.lance.search.distributed.enabled";

  public static final boolean SEARCH_DISTRIBUTED_ENABLED_DEFAULT = false;

  public static boolean isSearchDistributedEnabled() {
    return Boolean.parseBoolean(
        getConf(SEARCH_DISTRIBUTED_ENABLED, String.valueOf(SEARCH_DISTRIBUTED_ENABLED_DEFAULT)));
  }

  public static String getConf(String key, String defaultValue) {
    Option<SparkSession> session = SparkSession.getActiveSession();
    if (session.isEmpty()) {
      session = SparkSession.getDefaultSession();
    }
    return session.isDefined()
        ? session.get().conf().get(key, defaultValue)
        : SQLConf.get().getConfString(key, defaultValue);
  }
}
