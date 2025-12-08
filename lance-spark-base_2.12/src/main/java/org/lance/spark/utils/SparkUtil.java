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
package org.lance.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

public class SparkUtil {

  private static final String SPARK_CATALOG_CONF_PREFIX = "spark.sql.catalog";
  // Format string used as the prefix for Spark configuration keys to override Hadoop configuration
  // values for Iceberg tables from a given catalog. These keys can be specified as
  // `spark.sql.catalog.$catalogName.hadoop.*`, similar to using `spark.hadoop.*` to override
  // Hadoop configurations globally for a given Spark session.
  private static final String SPARK_CATALOG_HADOOP_CONF_OVERRIDE_FMT_STR =
      SPARK_CATALOG_CONF_PREFIX + ".%s.hadoop.";

  /**
   * Pulls any Catalog specific overrides for the Hadoop conf from the current SparkSession, which
   * can be set via `spark.sql.catalog.$catalogName.hadoop.*`
   *
   * <p>Mirrors the override of hadoop configurations for a given spark session using
   * `spark.hadoop.*`.
   *
   * <p>The SparkCatalog allows for hadoop configurations to be overridden per catalog, by setting
   * them on the SQLConf, where the following will add the property "fs.default.name" with value
   * "hdfs://hanksnamenode:8020" to the catalog's hadoop configuration. SparkSession.builder()
   * .config(s"spark.sql.catalog.$catalogName.hadoop.fs.default.name", "hdfs://hanksnamenode:8020")
   * .getOrCreate()
   *
   * @param spark The current Spark session
   * @param catalogName Name of the catalog to find overrides for.
   * @return the Hadoop Configuration that should be used for this catalog, with catalog specific
   *     overrides applied.
   */
  public static Configuration hadoopConfCatalogOverrides(SparkSession spark, String catalogName) {
    // Find keys for the catalog intended to be hadoop configurations
    final String hadoopConfCatalogPrefix = hadoopConfPrefixForCatalog(catalogName);
    final Configuration conf = spark.sessionState().newHadoopConf();
    scala.collection.immutable.Map<String, String> allConfs = spark.conf().getAll();
    scala.collection.Iterator<scala.Tuple2<String, String>> iterator = allConfs.iterator();
    while (iterator.hasNext()) {
      scala.Tuple2<String, String> entry = iterator.next();
      String k = entry._1();
      String v = entry._2();
      // these checks are copied from `spark.sessionState().newHadoopConfWithOptions()`
      // now using SparkSession.conf().getAll() for Spark 4.0 compatibility
      if (v != null && k != null && k.startsWith(hadoopConfCatalogPrefix)) {
        conf.set(k.substring(hadoopConfCatalogPrefix.length()), v);
      }
    }
    return conf;
  }

  private static String hadoopConfPrefixForCatalog(String catalogName) {
    return String.format(SPARK_CATALOG_HADOOP_CONF_OVERRIDE_FMT_STR, catalogName);
  }
}
