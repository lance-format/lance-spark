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

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LanceNamespaceSparkCatalog extends BaseLanceNamespaceSparkCatalog {

  private static final Logger logger = LoggerFactory.getLogger(LanceNamespaceSparkCatalog.class);

  private static final String CDF_UNSUPPORTED_WARNING =
      "CDF (Change Data Feed) via enable_stable_row_ids is not fully supported on Spark 3.4. "
          + "UPDATE operations required for CDF are not supported by Spark 3.4's V2 data source. "
          + "Consider upgrading to Spark 3.5 or later for full CDF support.";

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    super.initialize(name, options);
    if ("true"
        .equalsIgnoreCase(options.get(LanceSparkCatalogConfig.TABLE_OPT_ENABLE_STABLE_ROW_IDS))) {
      logger.warn(CDF_UNSUPPORTED_WARNING);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    if (properties != null
        && "true"
            .equalsIgnoreCase(
                properties.get(LanceSparkCatalogConfig.TABLE_OPT_ENABLE_STABLE_ROW_IDS))) {
      logger.warn(CDF_UNSUPPORTED_WARNING);
    }
    return super.createTable(ident, schema, partitions, properties);
  }

  @Override
  public LanceDataset createDataset(
      LanceSparkReadOptions readOptions,
      StructType sparkSchema,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    return new LancePositionDeltaDataset(
        readOptions, sparkSchema, initialStorageOptions, namespaceImpl, namespaceProperties);
  }
}
