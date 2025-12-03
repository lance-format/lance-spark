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

import org.lance.spark.internal.LanceDatasetAdapter;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Lance DataSource for Spark. Supports both direct path-based access and catalog-based access.
 *
 * <p><b>Direct path-based access (no catalog configuration required):</b>
 *
 * <pre>
 * // Reading
 * spark.read.format("lance").load("s3://bucket/path/to/table.lance")
 *
 * // Writing
 * df.write.format("lance").save("s3://bucket/path/to/table.lance")
 * </pre>
 *
 * <p><b>Catalog-based access (for SQL DDL operations):</b>
 *
 * <pre>
 * // Configure Lance catalog
 * spark.sql.catalog.lance = com.lancedb.lance.spark.LanceNamespaceSparkCatalog
 *
 * // Then use SQL
 * spark.sql("SELECT * FROM lance.namespace.table")
 * </pre>
 */
public abstract class LanceDataSource implements SupportsCatalogOptions, DataSourceRegister {
  public static final String name = "lance";

  /** Option key for specifying which catalog to use. */
  public static final String OPTION_CATALOG = "catalog";

  /** The standard Lance catalog name that users typically configure. */
  private static final String LANCE_CATALOG_NAME = "lance";

  /** Default catalog name used when no catalog is explicitly configured. */
  private static final String DEFAULT_LANCE_CATALOG_NAME = "lance_default";

  private static final String DEFAULT_LANCE_CATALOG_CONFIG =
      "spark.sql.catalog." + DEFAULT_LANCE_CATALOG_NAME;

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    Optional<StructType> schema = LanceDatasetAdapter.getSchema(LanceConfig.from(options));
    return schema.isPresent() ? schema.get() : null;
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return createDataset(LanceConfig.from(properties), schema);
  }

  @Override
  public String shortName() {
    return name;
  }

  /**
   * Indicates that this DataSource supports external metadata (user-provided schema). This is
   * useful when the schema cannot be inferred or when overriding the inferred schema is desired.
   */
  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    LanceConfig config = LanceConfig.from(options);
    return new LanceIdentifier(config.getDatasetUri());
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    SparkSession spark = SparkSession.active();

    // 1. If user explicitly specified catalog in options, use it
    if (options.containsKey(OPTION_CATALOG)) {
      String catalogName = options.get(OPTION_CATALOG);
      if (catalogName != null && !catalogName.isEmpty()) {
        return catalogName;
      }
    }

    // 2. Check if any Lance catalog is configured, prioritize "lance" catalog
    String lanceCatalogConfig = "spark.sql.catalog." + LANCE_CATALOG_NAME;
    if (spark.conf().contains(lanceCatalogConfig)) {
      return LANCE_CATALOG_NAME;
    }

    // 3. Otherwise, setup and use the default catalog
    setupDefaultLanceCatalog(spark);
    return DEFAULT_LANCE_CATALOG_NAME;
  }

  /**
   * Sets up a default Lance catalog if one is not already configured. This allows users to use
   * spark.read.format("lance").load("path") without explicitly configuring a catalog.
   */
  private static void setupDefaultLanceCatalog(SparkSession spark) {
    if (!spark.conf().contains(DEFAULT_LANCE_CATALOG_CONFIG)) {
      spark.conf().set(DEFAULT_LANCE_CATALOG_CONFIG, LanceCatalog.class.getName());
    }
  }

  public abstract LanceDataset createDataset(LanceConfig config, StructType sparkSchema);
}
