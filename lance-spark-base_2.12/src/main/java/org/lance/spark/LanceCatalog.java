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

import org.lance.Dataset;
import org.lance.WriteParams;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.util.Map;

/**
 * A simple Lance catalog that supports both path-based and catalog-based table access.
 *
 * <p>For path-based access (e.g., spark.read.format("lance").load("path")), this catalog is
 * automatically registered as "lance_default" and uses {@link LanceIdentifier} to identify tables.
 *
 * <p>For catalog-based access (e.g., spark.sql.catalog.lance configuration), users should use
 * {@link BaseLanceNamespaceSparkCatalog} for full namespace support.
 */
public class LanceCatalog implements TableCatalog {
  private CaseInsensitiveStringMap options;
  private String catalogName = "lance";
  private LanceSparkCatalogConfig catalogConfig;

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Please use LanceNamespaceSparkCatalog for table listing");
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    String datasetUri = getDatasetUri(ident);
    LanceSparkReadOptions readOptions = createReadOptions(datasetUri);
    StructType schema;
    try (Dataset dataset =
        Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(datasetUri)
            .readOptions(readOptions.toReadOptions())
            .build()) {
      schema = LanceArrowUtils.fromArrowSchema(dataset.getSchema());
    } catch (IllegalArgumentException e) {
      throw new NoSuchTableException(ident);
    }
    return new LanceDataset(readOptions, schema, null, null, null);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String datasetUri = getDatasetUri(ident);
    LanceSparkReadOptions readOptions = createReadOptions(datasetUri);
    try {
      Dataset.write()
          .allocator(LanceRuntime.allocator())
          .uri(datasetUri)
          .schema(LanceArrowUtils.toArrowSchema(schema, "UTC", true, false))
          .mode(WriteParams.WriteMode.CREATE)
          .storageOptions(readOptions.getStorageOptions())
          .execute()
          .close();
    } catch (IllegalArgumentException e) {
      throw new TableAlreadyExistsException(ident);
    }
    return new LanceDataset(readOptions, schema, null, null, null);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropTable(Identifier ident) {
    String datasetUri = getDatasetUri(ident);
    Dataset.drop(datasetUri, catalogConfig.getStorageOptions());
    return true;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.options = options;
    // Parse catalog configuration
    this.catalogConfig = LanceSparkCatalogConfig.from(options.asCaseSensitiveMap());
  }

  /**
   * Creates LanceSparkReadOptions for this catalog.
   *
   * @param datasetUri the dataset URI
   * @return a new LanceSparkReadOptions with catalog settings
   */
  private LanceSparkReadOptions createReadOptions(String datasetUri) {
    return LanceSparkReadOptions.builder()
        .datasetUri(datasetUri)
        .withCatalogDefaults(catalogConfig)
        .build();
  }

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Extracts the full dataset URI from an identifier.
   *
   * <p>If the identifier is a {@link LanceIdentifier}, use its location() method. Otherwise,
   * reconstruct the path from namespace and name.
   *
   * @param ident the identifier
   * @return the full dataset URI
   */
  private String getDatasetUri(Identifier ident) {
    if (ident instanceof LanceIdentifier) {
      return ((LanceIdentifier) ident).location();
    }

    // Reconstruct path from namespace and name
    String[] namespace = ident.namespace();
    String name = ident.name();

    if (namespace == null || namespace.length == 0) {
      return name;
    }

    // Join namespace parts with "/" and append the name
    StringBuilder sb = new StringBuilder();
    for (String ns : namespace) {
      if (sb.length() > 0 && !sb.toString().endsWith("/")) {
        sb.append("/");
      }
      sb.append(ns);
    }
    if (!sb.toString().endsWith("/")) {
      sb.append("/");
    }
    sb.append(name);
    return sb.toString();
  }
}
