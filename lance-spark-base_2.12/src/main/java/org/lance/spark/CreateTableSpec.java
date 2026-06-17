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

import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.SchemaConverter;

import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Resolved schema and Lance file format version for CREATE TABLE.
 *
 * <p>If the incoming schema already contains blob v2 metadata, the create version must support blob
 * v2. A table-level {@code file_format_version} is explicit user intent and is rejected when
 * incompatible. A catalog default is upgraded with a warning because it may not have been chosen
 * for this table's schema.
 *
 * <p>After the version resolves, property-driven blob encodings are applied once against the final
 * version.
 */
public final class CreateTableSpec {

  private static final Logger LOG = LoggerFactory.getLogger(CreateTableSpec.class);

  private final StructType schema;
  private final String fileFormatVersion;

  private CreateTableSpec(StructType schema, String fileFormatVersion) {
    this.schema = schema;
    this.fileFormatVersion = fileFormatVersion;
  }

  /**
   * Resolves the create-time schema and file format version.
   *
   * @param sparkSchema the Spark schema requested for the new table
   * @param tableProperties TBLPROPERTIES from CREATE TABLE (may request blob encodings and a
   *     per-table {@code file_format_version})
   * @param catalogDefaultVersion the catalog-level {@code file_format_version} default, or null
   * @throws IllegalArgumentException when the table-level properties request a version that cannot
   *     store the table's blob v2 columns
   */
  public static CreateTableSpec resolve(
      StructType sparkSchema, Map<String, String> tableProperties, String catalogDefaultVersion) {
    String tableVersion =
        tableProperties.get(LanceSparkCatalogConfig.TABLE_OPT_FILE_FORMAT_VERSION);
    String resolved = resolveVersion(sparkSchema, tableVersion, catalogDefaultVersion);
    StructType schema =
        SchemaConverter.processSchemaWithProperties(sparkSchema, tableProperties, resolved);
    return new CreateTableSpec(schema, resolved);
  }

  private static String resolveVersion(
      StructType schema, String tableVersion, String catalogDefaultVersion) {
    if (!BlobUtils.hasBlobV2Fields(schema)) {
      return tableVersion != null ? tableVersion : catalogDefaultVersion;
    }

    if (tableVersion != null) {
      if (!BlobUtils.fileFormatSupportsBlobV2(tableVersion)) {
        throw new IllegalArgumentException(
            "Blob v2 columns require Lance file_format_version "
                + BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION
                + " or newer. Requested file_format_version '"
                + tableVersion
                + "' cannot store blob v2 columns.");
      }
      return tableVersion;
    }

    if (catalogDefaultVersion == null) {
      return BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION;
    }

    if (BlobUtils.fileFormatSupportsBlobV2(catalogDefaultVersion)) {
      return catalogDefaultVersion;
    }

    LOG.warn(
        "Catalog default file_format_version '{}' cannot store this table's blob v2 columns."
            + " Creating the table with version {} instead",
        catalogDefaultVersion,
        BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION);
    return BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION;
  }

  public StructType schema() {
    return schema;
  }

  /** The file format version to create the dataset with, or null to let Lance pick its default. */
  public String fileFormatVersion() {
    return fileFormatVersion;
  }
}
