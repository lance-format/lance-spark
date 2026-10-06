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
 * <p>A table that requests a blob encoding through {@code <column>.lance.encoding = 'blob'} without
 * pinning a version resolves to {@link BlobUtils#MIN_BLOB_V2_FILE_FORMAT_VERSION}. Bump that
 * constant when new blob tables should move; a Lance stable bump leaves it alone.
 *
 * <p>A schema that already carries legacy (v1) blob metadata keeps v1; see {@code
 * resolveForInheritedBlobV1}.
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
    // Whether the table asks for a blob column has to be read from the properties here: the
    // matching schema metadata is attached by SchemaConverter below, which needs the resolved
    // version to know whether to write v1 or v2 metadata.
    boolean blobEncodingRequested = BlobUtils.requestsBlobEncoding(tableProperties);
    String resolved =
        resolveVersion(sparkSchema, blobEncodingRequested, tableVersion, catalogDefaultVersion);
    StructType schema =
        SchemaConverter.processSchemaWithProperties(sparkSchema, tableProperties, resolved);
    return new CreateTableSpec(schema, resolved);
  }

  private static String resolveVersion(
      StructType schema,
      boolean blobEncodingRequested,
      String tableVersion,
      String catalogDefaultVersion) {
    boolean schemaHasBlobV1 = BlobUtils.hasBlobV1Fields(schema);

    if (!BlobUtils.hasBlobV2Fields(schema)) {
      String requested = tableVersion != null ? tableVersion : catalogDefaultVersion;

      if (schemaHasBlobV1) {
        return resolveForInheritedBlobV1(schema, tableVersion, catalogDefaultVersion);
      }

      if (requested == null && blobEncodingRequested) {
        // Pin the version so the schema metadata and the created dataset agree.
        LOG.info(
            "Creating table with file_format_version {} so its blob columns use blob v2."
                + " Set file_format_version to 2.0 or 2.1 for the legacy (v1) blob encoding.",
            BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION);
        return BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION;
      }
      return requested;
    }

    if (schemaHasBlobV1) {
      // Blob v2 columns force the version to 2.2 or newer, which cannot store the v1 columns in
      // the same schema. Lance would reject the write; say why here instead.
      throw new IllegalArgumentException(
          "Schema mixes legacy (v1) blob columns "
              + BlobUtils.blobV1ColumnNames(schema)
              + " with blob v2 columns "
              + BlobUtils.blobV2ColumnNames(schema)
              + ". Blob v2 requires file_format_version "
              + BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION
              + " or newer, which cannot store v1 blob columns. Create the table with one blob"
              + " encoding, converting the v1 columns first if they should become blob v2.");
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

  /**
   * Resolves the version for a schema that already carries legacy (v1) blob metadata, typically a
   * schema read back from an existing v1 table and reused for a new one.
   *
   * <p>A table-level {@code file_format_version} that cannot store them is explicit user intent and
   * is rejected; a catalog default is held down with a warning.
   */
  private static String resolveForInheritedBlobV1(
      StructType schema, String tableVersion, String catalogDefaultVersion) {
    if (tableVersion != null) {
      if (BlobUtils.knownToRejectBlobV1(tableVersion)) {
        throw new IllegalArgumentException(
            "Schema carries legacy (v1) blob columns "
                + BlobUtils.blobV1ColumnNames(schema)
                + ", which Lance rejects from file_format_version "
                + BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION
                + " on. Requested file_format_version '"
                + tableVersion
                + "' cannot store them. Use "
                + BlobUtils.MAX_BLOB_V1_FILE_FORMAT_VERSION
                + " or older, or rebuild the column as blob v2.");
      }
      return tableVersion;
    }

    if (catalogDefaultVersion != null && !BlobUtils.knownToRejectBlobV1(catalogDefaultVersion)) {
      return catalogDefaultVersion;
    }

    if (catalogDefaultVersion != null) {
      LOG.warn(
          "Catalog default file_format_version '{}' cannot store this table's legacy (v1) blob"
              + " columns. Creating the table with version {} instead",
          catalogDefaultVersion,
          BlobUtils.MAX_BLOB_V1_FILE_FORMAT_VERSION);
    }
    return BlobUtils.MAX_BLOB_V1_FILE_FORMAT_VERSION;
  }

  public StructType schema() {
    return schema;
  }

  /** The file format version to create the dataset with, or null to let Lance pick its default. */
  public String fileFormatVersion() {
    return fileFormatVersion;
  }
}
