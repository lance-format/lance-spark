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

import org.lance.DataStorageVersion;
import org.lance.Dataset;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

/**
 * Centralized file-format version policy for Spark data-file writes and compaction.
 *
 * <p>Lance separates a dataset's manifest default ({@code data_storage_format}) from the physical
 * file version of individual fragments ({@code FLAG_MIXED_DATA_FILE_VERSIONS}). For example,
 * promoting a legacy blob column to Blob v2 in the manifest leaves the dataset's default version
 * unchanged (such as {@code 2.1}), while Spark writes Blob v2 columns as Blob v2 structs that
 * require {@value BlobUtils#MIN_BLOB_V2_FILE_FORMAT_VERSION} or newer data files.
 *
 * <p>Callers use these methods to pick a compatible data-file version when the user did not
 * explicitly request {@code file_format_version}, without mutating the dataset's manifest default.
 */
public final class FileFormatVersionResolver {

  private static final String COMPACTION_DATA_STORAGE_VERSION_KEY =
      "lance.compaction.data_storage_version";

  private FileFormatVersionResolver() {}

  /**
   * Resolves the physical data-file version to write for {@code schema} when no explicit {@code
   * file_format_version} option was supplied by the user.
   */
  public static String resolveForDataFiles(StructType schema, String datasetDefaultVersion) {
    if (datasetDefaultVersion != null
        && BlobUtils.isV2FormatBelowBlobV2(datasetDefaultVersion)
        && BlobUtils.containsBlobV2SparkField(schema)) {
      return BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION;
    }
    return datasetDefaultVersion;
  }

  /**
   * Resolves the physical data-file version to write for Arrow {@code schema} when no explicit
   * {@code file_format_version} option was supplied by the user.
   */
  public static String resolveForDataFiles(Schema schema, String datasetDefaultVersion) {
    if (datasetDefaultVersion != null
        && BlobUtils.isV2FormatBelowBlobV2(datasetDefaultVersion)
        && BlobUtils.containsBlobV2ArrowField(schema)) {
      return BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION;
    }
    return datasetDefaultVersion;
  }

  /**
   * Resolves an explicit compaction output data-file version when the dataset's default format
   * cannot encode its current schema (such as a {@code 2.0}/{@code 2.1} dataset with promoted Blob
   * v2 fields) and {@code lance.compaction.data_storage_version} is not configured on the dataset.
   */
  public static Optional<DataStorageVersion> resolveForCompaction(Dataset dataset) {
    if (dataset.getConfig().containsKey(COMPACTION_DATA_STORAGE_VERSION_KEY)) {
      return Optional.empty();
    }
    String currentDefault = dataset.getLanceFileFormatVersion();
    String resolved = resolveForDataFiles(dataset.getSchema(), currentDefault);
    if (resolved != null && !resolved.equals(currentDefault)) {
      return Optional.of(DataStorageVersion.fromRustString(resolved));
    }
    return Optional.empty();
  }
}
