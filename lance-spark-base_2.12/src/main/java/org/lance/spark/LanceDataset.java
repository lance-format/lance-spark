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

import org.lance.spark.read.LanceScanBuilder;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.write.AddColumnsBackfillWrite;
import org.lance.spark.write.SparkWrite;

import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Lance Spark Dataset. */
public class LanceDataset implements SupportsRead, SupportsWrite, SupportsMetadataColumns {
  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(
          TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE);

  public static final MetadataColumn FRAGMENT_ID_COLUMN =
      new MetadataColumn() {
        @Override
        public String name() {
          return LanceConstant.FRAGMENT_ID;
        }

        @Override
        public DataType dataType() {
          return DataTypes.IntegerType;
        }

        @Override
        public boolean isNullable() {
          return false;
        }
      };

  public static final MetadataColumn ROW_ID_COLUMN =
      new MetadataColumn() {
        @Override
        public String name() {
          return LanceConstant.ROW_ID;
        }

        @Override
        public DataType dataType() {
          return DataTypes.LongType;
        }
      };

  public static final MetadataColumn ROW_ADDRESS_COLUMN =
      new MetadataColumn() {
        @Override
        public String name() {
          return LanceConstant.ROW_ADDRESS;
        }

        @Override
        public DataType dataType() {
          return DataTypes.LongType;
        }

        @Override
        public boolean isNullable() {
          return false;
        }
      };

  public static final MetadataColumn[] METADATA_COLUMNS =
      new MetadataColumn[] {ROW_ID_COLUMN, ROW_ADDRESS_COLUMN, FRAGMENT_ID_COLUMN};

  private final LanceSparkReadOptions readOptions;
  protected final StructType sparkSchema;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;

  /**
   * Creates a Lance dataset.
   *
   * @param readOptions read options including dataset URI and settings
   * @param sparkSchema spark struct type
   * @param initialStorageOptions initial storage options fetched from namespace.describeTable()
   * @param namespaceImpl namespace implementation type for credential refresh on workers
   * @param namespaceProperties namespace connection properties for credential refresh on workers
   */
  public LanceDataset(
      LanceSparkReadOptions readOptions,
      StructType sparkSchema,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    this.readOptions = readOptions;
    this.sparkSchema = sparkSchema;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
  }

  public LanceSparkReadOptions readOptions() {
    return readOptions;
  }

  public Map<String, String> getInitialStorageOptions() {
    return initialStorageOptions;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
    // Merge scan-time options with the existing read options
    LanceSparkReadOptions scanOptions = readOptions;
    if (!caseInsensitiveStringMap.isEmpty()) {
      Map<String, String> mergedOptions = new HashMap<>(readOptions.getStorageOptions());
      mergedOptions.putAll(caseInsensitiveStringMap.asCaseSensitiveMap());
      scanOptions =
          LanceSparkReadOptions.builder()
              .datasetUri(readOptions.getDatasetUri())
              .namespace(readOptions.getNamespace())
              .tableId(readOptions.getTableId())
              .fromOptions(mergedOptions)
              .build();
    }
    return new LanceScanBuilder(
        sparkSchema, scanOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
  }

  @Override
  public String name() {
    return this.readOptions.getDatasetName();
  }

  @Override
  public StructType schema() {
    return sparkSchema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    // Merge write-time options with the base options from read options
    CaseInsensitiveStringMap sparkWriteOptions = logicalWriteInfo.options();
    Map<String, String> mergedOptions = new HashMap<>(readOptions.getStorageOptions());
    mergedOptions.putAll(sparkWriteOptions.asCaseSensitiveMap());

    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(readOptions.getDatasetUri())
            .namespace(readOptions.getNamespace())
            .tableId(readOptions.getTableId())
            .fromOptions(mergedOptions)
            .build();

    List<String> backfillColumns =
        Arrays.stream(
                sparkWriteOptions.getOrDefault(LanceConstant.BACKFILL_COLUMNS_KEY, "").split(","))
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(Collectors.toList());
    if (!backfillColumns.isEmpty()) {
      return new AddColumnsBackfillWrite.AddColumnsWriteBuilder(
          sparkSchema,
          writeOptions,
          backfillColumns,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          readOptions.getTableId());
    }

    return new SparkWrite.SparkWriteBuilder(
        sparkSchema,
        writeOptions,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        readOptions.getTableId());
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    // Start with the base metadata columns
    List<MetadataColumn> columns = new ArrayList<>();
    for (MetadataColumn col : METADATA_COLUMNS) {
      columns.add(col);
    }

    // Add virtual columns for blob fields
    for (StructField field : sparkSchema.fields()) {
      if (BlobUtils.isBlobSparkField(field)) {
        final String fieldName = field.name();

        // Add position column
        columns.add(
            new MetadataColumn() {
              @Override
              public String name() {
                return fieldName + LanceConstant.BLOB_POSITION_SUFFIX;
              }

              @Override
              public DataType dataType() {
                return DataTypes.LongType;
              }

              @Override
              public boolean isNullable() {
                return true;
              }
            });

        // Add size column
        columns.add(
            new MetadataColumn() {
              @Override
              public String name() {
                return fieldName + LanceConstant.BLOB_SIZE_SUFFIX;
              }

              @Override
              public DataType dataType() {
                return DataTypes.LongType;
              }

              @Override
              public boolean isNullable() {
                return true;
              }
            });
      }
    }

    return columns.toArray(new MetadataColumn[0]);
  }
}
