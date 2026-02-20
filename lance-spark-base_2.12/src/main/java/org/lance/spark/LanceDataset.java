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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.spark.read.LanceScanBuilder;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.write.AddColumnsBackfillWrite;
import org.lance.spark.write.SparkWrite;
import org.lance.spark.write.StagedCommit;
import org.lance.spark.write.UpdateColumnsBackfillWrite;

import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.StagedTable;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** Lance Spark Dataset. */
public class LanceDataset
    implements SupportsRead, SupportsWrite, SupportsMetadataColumns, StagedTable {

  private static final Logger LOG = LoggerFactory.getLogger(LanceDataset.class);

  /** The type of staging operation for staged table creation. */
  public enum StagingOperation {
    NONE,
    CREATE,
    REPLACE,
    CREATE_OR_REPLACE
  }

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

  public static final MetadataColumn ROW_LAST_UPDATED_AT_VERSION_COLUMN =
      new MetadataColumn() {
        @Override
        public String name() {
          return LanceConstant.ROW_LAST_UPDATED_AT_VERSION;
        }

        @Override
        public DataType dataType() {
          return DataTypes.LongType;
        }
      };

  public static final MetadataColumn ROW_CREATED_AT_VERSION_COLUMN =
      new MetadataColumn() {
        @Override
        public String name() {
          return LanceConstant.ROW_CREATED_AT_VERSION;
        }

        @Override
        public DataType dataType() {
          return DataTypes.LongType;
        }
      };

  public static final MetadataColumn[] METADATA_COLUMNS =
      new MetadataColumn[] {
        ROW_ID_COLUMN,
        ROW_ADDRESS_COLUMN,
        ROW_LAST_UPDATED_AT_VERSION_COLUMN,
        ROW_CREATED_AT_VERSION_COLUMN,
        FRAGMENT_ID_COLUMN
      };

  protected final LanceSparkReadOptions readOptions;
  protected final StructType sparkSchema;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;

  /** Staging-related fields for StagedTable support. */
  private final StagingOperation stagingOperation;

  private final LanceNamespace stagingNamespace;
  private final List<String> tableIdList;
  private final Schema arrowSchema;
  private final Map<String, String> storageOptions;
  private final boolean tableExisted;
  private final AtomicReference<StagedCommit> stagedCommit = new AtomicReference<>();

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
    this(
        readOptions,
        sparkSchema,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        StagingOperation.NONE,
        null,
        null,
        null,
        null,
        false);
  }

  /**
   * Creates a Lance dataset with staging support.
   *
   * @param readOptions read options including dataset URI and settings
   * @param sparkSchema spark struct type
   * @param initialStorageOptions initial storage options fetched from namespace.describeTable()
   * @param namespaceImpl namespace implementation type for credential refresh on workers
   * @param namespaceProperties namespace connection properties for credential refresh on workers
   * @param stagingOperation the staging operation type
   * @param stagingNamespace the Lance namespace for table operations
   * @param tableIdList the table identifier path
   * @param arrowSchema the Arrow schema for the table
   * @param storageOptions storage options for table creation
   * @param tableExisted whether the table existed at staging time
   */
  public LanceDataset(
      LanceSparkReadOptions readOptions,
      StructType sparkSchema,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      StagingOperation stagingOperation,
      LanceNamespace stagingNamespace,
      List<String> tableIdList,
      Schema arrowSchema,
      Map<String, String> storageOptions,
      boolean tableExisted) {
    this.readOptions = readOptions;
    this.sparkSchema = sparkSchema;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.stagingOperation = stagingOperation;
    this.stagingNamespace = stagingNamespace;
    this.tableIdList = tableIdList;
    this.arrowSchema = arrowSchema;
    this.storageOptions = storageOptions;
    this.tableExisted = tableExisted;
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

    List<String> updateColumns =
        Arrays.stream(
                sparkWriteOptions.getOrDefault(LanceConstant.UPDATE_COLUMNS_KEY, "").split(","))
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(Collectors.toList());
    if (!updateColumns.isEmpty()) {
      return new UpdateColumnsBackfillWrite.UpdateColumnsWriteBuilder(
          sparkSchema,
          writeOptions,
          updateColumns,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          readOptions.getTableId());
    }

    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            sparkSchema,
            writeOptions,
            initialStorageOptions,
            namespaceImpl,
            namespaceProperties,
            readOptions.getTableId());

    if (stagingOperation != StagingOperation.NONE) {
      builder.setStagedCommit(stagedCommit);
      if (!tableExisted) {
        builder.setNewTable(true);
      }
      if (stagingOperation == StagingOperation.REPLACE
          || stagingOperation == StagingOperation.CREATE_OR_REPLACE) {
        builder.truncate();
      }
    }
    return builder;
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

  @Override
  public void commitStagedChanges() {
    if (stagingOperation == StagingOperation.NONE) {
      return;
    }

    StagedCommit commit = stagedCommit.get();
    if (commit == null) {
      throw new IllegalStateException(
          "No staged commit found. Was newWriteBuilder() called and write completed?");
    }

    try {
      commit.commit();
    } finally {
      commit.close();
    }
  }

  @Override
  public void abortStagedChanges() {
    if (stagingOperation == StagingOperation.NONE) {
      return;
    }

    // Close the staged commit if it exists (without committing)
    StagedCommit commit = stagedCommit.get();
    if (commit != null) {
      commit.close();
    }

    // Deregister the table if it was newly created
    if (!tableExisted) {
      DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
      tableIdList.forEach(deregisterRequest::addIdItem);
      try {
        stagingNamespace.deregisterTable(deregisterRequest);
      } catch (Exception e) {
        LOG.warn(
            "Failed to deregister table {} during abort. Manual cleanup may be required.",
            tableIdList,
            e);
      }
    }
  }
}
