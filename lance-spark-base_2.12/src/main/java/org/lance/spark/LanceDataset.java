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

  LanceConfig config;
  protected final StructType sparkSchema;

  /**
   * Creates a Lance dataset.
   *
   * @param config read config
   * @param sparkSchema spark struct type
   */
  public LanceDataset(LanceConfig config, StructType sparkSchema) {
    this.config = config;
    this.sparkSchema = sparkSchema;
  }

  public LanceConfig config() {
    return config;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
    // Merge scan-time options with the existing config options
    LanceConfig scanConfig = config;
    if (!caseInsensitiveStringMap.isEmpty()) {
      Map<String, String> mergedOptions = new HashMap<>(config.getOptions());
      mergedOptions.putAll(caseInsensitiveStringMap.asCaseSensitiveMap());
      scanConfig = LanceConfig.from(mergedOptions, config.getDatasetUri());
    }
    return new LanceScanBuilder(sparkSchema, scanConfig);
  }

  @Override
  public String name() {
    return this.config.getDatasetName();
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
    // Merge write-time options with the existing config options
    CaseInsensitiveStringMap writeOptions = logicalWriteInfo.options();
    LanceConfig writeConfig = config;
    if (!writeOptions.isEmpty()) {
      Map<String, String> mergedOptions = new HashMap<>(config.getOptions());
      mergedOptions.putAll(writeOptions.asCaseSensitiveMap());
      writeConfig = LanceConfig.from(mergedOptions, config.getDatasetUri());
    }

    List<String> backfillColumns =
        Arrays.stream(writeOptions.getOrDefault(LanceConstant.BACKFILL_COLUMNS_KEY, "").split(","))
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(Collectors.toList());
    if (!backfillColumns.isEmpty()) {
      return new AddColumnsBackfillWrite.AddColumnsWriteBuilder(
          sparkSchema, writeConfig, backfillColumns);
    }

    return new SparkWrite.SparkWriteBuilder(sparkSchema, writeConfig);
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
