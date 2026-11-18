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
package org.lance.spark.write;

import org.lance.spark.LancePositionDeltaOperation;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Verifies that the row-level DML path promotes the option map into the typed write options, the
 * way {@code LanceDataset#newWriteBuilder} does for INSERT. Only {@code storageOptions} used to be
 * set here, so catalog-level write settings were silently dropped on UPDATE/DELETE/MERGE.
 */
public class PositionDeltaWriteOptionsTest {

  private static final StructType SCHEMA = new StructType().add("id", DataTypes.IntegerType, false);

  private static LogicalWriteInfo writeInfo(Map<String, String> options) {
    return new LogicalWriteInfo() {
      @Override
      public String queryId() {
        return "test-query";
      }

      @Override
      public StructType schema() {
        return SCHEMA;
      }

      @Override
      public CaseInsensitiveStringMap options() {
        return new CaseInsensitiveStringMap(options);
      }
    };
  }

  private static SparkPositionDeltaWriteBuilder newBuilder(
      Map<String, String> storedOptions, Map<String, String> writeOptions, String tableFormat) {
    return newBuilder(storedOptions, writeOptions, tableFormat, Collections.emptyMap());
  }

  private static SparkPositionDeltaWriteBuilder newBuilder(
      Map<String, String> storedOptions,
      Map<String, String> writeOptions,
      String tableFormat,
      Map<String, String> tableProperties) {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri("/tmp/position-delta-write-options.lance")
            .fromOptions(storedOptions)
            .build();
    LancePositionDeltaOperation operation =
        new LancePositionDeltaOperation(
            RowLevelOperation.Command.UPDATE,
            SCHEMA,
            readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            false,
            tableFormat,
            tableProperties);
    return (SparkPositionDeltaWriteBuilder) operation.newWriteBuilder(writeInfo(writeOptions));
  }

  @Test
  public void testStoredOptionsArePromotedToTypedFields() {
    Map<String, String> stored = new HashMap<>();
    stored.put(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_FILE, "7");
    stored.put(LanceSparkWriteOptions.CONFIG_BATCH_SIZE, "128");
    stored.put(LanceSparkWriteOptions.CONFIG_USE_LARGE_VAR_TYPES, "true");

    LanceSparkWriteOptions options =
        newBuilder(stored, Collections.emptyMap(), null).getWriteOptions();

    Assertions.assertEquals(7, options.getMaxRowsPerFile());
    Assertions.assertEquals(128, options.getBatchSize());
    Assertions.assertTrue(options.isUseLargeVarTypes());
    Assertions.assertEquals("7", options.getStorageOptions().get("max_row_per_file"));
  }

  @Test
  public void testWriteOptionOverridesTheTableFileFormatVersion() {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(LanceSparkWriteOptions.CONFIG_FILE_FORMAT_VERSION, "2.2");

    LanceSparkWriteOptions options =
        newBuilder(Collections.emptyMap(), writeOptions, "2.0").getWriteOptions();

    Assertions.assertEquals("2.2", options.getFileFormatVersion());
  }

  @Test
  public void testTableFileFormatVersionAppliesWhenOptionsDoNotSetIt() {
    LanceSparkWriteOptions options =
        newBuilder(Collections.emptyMap(), Collections.emptyMap(), "2.0").getWriteOptions();

    Assertions.assertEquals("2.0", options.getFileFormatVersion());
  }

  @Test
  public void testTableEnableStableRowIdsAppliesWhenOptionsDoNotSetIt() {
    Map<String, String> tableProperties =
        Collections.singletonMap(LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS, "true");

    LanceSparkWriteOptions options =
        newBuilder(Collections.emptyMap(), Collections.emptyMap(), null, tableProperties)
            .getWriteOptions();

    Assertions.assertEquals(Boolean.TRUE, options.getEnableStableRowIds());
  }

  @Test
  public void testWriteOptionOverridesTableEnableStableRowIds() {
    Map<String, String> writeOptions =
        Collections.singletonMap(LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS, "false");
    Map<String, String> tableProperties =
        Collections.singletonMap(LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS, "true");

    LanceSparkWriteOptions options =
        newBuilder(Collections.emptyMap(), writeOptions, null, tableProperties).getWriteOptions();

    Assertions.assertEquals(Boolean.FALSE, options.getEnableStableRowIds());
  }
}
