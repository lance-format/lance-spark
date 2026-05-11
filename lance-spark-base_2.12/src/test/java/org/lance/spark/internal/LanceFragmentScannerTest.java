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
package org.lance.spark.internal;

import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceFragmentScannerTest {

  private List<String> callGetColumnNames(StructType schema)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method =
        LanceFragmentScanner.class.getDeclaredMethod("getColumnNames", StructType.class);
    method.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) method.invoke(null, schema);
    return result;
  }

  @Test
  public void testGetColumnNamesWithOnlyDataColumns() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("age", DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id", "name", "age");
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithRowId() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField(LanceConstant.ROW_ID, DataTypes.LongType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id", "name", LanceConstant.ROW_ID);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithRowAddress() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.ROW_ADDRESS, DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id", "name", LanceConstant.ROW_ADDRESS);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithVersionColumns() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_LAST_UPDATED_AT_VERSION, DataTypes.LongType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected =
        Arrays.asList(
            "id",
            "name",
            LanceConstant.ROW_LAST_UPDATED_AT_VERSION,
            LanceConstant.ROW_CREATED_AT_VERSION);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithAllMetadataColumns() throws Exception {
    // Test with all metadata columns in the order defined in LanceDataset.METADATA_COLUMNS
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField(LanceConstant.ROW_ID, DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.ROW_ADDRESS, DataTypes.LongType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_LAST_UPDATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    // Data columns first, then metadata columns in METADATA_COLUMNS order
    // Note: FRAGMENT_ID is excluded as it's not included in the projection
    List<String> expected =
        Arrays.asList(
            "id",
            "name",
            LanceConstant.ROW_ID,
            LanceConstant.ROW_ADDRESS,
            LanceConstant.ROW_LAST_UPDATED_AT_VERSION,
            LanceConstant.ROW_CREATED_AT_VERSION);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesExcludesBlobColumns() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
              DataTypes.createStructField(
                  "data" + LanceConstant.BLOB_POSITION_SUFFIX, DataTypes.LongType, true),
              DataTypes.createStructField(
                  "data" + LanceConstant.BLOB_SIZE_SUFFIX, DataTypes.LongType, true)
            });

    List<String> result = callGetColumnNames(schema);
    // Blob metadata columns should be excluded
    List<String> expected = Arrays.asList("id", "data");
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesOrderingWithMixedColumns() throws Exception {
    // Test that regular columns come first, then metadata columns in the correct order
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(LanceConstant.ROW_ID, DataTypes.LongType, true),
              DataTypes.createStructField("z_last_column", DataTypes.StringType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField("a_first_column", DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.ROW_ADDRESS, DataTypes.LongType, true),
              DataTypes.createStructField("m_middle_column", DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    // Regular data columns in schema order, then metadata columns in METADATA_COLUMNS order
    List<String> expected =
        Arrays.asList(
            "z_last_column",
            "a_first_column",
            "m_middle_column",
            LanceConstant.ROW_ID,
            LanceConstant.ROW_ADDRESS,
            LanceConstant.ROW_CREATED_AT_VERSION);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithFragmentId() throws Exception {
    // FRAGMENT_ID should be excluded from the projection
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id");
    assertEquals(expected, result);
  }

  /**
   * Locks down the executor-branch contract for {@code executor_credential_refresh=false}: when an
   * executor opens a fragment for a namespace-backed table with the flag disabled, {@link
   * LanceFragmentScanner#create} must <i>not</i> reconstruct the namespace client. Without this
   * gate, executors of Kerberized HMS catalogs hit {@code GSS initiate failed} because they lack a
   * TGT for the eager {@code describeTable()} RPC.
   *
   * <p>Strategy: hand a real, loadable {@link LanceNamespace} impl to the partition. If the gate
   * regresses (rebuild not skipped), {@code LanceNamespace.connect} would succeed via {@code
   * Class.forName}, {@link RecordingNamespace#initialize} would run, and {@code
   * readOptions.setNamespace(...)} would fire — all observable here. The bogus dataset URI lets the
   * outer {@code Utils.openDatasetBuilder().build()} call fail predictably, since the gate runs
   * <i>before</i> the dataset is opened. No real Lance dataset is required.
   */
  @Test
  public void testCreateSkipsNamespaceRebuildWhenExecutorCredentialRefreshDisabled() {
    RecordingNamespace.INITIALIZE_CALLS.set(0);

    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri("file:///tmp/__lance_nonexistent_for_executor_gate_test__")
            .executorCredentialRefresh(false)
            .build();

    LanceInputPartition partition =
        new LanceInputPartition(
            new StructType(),
            0,
            null,
            readOptions,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            "test-scan",
            Collections.emptyMap(),
            RecordingNamespace.class.getName(),
            Collections.emptyMap(),
            null);

    assertThrows(RuntimeException.class, () -> LanceFragmentScanner.create(0, partition));

    assertNull(
        readOptions.getNamespace(),
        "executor_credential_refresh=false must skip namespace rebuild on the executor");
    assertEquals(
        0,
        RecordingNamespace.INITIALIZE_CALLS.get(),
        "executor_credential_refresh=false must not load or initialize the namespace impl");
  }

  /**
   * Public, top-level-by-FQCN, no-arg {@link LanceNamespace} so that {@link
   * LanceNamespace#connect(String, Map, BufferAllocator)} can resolve it via {@code Class.forName}
   * if the executor branch is (incorrectly) taken.
   */
  public static class RecordingNamespace implements LanceNamespace {
    static final AtomicInteger INITIALIZE_CALLS = new AtomicInteger();

    public RecordingNamespace() {}

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
      INITIALIZE_CALLS.incrementAndGet();
    }

    @Override
    public String namespaceId() {
      return "recording";
    }
  }
}
