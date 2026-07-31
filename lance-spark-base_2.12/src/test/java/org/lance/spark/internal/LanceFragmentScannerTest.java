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
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.read.LanceColumnarPartitionReader;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.read.LanceSplit;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    List<String> expected = Arrays.asList("id", "name");
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
    List<String> expected = Arrays.asList("id", "name");
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
    // Data columns first, then version columns. Row ID, row address, and fragment ID are requested
    // outside the projection list.
    List<String> expected =
        Arrays.asList(
            "id",
            "name",
            LanceConstant.ROW_LAST_UPDATED_AT_VERSION,
            LanceConstant.ROW_CREATED_AT_VERSION);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesExcludesScore() throws Exception {
    // _score is auto-projected by Lance when a full-text query is set, so it must not be requested
    // in the native column projection.
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.SCORE, DataTypes.FloatType, true)
            });

    assertEquals(Arrays.asList("id"), callGetColumnNames(schema));
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
    // Regular data columns in schema order, then projected version metadata columns.
    List<String> expected =
        Arrays.asList(
            "z_last_column",
            "a_first_column",
            "m_middle_column",
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
   * executor opens a namespace-backed table with the flag disabled, the partition reader must
   * <i>not</i> reconstruct the namespace client. Without this gate, executors of Kerberized HMS
   * catalogs hit {@code GSS initiate failed} because they lack a TGT for the eager {@code
   * describeTable()} RPC.
   */
  @Test
  public void testPartitionReaderSkipsNamespaceWhenExecutorCredentialRefreshDisabled()
      throws Exception {
    RecordingNamespace.reset();
    LanceInputPartition partition =
        namespacePartition(
            TestUtils.TestTable1Config.datasetUri,
            Collections.singletonList(0),
            "testPartitionReaderSkipsNamespaceWhenExecutorCredentialRefreshDisabled",
            false);
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
      while (reader.next()) {
        reader.get().close();
      }
    }

    assertNull(
        partition.getReadOptions().getNamespace(),
        "executor_credential_refresh=false must skip namespace rebuild on the executor");
    assertEquals(
        0,
        RecordingNamespace.INITIALIZE_CALLS.get(),
        "executor_credential_refresh=false must not load or initialize the namespace impl");
  }

  @Test
  public void testPartitionReaderReusesAndClosesExecutorNamespace() throws Exception {
    RecordingNamespace.reset();
    LanceInputPartition partition =
        namespacePartition(
            TestUtils.TestTable1Config.datasetUri,
            Arrays.asList(0, 1),
            "testPartitionReaderReusesAndClosesExecutorNamespace",
            true);

    LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition);
    int rowsRead = 0;
    try {
      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        assertNotNull(batch);
        rowsRead += batch.numRows();
        batch.close();
      }
    } finally {
      reader.close();
    }
    reader.close();

    assertEquals(TestUtils.TestTable1Config.expectedValues.size(), rowsRead);
    assertEquals(
        1,
        RecordingNamespace.INITIALIZE_CALLS.get(),
        "all fragments in one Spark task must reuse one namespace client");
    assertEquals(
        1,
        RecordingNamespace.CLOSE_CALLS.get(),
        "the task-owned namespace client must be closed exactly once");
  }

  @Test
  public void testPartitionReaderClosesExecutorNamespaceWhenFragmentOpenFails() throws Exception {
    RecordingNamespace.reset();
    LanceInputPartition partition =
        namespacePartition(
            TestUtils.TestTable1Config.datasetUri,
            Collections.singletonList(999999),
            "testPartitionReaderClosesExecutorNamespaceWhenFragmentOpenFails",
            true);
    LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition);

    RuntimeException failure = assertThrows(RuntimeException.class, reader::next);

    assertNotNull(failure.getCause());
    assertEquals(1, RecordingNamespace.INITIALIZE_CALLS.get());
    assertEquals(
        1,
        RecordingNamespace.CLOSE_CALLS.get(),
        "namespace initialization must be rolled back when fragment open fails");
    reader.close();
    assertEquals(
        1,
        RecordingNamespace.CLOSE_CALLS.get(),
        "close after failed initialization cleanup must be idempotent");
  }

  private LanceInputPartition namespacePartition(
      String location, List<Integer> fragments, String scanId, boolean executorCredentialRefresh) {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(location)
            .tableId(Collections.singletonList(TestUtils.TestTable1Config.datasetName))
            .executorCredentialRefresh(executorCredentialRefresh)
            .build();
    return new LanceInputPartition(
        TestUtils.TestTable1Config.schema,
        0,
        new LanceSplit(fragments),
        readOptions,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        scanId,
        Collections.emptyMap(),
        RecordingNamespace.class.getName(),
        Collections.singletonMap("location", location),
        null);
  }

  @Test
  public void getBlobColumnNamesIncludesBlobV2ReadColumns() throws Exception {
    Method method =
        LanceFragmentScanner.class.getDeclaredMethod("getBlobColumnNames", StructType.class);
    method.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Set<String> names =
        (java.util.Set<String>)
            method.invoke(
                null,
                new StructType(
                    new StructField[] {
                      new StructField(
                          "payload",
                          BlobUtils.BLOB_DESCRIPTOR_STRUCT,
                          true,
                          new MetadataBuilder()
                              .putString(
                                  BlobUtils.ARROW_EXTENSION_NAME_KEY,
                                  BlobUtils.ARROW_EXTENSION_BLOB_V2)
                              .build())
                    }));
    assertEquals(java.util.Collections.singleton("payload"), names);
  }

  /**
   * Public, top-level-by-FQCN, no-arg {@link LanceNamespace} so that {@link
   * LanceNamespace#connect(String, Map, BufferAllocator)} can resolve it via {@code Class.forName}
   * during executor-side namespace initialization.
   */
  public static class RecordingNamespace implements LanceNamespace, AutoCloseable {
    static final AtomicInteger INITIALIZE_CALLS = new AtomicInteger();
    static final AtomicInteger CLOSE_CALLS = new AtomicInteger();

    private String location;

    public RecordingNamespace() {}

    static void reset() {
      INITIALIZE_CALLS.set(0);
      CLOSE_CALLS.set(0);
    }

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
      INITIALIZE_CALLS.incrementAndGet();
      location = properties.get("location");
    }

    @Override
    public String namespaceId() {
      return "recording";
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
      return new DescribeTableResponse().location(location);
    }

    @Override
    public void close() {
      CLOSE_CALLS.incrementAndGet();
    }
  }
}
