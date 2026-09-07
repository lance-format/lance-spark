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

import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.read.LanceSplit;
import org.lance.spark.utils.Optional;

import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceArrowStreamScannerTest {

  /**
   * Exports each fragment of the bundled test table as an Arrow C Data Interface stream, re-imports
   * it on the JVM (standing in for a native consumer), and asserts the rows match what the Spark
   * columnar reader produces. Closing the imported reader and then the {@link
   * LanceArrowStreamScanner.LanceArrowStream} handle under the leak-checking allocator also
   * verifies the export/reader/scanner lifecycle releases cleanly.
   */
  @Test
  public void exportsFragmentAsArrowCStream() throws Exception {
    List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;
    int rowIndex = 0;
    for (int fragmentId = 0; fragmentId <= 1; fragmentId++) {
      try (LanceArrowStreamScanner.LanceArrowStream handle =
              LanceArrowStreamScanner.export(
                  fragmentId, TestUtils.TestTable1Config.inputPartition);
          ArrowReader reader = Data.importArrayStream(LanceRuntime.allocator(), handle.stream())) {

        // Schema is available before the first batch: x, y, b, c.
        assertEquals(4, reader.getVectorSchemaRoot().getSchema().getFields().size());

        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          int columns = root.getFieldVectors().size();
          for (int r = 0; r < root.getRowCount(); r++) {
            List<Long> expectedRow = expectedValues.get(rowIndex);
            for (int col = 0; col < columns; col++) {
              Object actual = root.getVector(col).getObject(r);
              assertNotNull(actual, "Null at row " + rowIndex + " column " + col);
              assertEquals(
                  expectedRow.get(col).longValue(),
                  ((Number) actual).longValue(),
                  "Mismatch at row " + rowIndex + " column " + col);
            }
            rowIndex++;
          }
        }
      }
    }
    assertEquals(4, rowIndex);
  }

  /**
   * A fragment scan that matches no rows must still export its full declared schema and drain
   * cleanly with zero batches — the schema-match check and the native consumer both rely on the
   * schema being present up front, and the empty stream must release without leaking under the
   * leak-checking allocator. An always-false filter ({@code x < 0}; every {@code x} is 0..3) yields
   * the {@code x, y, b, c} schema with no rows.
   */
  @Test
  public void exportsEmptyFragmentScanPreservingSchema() throws Exception {
    int rows = 0;
    try (LanceArrowStreamScanner.LanceArrowStream handle =
            LanceArrowStreamScanner.export(0, partitionMatchingNoRows());
        ArrowReader reader = Data.importArrayStream(LanceRuntime.allocator(), handle.stream())) {

      // The full schema is available before (and without) any batch.
      assertEquals(4, reader.getVectorSchemaRoot().getSchema().getFields().size());

      while (reader.loadNextBatch()) {
        rows += reader.getVectorSchemaRoot().getRowCount();
      }
    }
    assertEquals(0, rows);
  }

  /**
   * A native consumer receives {@link LanceArrowStreamScanner.LanceArrowStream#streamAddress()} and
   * may abandon or only partially consume the raw C stream. Closing the handle with no Java-side
   * import must still run the stream's release callback and free the struct — under the
   * leak-checking allocator, skipping either would surface an outstanding buffer.
   */
  @Test
  public void closeReleasesStreamThatWasNeverImported() throws Exception {
    LanceArrowStreamScanner.LanceArrowStream handle =
        LanceArrowStreamScanner.export(0, TestUtils.TestTable1Config.inputPartition);
    handle.close();
  }

  @Test
  public void exportInitializesAndClosesExecutorNamespace() throws Exception {
    LanceFragmentScannerTest.RecordingNamespace.reset();
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .tableId(Collections.singletonList(TestUtils.TestTable1Config.datasetName))
            .build();
    LanceInputPartition partition = namespacePartition(readOptions);

    try (LanceArrowStreamScanner.LanceArrowStream ignored =
        LanceArrowStreamScanner.export(0, partition)) {
      assertEquals(1, LanceFragmentScannerTest.RecordingNamespace.INITIALIZE_CALLS.get());
      assertNotNull(readOptions.getNamespace());
    }

    assertNull(readOptions.getNamespace());
    assertEquals(1, LanceFragmentScannerTest.RecordingNamespace.CLOSE_CALLS.get());
  }

  /**
   * A synthesized {@code _fragid} makes the native-schema check throw after {@code
   * ExecutorNamespace.acquire}, so cleanup must use the construction {@code catch} path rather than
   * {@link LanceArrowStreamScanner.LanceArrowStream#close()}. Opening the fragment can also fail
   * after acquire (for example when the JNI library is unavailable); that is the same catch path.
   */
  @Test
  public void exportClosesExecutorNamespaceWhenSchemaMismatch() {
    LanceFragmentScannerTest.RecordingNamespace.reset();
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .tableId(Collections.singletonList(TestUtils.TestTable1Config.datasetName))
            .build();
    LanceInputPartition partition =
        namespacePartition(readOptions, longSchema("x", LanceConstant.FRAGMENT_ID));

    assertThrows(RuntimeException.class, () -> LanceArrowStreamScanner.export(0, partition));

    assertNull(readOptions.getNamespace());
    assertEquals(1, LanceFragmentScannerTest.RecordingNamespace.INITIALIZE_CALLS.get());
    assertEquals(
        1,
        LanceFragmentScannerTest.RecordingNamespace.CLOSE_CALLS.get(),
        "a post-acquire export failure must close the executor namespace");
  }

  /**
   * When the native scan schema does not match the declared partition schema, export rejects the
   * partition so the caller falls back to the columnar reader. A synthesized {@code _fragid} is not
   * produced by the native scan (native returns fewer columns), while an empty projection makes the
   * native scan surface {@code _rowid} (native returns an extra column — the same shape a full-text
   * query's auto-projected {@code _score} takes).
   */
  @Test
  public void exportRejectsSchemasNeedingJvmPostProcessing() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            LanceArrowStreamScanner.export(
                0, partitionWithSchema(longSchema("x", LanceConstant.FRAGMENT_ID))));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            LanceArrowStreamScanner.export(
                0, partitionWithSchema(new StructType(new StructField[0]))));
  }

  /**
   * A pushed {@code COUNT(*)} partition is served by a dedicated aggregate reader that returns a
   * single {@code count} column; {@link LanceFragmentScanner} ignores {@code pushedAggregation} and
   * scans data rows, so exporting it would produce the wrong output. It must be rejected even
   * though its schema alone looks exportable.
   */
  @Test
  public void exportRejectsPushedAggregation() {
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    assertThrows(
        UnsupportedOperationException.class,
        () -> LanceArrowStreamScanner.export(0, partitionWithAggregation(countStar)));
  }

  private static LanceInputPartition partitionWithAggregation(Aggregation pushedAggregation) {
    return new LanceInputPartition(
        TestUtils.TestTable1Config.inputPartition.getSchema(),
        0 /* partitionId */,
        new LanceSplit(Arrays.asList(0, 1)),
        TestUtils.TestTable1Config.readOptions,
        Optional.empty() /* whereCondition */,
        Optional.empty() /* limit */,
        Optional.empty() /* offset */,
        Optional.empty() /* topNSortOrders */,
        Optional.of(pushedAggregation),
        "gate-probe" /* scanId */,
        null /* initialStorageOptions */,
        null /* namespaceImpl */,
        null /* namespaceProperties */,
        null /* partitionKeyRow */);
  }

  private static LanceInputPartition namespacePartition(LanceSparkReadOptions readOptions) {
    return namespacePartition(readOptions, TestUtils.TestTable1Config.schema);
  }

  private static LanceInputPartition namespacePartition(
      LanceSparkReadOptions readOptions, StructType schema) {
    return new LanceInputPartition(
        schema,
        0,
        new LanceSplit(Collections.singletonList(0)),
        readOptions,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        "arrow-refresh",
        Collections.emptyMap(),
        LanceFragmentScannerTest.RecordingNamespace.class.getName(),
        Collections.singletonMap("location", TestUtils.TestTable1Config.datasetUri),
        null);
  }

  private static LanceInputPartition partitionMatchingNoRows() {
    return new LanceInputPartition(
        TestUtils.TestTable1Config.inputPartition.getSchema(),
        0 /* partitionId */,
        new LanceSplit(Arrays.asList(0, 1)),
        TestUtils.TestTable1Config.readOptions,
        Optional.of("x < 0") /* whereCondition: matches no row */,
        Optional.empty() /* limit */,
        Optional.empty() /* offset */,
        Optional.empty() /* topNSortOrders */,
        Optional.empty() /* pushedAggregation */,
        "gate-probe" /* scanId */,
        null /* initialStorageOptions */,
        null /* namespaceImpl */,
        null /* namespaceProperties */,
        null /* partitionKeyRow */);
  }

  private static StructType longSchema(String... names) {
    StructField[] fields = new StructField[names.length];
    for (int i = 0; i < names.length; i++) {
      fields[i] = DataTypes.createStructField(names[i], DataTypes.LongType, true);
    }
    return new StructType(fields);
  }

  private static LanceInputPartition partitionWithSchema(StructType schema) {
    return new LanceInputPartition(
        schema,
        0 /* partitionId */,
        new LanceSplit(Arrays.asList(0, 1)),
        TestUtils.TestTable1Config.readOptions,
        Optional.empty() /* whereCondition */,
        Optional.empty() /* limit */,
        Optional.empty() /* offset */,
        Optional.empty() /* topNSortOrders */,
        Optional.empty() /* pushedAggregation */,
        "gate-probe" /* scanId */,
        null /* initialStorageOptions */,
        null /* namespaceImpl */,
        null /* namespaceProperties */,
        null /* partitionKeyRow */);
  }
}
