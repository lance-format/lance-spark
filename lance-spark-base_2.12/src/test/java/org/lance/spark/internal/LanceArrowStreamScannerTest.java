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

import org.lance.spark.LanceRuntime;
import org.lance.spark.TestUtils;

import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
