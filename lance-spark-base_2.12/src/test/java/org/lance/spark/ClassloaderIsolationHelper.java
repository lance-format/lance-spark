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

import org.lance.Dataset;
import org.lance.ipc.AsyncScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.ipc.ArrowReader;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/** Runs a real Lance scan from the isolated classloader created by the bootstrap process. */
public final class ClassloaderIsolationHelper {

  private static final int EXPECTED_ROWS = 4;

  private ClassloaderIsolationHelper() {}

  public static void run() throws Exception {
    URL datasetUrl =
        ClassloaderIsolationHelper.class.getResource("/example_db/test_dataset1.lance");
    if (datasetUrl == null) {
      throw new IllegalStateException("example dataset not found");
    }

    LanceSparkReadOptions readOptions = LanceSparkReadOptions.from(datasetUrl.toString());
    try (Dataset dataset = Utils.openDatasetBuilder(readOptions).build();
        AsyncScanner scanner =
            AsyncScanner.create(
                dataset, new ScanOptions.Builder().build(), LanceRuntime.allocator());
        ArrowReader reader = scanner.scanBatchesAsync().get(20, TimeUnit.SECONDS)) {
      int rowCount = 0;
      while (reader.loadNextBatch()) {
        rowCount += reader.getVectorSchemaRoot().getRowCount();
      }
      if (rowCount != EXPECTED_ROWS) {
        throw new AssertionError("Expected " + EXPECTED_ROWS + " rows, found " + rowCount);
      }
    }
  }
}
