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
package org.lance.spark.read;

import org.lance.index.DistanceType;
import org.lance.ipc.Query;
import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.QueryUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public abstract class BaseSparkConnectorReadWithVectorSearchTest {
  private static SparkSession spark;
  private static String dbPath;

  // test_dataset5 has 5 fragments and no pre-built vector index.
  private static Dataset<Row> indexedData; // useIndex=true  → single dataset-level scan
  private static Dataset<Row> bruteForceData; // useIndex=false → per-fragment parallel scan

  @BeforeAll
  static void setup() {
    float[] key = new float[32];
    for (int i = 0; i < 32; i++) {
      key[i] = (float) (i + 32);
    }

    spark =
        SparkSession.builder()
            .appName("spark-lance-connector-test")
            .master("local")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .getOrCreate();
    dbPath = TestUtils.TestTable1Config.dbPath;
    String datasetUri = TestUtils.getDatasetUri(dbPath, "test_dataset5");

    Query.Builder indexedBuilder = new Query.Builder();
    indexedBuilder.setK(1);
    indexedBuilder.setColumn("vec");
    indexedBuilder.setKey(key);
    indexedBuilder.setUseIndex(true);
    indexedBuilder.setDistanceType(DistanceType.L2);
    indexedData =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_NEAREST,
                QueryUtils.queryToString(indexedBuilder.build()))
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, datasetUri)
            .load();

    Query.Builder bruteForceBuilder = new Query.Builder();
    bruteForceBuilder.setK(1);
    bruteForceBuilder.setColumn("vec");
    bruteForceBuilder.setKey(key);
    bruteForceBuilder.setUseIndex(false);
    bruteForceBuilder.setDistanceType(DistanceType.L2);
    bruteForceData =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(
                LanceSparkReadOptions.CONFIG_NEAREST,
                QueryUtils.queryToString(bruteForceBuilder.build()))
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, datasetUri)
            .load();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testIndexedSearchReturnsGlobalTopK() {
    // useIndex=true uses a single dataset-level scan, so k=1 returns exactly 1 row
    // globally — the nearest neighbor across all fragments combined.
    List<Row> rows = indexedData.collectAsList();
    assertEquals(1, rows.size(), "Indexed k=1 search must return exactly 1 row globally");
    assertEquals(1, rows.get(0).getInt(0), "Unexpected value in 'i' column");
  }

  @Test
  public void testBruteForceSearchReturnsPerFragmentCandidates() {
    // useIndex=false keeps per-fragment splits for parallel brute-force scan.
    // With k=1 and 5 fragments, each fragment returns its local top-1,
    // yielding 5 candidate rows for the caller to aggregate.
    Set<Integer> expectedI = new HashSet<>(Arrays.asList(1, 81, 161, 241, 321));
    Set<Integer> actualI = new HashSet<>();
    List<Row> rows = bruteForceData.collectAsList();
    for (Row row : rows) {
      actualI.add(row.getInt(0));
    }
    assertEquals(expectedI, actualI, "Unexpected values in 'i' column");
  }
}
