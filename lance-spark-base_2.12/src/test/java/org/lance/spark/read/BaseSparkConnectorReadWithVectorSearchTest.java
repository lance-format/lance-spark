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

/*
 *The test logic is same with org.lance.VectorSearchTest.test_knn
 */

public abstract class BaseSparkConnectorReadWithVectorSearchTest {
  private static SparkSession spark;
  private static String dbPath;
  private static Dataset<Row> data;

  @BeforeAll
  static void setup() {

    Query.Builder builder = new Query.Builder();
    float[] key = new float[32];
    for (int i = 0; i < 32; i++) {
      key[i] = (float) (i + 32);
    }
    builder.setK(1);
    builder.setColumn("vec");
    builder.setKey(key);
    builder.setUseIndex(true);
    builder.setDistanceType(DistanceType.L2);

    spark =
        SparkSession.builder()
            .appName("spark-lance-connector-test")
            .master("local")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .getOrCreate();
    dbPath = TestUtils.TestTable1Config.dbPath;
    data =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_NEAREST, QueryUtils.queryToString(builder.build()))
            .option(
                LanceSparkReadOptions.CONFIG_DATASET_URI,
                TestUtils.getDatasetUri(dbPath, "test_dataset5"))
            .load();
    data.createOrReplaceTempView("test_dataset5");
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void validateData() {
    Set<Integer> expectedI = new HashSet<>(Arrays.asList(1, 81, 161, 241, 321));
    Set<Integer> actualI = new HashSet<>();
    List<Row> rows = data.collectAsList();
    for (int i = 0; i < rows.size(); i++) {
      actualI.add(rows.get(i).getInt(0));
    }
    assertEquals(expectedI, actualI, "Unexpected values in 'i' column");
  }
}
