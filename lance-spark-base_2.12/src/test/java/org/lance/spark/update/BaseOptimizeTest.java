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
package org.lance.spark.update;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseOptimizeTest {
  protected String catalogName = "lance_test";
  protected String tableName = "optimize_test";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-optimize-test")
            .master("local[10]") // 10 tasks to make sure that the fragment number is 10
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  protected void prepareDataset() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  @Test
  public void testOptimizeToOneFragment() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(String.format("optimize %s with (target_rows_per_fragment=20000)", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_removed,LongType,true),StructField(fragments_added,LongType,true),StructField(files_removed,LongType,true),StructField(files_added,LongType,true))",
        result.schema().toString());
    Assertions.assertEquals("[10,1,10,1]", result.collectAsList().get(0).toString());
  }

  @Test
  public void testOptimizeToTwoFragments() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(String.format("optimize %s with (target_rows_per_fragment=5)", fullTable));

    Assertions.assertEquals("[10,2,10,2]", result.collectAsList().get(0).toString());
  }

  @Test
  public void testNoOptimize() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(String.format("optimize %s with (target_rows_per_fragment=1)", fullTable));

    Assertions.assertEquals("[0,0,0,0]", result.collectAsList().get(0).toString());
  }

  @Test
  public void testWithFullArgs() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "optimize %s with "
                    + "("
                    + "target_rows_per_fragment=20000,"
                    + "max_rows_per_group=20000,"
                    + "max_bytes_per_file=20000,"
                    + "materialize_deletions=true,"
                    + "materialize_deletions_threshold=0.2f,"
                    + "num_threads=2,"
                    + "batch_size=2000,"
                    + "defer_index_remap=true"
                    + ")",
                fullTable));

    Assertions.assertEquals("[10,1,10,1]", result.collectAsList().get(0).toString());
  }

  @Test
  public void testWithoutArgs() {
    prepareDataset();

    Dataset<Row> result = spark.sql(String.format("optimize %s", fullTable));

    Assertions.assertEquals("[10,1,10,1]", result.collectAsList().get(0).toString());
  }
}
