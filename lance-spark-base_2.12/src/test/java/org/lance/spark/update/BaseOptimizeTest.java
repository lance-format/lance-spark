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
                    + "defer_index_remap=true,"
                    + "max_source_fragments=128"
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

  @Test
  public void testMaterializeDeletionsThresholdAcceptsAnyNumericLiteral() {
    prepareDataset();

    // The grammar boxes 1 as Long, 0.5 as Float and 0.5d as Double, so every spelling has to
    // reach withMaterializeDeletionsThreshold instead of failing the cast. Only the first call
    // has fragments left to compact, so assert acceptance rather than the compaction counts.
    for (String threshold : new String[] {"1", "0.5", "0.5d", "0.5f"}) {
      Dataset<Row> result =
          spark.sql(
              String.format(
                  "optimize %s with (materialize_deletions=true, "
                      + "materialize_deletions_threshold=%s)",
                  fullTable, threshold));
      Assertions.assertEquals(
          1,
          result.collectAsList().size(),
          "threshold literal " + threshold + " should be accepted");
    }
  }

  @Test
  public void testNonNumericOptionNamesTheOption() {
    prepareDataset();

    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    String.format(
                        "optimize %s with (materialize_deletions_threshold='half')", fullTable)));
    Assertions.assertTrue(
        exception.getMessage().contains("'materialize_deletions_threshold'"),
        "error should name the option, got: " + exception.getMessage());
  }

  @Test
  public void testFractionalLongOptionIsRejectedNotTruncated() {
    prepareDataset();

    // num_threads is documented as Long. Accepting a fractional literal here would run the
    // compaction with a silently truncated value, so it has to be rejected outright.
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> spark.sql(String.format("optimize %s with (num_threads=2.5)", fullTable)));
    Assertions.assertTrue(
        exception.getMessage().contains("'num_threads'")
            && exception.getMessage().contains("integer literal"),
        "error should name the option and ask for an integer, got: " + exception.getMessage());
  }
}
