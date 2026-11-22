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

import org.lance.Fragment;
import org.lance.Transaction;
import org.lance.operation.CreateIndex;
import org.lance.spark.LanceDataset;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LanceNamedArgument;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.LanceOptimizeIndexExec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
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
                    + "max_source_fragments=128,"
                    + "compaction_mode='try_binary_copy',"
                    + "binary_copy_read_batch_bytes=1048576,"
                    + "max_source_rows=1000000,"
                    + "max_source_bytes=1073741824,"
                    + "excluded_fragment_ids='4294967295',"
                    + "data_storage_version='stable'"
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
  public void testOptimizeIndex() {
    prepareDataset();
    spark.sql(String.format("alter table %s create index idx_id using zonemap (id)", fullTable));
    spark.sql(String.format("insert into %s values (10, 'text_10')", fullTable));

    Row before =
        spark.sql(String.format("show indexes in %s", fullTable)).collectAsList().stream()
            .filter(row -> "idx_id".equals(row.getAs("name")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Index not found: idx_id"));
    long unindexedFragments = before.getAs("num_unindexed_fragments");
    Assertions.assertTrue(unindexedFragments > 0);

    Dataset<Row> result =
        spark.sql(String.format("alter table %s optimize index idx_id", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(index_name,StringType,false),StructField(fragments_indexed,LongType,false),StructField(segments_before,LongType,false),StructField(segments_after,LongType,false))",
        result.schema().toString());
    Row optimized = result.collectAsList().get(0);
    Assertions.assertEquals("idx_id", optimized.getAs("index_name"));
    Assertions.assertEquals(unindexedFragments, optimized.<Long>getAs("fragments_indexed"));
    Assertions.assertTrue(optimized.<Long>getAs("segments_before") > 0);
    Assertions.assertTrue(optimized.<Long>getAs("segments_after") > 0);

    Row after =
        spark.sql(String.format("show indexes in %s", fullTable)).collectAsList().stream()
            .filter(row -> "idx_id".equals(row.getAs("name")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Index not found: idx_id"));
    Assertions.assertEquals(0L, after.<Long>getAs("num_unindexed_fragments"));

    Row noOp =
        spark
            .sql(String.format("alter table %s optimize index idx_id", fullTable))
            .collectAsList()
            .get(0);
    Assertions.assertEquals(0L, noOp.<Long>getAs("fragments_indexed"));
    Assertions.assertEquals(
        noOp.<Long>getAs("segments_before").longValue(),
        noOp.<Long>getAs("segments_after").longValue());
  }

  @Test
  public void testOptimizeIndexAcceptsMemWalCatchUpCommit() throws Exception {
    String protocolTableName = tableName + "_protocol";
    String protocolTable = catalogName + ".default." + protocolTableName;
    spark.sql(
        String.format(
            "create table %s (id int, text string) using lance "
                + "partitioned by (bucket(4, text))",
            protocolTable));
    spark.sql(String.format("insert into %s values (1, 'same-shard')", protocolTable));
    spark.sql(
        String.format("alter table %s create index idx_id using zonemap (id)", protocolTable));

    Row noOp =
        spark
            .sql(String.format("alter table %s optimize index idx_id", protocolTable))
            .collectAsList()
            .get(0);
    Assertions.assertEquals(0L, noOp.<Long>getAs("fragments_indexed"));

    try (org.lance.Dataset dataset = openDataset(protocolTableName)) {
      long beforeVersion = dataset.version();
      CreateIndex emptyCreateIndex =
          CreateIndex.builder()
              .withNewIndices(Collections.emptyList())
              .withRemovedIndices(Collections.emptyList())
              .build();
      try (Transaction transaction = new Transaction(beforeVersion, emptyCreateIndex);
          org.lance.Dataset committed = dataset.commitTransaction(transaction)) {
        Assertions.assertEquals(beforeVersion + 1, committed.version());

        LanceOptimizeIndexExec exec =
            new LanceOptimizeIndexExec(
                null,
                null,
                "idx_id",
                JavaConverters.asScalaBuffer(Collections.<LanceNamedArgument>emptyList()).toSeq());
        Method indexDelta =
            LanceOptimizeIndexExec.class.getDeclaredMethod(
                "indexDelta", org.lance.Dataset.class, long.class, long.class);
        indexDelta.setAccessible(true);
        Object delta = indexDelta.invoke(exec, committed, beforeVersion, committed.version());

        Assertions.assertEquals(0L, metric(delta, "fragmentsIndexed"));
        Assertions.assertEquals(0L, metric(delta, "segmentsAdded"));
        Assertions.assertEquals(0L, metric(delta, "segmentsRemoved"));
      }
    }
  }

  private org.lance.Dataset openDataset(String currentTableName) throws Exception {
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, currentTableName));
    return Utils.openDatasetBuilder(table.readOptions())
        .initialStorageOptions(table.getInitialStorageOptions())
        .build();
  }

  private static long metric(Object delta, String name) throws Exception {
    return ((Long) delta.getClass().getDeclaredMethod(name).invoke(delta)).longValue();
  }

  @Test
  public void testOptimizeMissingIndex() {
    prepareDataset();

    assertOptimizeIndexFails("missing", "", "Index 'missing' does not exist");
  }

  @Test
  public void testOptimizeRejectsSystemIndex() {
    prepareDataset();

    assertOptimizeIndexFails(
        "__lance_frag_reuse", "", "Cannot optimize system index '__lance_frag_reuse'");
  }

  @Test
  public void testOptimizeIndexValidatesOptions() {
    prepareDataset();
    spark.sql(String.format("alter table %s create index idx_id using zonemap (id)", fullTable));

    assertOptimizeIndexFails(
        "idx_id", "with (unknown_option=1)", "Unsupported OPTIMIZE INDEX options: unknown_option");
    assertOptimizeIndexFails(
        "idx_id",
        "with (num_indices_to_merge=0, NUM_INDICES_TO_MERGE=1)",
        "Duplicate OPTIMIZE INDEX options: num_indices_to_merge");
    assertOptimizeIndexFails(
        "idx_id",
        "with (num_indices_to_merge='two')",
        "num_indices_to_merge must be a non-negative integer");
    assertOptimizeIndexFails(
        "idx_id",
        "with (num_indices_to_merge=-1)",
        "num_indices_to_merge must be between 0 and 2147483647");
    assertOptimizeIndexFails(
        "idx_id",
        "with (num_indices_to_merge=2147483648)",
        "num_indices_to_merge must be between 0 and 2147483647");
  }

  private void assertOptimizeIndexFails(String indexName, String options, String expectedMessage) {
    Exception error =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "alter table %s optimize index %s %s", fullTable, indexName, options))
                    .collectAsList());
    Assertions.assertTrue(
        exceptionChainMessages(error).contains(expectedMessage),
        () -> "Expected error containing '" + expectedMessage + "', got: " + error);
  }

  private static String exceptionChainMessages(Throwable throwable) {
    StringBuilder messages = new StringBuilder();
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        messages.append(current.getMessage()).append('\n');
      }
    }
    return messages.toString();
  }

  @Test
  public void testMaterializeDeletionsThresholdAcceptsAnyNumericLiteral() {
    prepareDataset();

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

  @Test
  public void testCompactionModes() {
    for (String mode : new String[] {"reencode", "try_binary_copy", "FORCE_BINARY_COPY"}) {
      spark.sql(String.format("drop table if exists %s", fullTable));
      prepareDataset();

      Dataset<Row> result =
          spark.sql(String.format("optimize %s with (compaction_mode='%s')", fullTable, mode));

      Assertions.assertEquals(
          "[10,1,10,1]", result.collectAsList().get(0).toString(), "compaction_mode=" + mode);
      Assertions.assertEquals(10L, spark.table(fullTable).count(), "compaction_mode=" + mode);
    }
  }

  @Test
  public void testMaxSourceRowsBoundsCompaction() {
    prepareDataset();

    // target_rows_per_fragment=5 plans two 5-row tasks; a 5-row budget admits only the first.
    Dataset<Row> result =
        spark.sql(
            String.format(
                "optimize %s with (target_rows_per_fragment=5, max_source_rows=5)", fullTable));

    Assertions.assertEquals("[5,1,5,1]", result.collectAsList().get(0).toString());
  }

  @Test
  public void testMaxSourceBytesBoundsCompaction() throws Exception {
    prepareDataset();

    long firstTaskBytes;
    try (org.lance.Dataset dataset = openDataset(tableName)) {
      firstTaskBytes =
          sortedFragmentIds(dataset).stream()
              .limit(5)
              .mapToLong(
                  id ->
                      dataset.getFragment(id).metadata().getFiles().stream()
                          .mapToLong(f -> f.getFileSizeBytes())
                          .sum())
              .sum();
    }

    Dataset<Row> result =
        spark.sql(
            String.format(
                "optimize %s with (target_rows_per_fragment=5, max_source_bytes=%d)",
                fullTable, firstTaskBytes));

    Assertions.assertEquals("[5,1,5,1]", result.collectAsList().get(0).toString());
  }

  @Test
  public void testExcludedFragmentIds() throws Exception {
    prepareDataset();

    List<Integer> before;
    try (org.lance.Dataset dataset = openDataset(tableName)) {
      before = sortedFragmentIds(dataset);
    }
    int first = before.get(0);
    int middle = before.get(5);

    // The excluded fragments stay untouched and split the rest into two separate tasks.
    Dataset<Row> result =
        spark.sql(
            String.format(
                "optimize %s with (excluded_fragment_ids=' %d, %d ')", fullTable, first, middle));

    Assertions.assertEquals("[8,2,8,2]", result.collectAsList().get(0).toString());
    try (org.lance.Dataset dataset = openDataset(tableName)) {
      List<Integer> after = sortedFragmentIds(dataset);
      Assertions.assertTrue(after.contains(first), "excluded fragment was compacted: " + after);
      Assertions.assertTrue(after.contains(middle), "excluded fragment was compacted: " + after);
      Assertions.assertEquals(4, after.size());
    }
    Assertions.assertEquals(10L, spark.table(fullTable).count());
  }

  @Test
  public void testExcludedFragmentIdsAcceptsSingleIntegerLiteral() throws Exception {
    prepareDataset();

    int first;
    try (org.lance.Dataset dataset = openDataset(tableName)) {
      first = sortedFragmentIds(dataset).get(0);
    }

    Dataset<Row> result =
        spark.sql(String.format("optimize %s with (excluded_fragment_ids=%d)", fullTable, first));

    Assertions.assertEquals("[9,1,9,1]", result.collectAsList().get(0).toString());
  }

  @Test
  public void testDataStorageVersionRewritesFilesWithoutChangingTableVersion() throws Exception {
    spark.sql(
        String.format(
            "create table %s (id int, text string) using lance "
                + "tblproperties ('file_format_version' = '2.0')",
            fullTable));
    spark.sql(String.format("insert into %s values (1, 'a'), (2, 'b'), (3, 'c')", fullTable));

    Dataset<Row> result =
        spark.sql(String.format("optimize %s with (data_storage_version='2.1')", fullTable));

    Assertions.assertEquals(1L, result.collectAsList().get(0).<Long>getAs("fragments_added"));
    try (org.lance.Dataset dataset = openDataset(tableName)) {
      Assertions.assertEquals("2.0", dataset.getLanceFileFormatVersion());
      for (Fragment fragment : dataset.getFragments()) {
        for (org.lance.fragment.DataFile file : fragment.metadata().getFiles()) {
          Assertions.assertEquals(
              "2.1", file.getFileMajorVersion() + "." + file.getFileMinorVersion());
        }
      }
    }
    Assertions.assertEquals(3L, spark.table(fullTable).count());
  }

  @Test
  public void testInvalidNewOptionsNameTheOption() {
    prepareDataset();

    String[][] cases = {
      {"compaction_mode='bogus'", "'compaction_mode' option must be one of"},
      {"compaction_mode=1", "'compaction_mode' option must be a string literal"},
      {"binary_copy_read_batch_bytes=0", "'binary_copy_read_batch_bytes' option must be greater"},
      {"max_source_rows=0", "'max_source_rows' option must be greater than 0"},
      {"max_source_bytes=-1", "'max_source_bytes' option must be greater than 0"},
      {"max_source_rows='10'", "'max_source_rows' option must be an integer literal"},
      {"excluded_fragment_ids='1,,2'", "'excluded_fragment_ids' option must be a comma-separated"},
      {"excluded_fragment_ids='1,a'", "'excluded_fragment_ids' option must be a comma-separated"},
      {"excluded_fragment_ids=-1", "'excluded_fragment_ids' option must be a comma-separated"},
      {
        "excluded_fragment_ids='4294967296'",
        "'excluded_fragment_ids' option must be a comma-separated"
      },
      {"data_storage_version='9.9'", "'data_storage_version' option must be one of"},
      {"data_storage_version=2.1", "'data_storage_version' option must be a string literal"},
    };
    for (String[] c : cases) {
      Exception exception =
          Assertions.assertThrows(
              Exception.class,
              () -> spark.sql(String.format("optimize %s with (%s)", fullTable, c[0])),
              c[0] + " should be rejected");
      Assertions.assertTrue(
          exception.getMessage().contains(c[1]),
          c[0] + ": expected '" + c[1] + "', got: " + exception.getMessage());
    }
  }

  private static List<Integer> sortedFragmentIds(org.lance.Dataset dataset) {
    return dataset.getFragments().stream()
        .map(Fragment::getId)
        .sorted()
        .collect(Collectors.toList());
  }
}
