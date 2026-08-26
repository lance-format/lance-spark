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

import org.lance.index.Index;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Base test for distributed REFRESH INDEX. */
public abstract class BaseRefreshIndexTest {

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("text", DataTypes.StringType, false)
          });

  protected String catalogName = "lance_test";
  protected String tableName;
  protected String fullTable;

  protected SparkSession spark;

  @TempDir Path tempDir;
  protected String tableDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-refresh-index-test")
            .master("local[3]")
            .config("spark.default.parallelism", "10")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "refresh_index_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
    this.tableDir =
        FileSystems.getDefault().getPath(testRoot, this.tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      // Spark 4 declares SparkSession.close() as throwing IOException; Spark 3 does not.
      spark.close();
    }
  }

  /** Refresh indexes the fragments an index does not cover, and only those. */
  @Test
  public void testRefreshIndexesOnlyUncoveredFragments() {
    createTable();
    appendFragment(0, 10);
    appendFragment(10, 20);
    createZonemapIndex();

    Assertions.assertEquals(2, coverage().size(), "Create should cover both initial fragments");
    Set<UUID> segmentsBefore = segmentUuids();

    appendFragment(20, 30);
    appendFragment(30, 40);

    Row result = refresh("");
    Assertions.assertEquals(2L, result.getLong(0), "Only the two new fragments should be indexed");
    Assertions.assertTrue(result.getLong(1) >= 1L, "At least one segment should be added");
    Assertions.assertEquals("idx_id", result.getString(2));

    Assertions.assertEquals(4, coverage().size(), "All four fragments should now be covered");
    Assertions.assertTrue(
        segmentUuids().containsAll(segmentsBefore),
        "Refresh must preserve the segments covering already-indexed fragments");
  }

  /** A refresh with nothing to do must not commit a new version. */
  @Test
  public void testRefreshIsNoOpWhenFullyCovered() {
    createTable();
    appendFragment(0, 10);
    appendFragment(10, 20);
    createZonemapIndex();

    long versionBefore = datasetVersion();
    Row result = refresh("");

    Assertions.assertEquals(0L, result.getLong(0));
    Assertions.assertEquals(0L, result.getLong(1));
    Assertions.assertEquals(
        versionBefore, datasetVersion(), "A no-op refresh must not commit a version");
  }

  /** A deferred index covers nothing, so refreshing one builds it over the whole table. */
  @Test
  public void testRefreshPopulatesDeferredIndex() {
    createTable();
    appendFragment(0, 10);
    appendFragment(10, 20);
    spark.sql(
        String.format(
            "alter table %s create index idx_id using zonemap (id) with (train = false)",
            fullTable));

    Assertions.assertTrue(coverage().isEmpty(), "A deferred index should cover no fragments");

    // Statistics must be readable for an index that has no files yet.
    Row deferred =
        spark.sql(String.format("show indexes from %s", fullTable)).collectAsList().get(0);
    Assertions.assertEquals(0L, deferred.getLong(4), "A deferred index covers no rows");
    Assertions.assertEquals(
        0.0d, deferred.getDouble(7), 1e-9, "A deferred index should report 0 percent coverage");

    Row result = refresh("");
    Assertions.assertEquals(2L, result.getLong(0));
    Assertions.assertEquals(2, coverage().size());

    Row stats = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList().get(0);
    Assertions.assertEquals(0L, stats.getLong(5), "No fragment should remain unindexed");
    Assertions.assertEquals(0L, stats.getLong(6), "No row should remain unindexed");
    Assertions.assertEquals(100.0d, stats.getDouble(7), 1e-9, "Index should report full coverage");
  }

  /** Data stays queryable and complete through a refresh. */
  @Test
  public void testRefreshedIndexAnswersQueries() {
    createTable();
    appendFragment(0, 10);
    appendFragment(10, 20);
    createZonemapIndex();
    appendFragment(20, 30);

    refresh("");

    Assertions.assertEquals(30L, spark.table(fullTable).count(), "No rows should be lost");
    Assertions.assertEquals(
        1L,
        spark.sql(String.format("select * from %s where id = 25", fullTable)).count(),
        "Point lookup in a refreshed fragment should find its row");
    Assertions.assertEquals(
        10L,
        spark.sql(String.format("select * from %s where id >= 20 and id < 30", fullTable)).count(),
        "Range scan over a refreshed fragment should return every row");
    Assertions.assertEquals(
        0L,
        spark.sql(String.format("select * from %s where id = 99", fullTable)).count(),
        "Absent value should match nothing");
  }

  /**
   * Compaction retires the fragments an index covers and creates new ones the index does not, so a
   * refresh is what restores coverage afterwards.
   *
   * <p>Asserts only the post-refresh state: query results are correct and every fragment is
   * covered. It deliberately does not pin down the intermediate post-compaction state, which is a
   * separate concern from this command.
   */
  @Test
  public void testRefreshRestoresCoverageAfterCompaction() {
    createTable();
    // One fragment already at the compaction target so it is not a candidate, plus smaller ones
    // that are: compaction then retires part of the index's coverage and leaves the rest.
    appendFragment(0, 100);
    appendFragment(100, 110);
    appendFragment(110, 120);
    appendFragment(120, 130);
    spark.sql(
        String.format(
            "alter table %s create index idx_id using zonemap (id) with (num_segments = 1)",
            fullTable));

    spark.sql(String.format("optimize %s with (target_rows_per_fragment = 50)", fullTable));

    refresh("");

    Row stats = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList().get(0);
    Assertions.assertEquals(
        0L,
        stats.getLong(5),
        "Every fragment should be covered after refreshing a compacted table");
    Assertions.assertEquals(
        100.0d, stats.getDouble(7), 1e-9, "Coverage should be complete after refresh");

    Assertions.assertEquals(130L, spark.table(fullTable).count(), "No rows should be lost");
    Assertions.assertEquals(
        1L,
        spark.sql(String.format("select * from %s where id = 105", fullTable)).count(),
        "A row moved by compaction must be found through the refreshed index");
    Assertions.assertEquals(
        30L,
        spark.sql(String.format("select * from %s where id >= 95 and id < 125", fullTable)).count(),
        "A range spanning compacted and untouched fragments must return every row exactly once");
  }

  /**
   * A table one row short of full coverage must not report 100 percent, or an operator polling SHOW
   * INDEXES would conclude the index is complete while a fragment is still unindexed.
   */
  @Test
  public void testIndexedPercentNeverOverstatesCoverage() {
    createTable();
    appendFragment(0, 20000);
    createZonemapIndex();
    appendFragment(20000, 20001);

    Row stats = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList().get(0);
    Assertions.assertEquals(1L, stats.getLong(6), "Exactly one row should be unindexed");
    Assertions.assertTrue(
        stats.getDouble(7) < 100.0d,
        "A partially indexed table must not report 100 percent, got " + stats.getDouble(7));

    refresh("");

    Row after = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList().get(0);
    Assertions.assertEquals(
        100.0d, after.getDouble(7), 1e-9, "Full coverage should report exactly 100 percent");
  }

  /** num_segments bounds the number of parallel build tasks, and so the segments committed. */
  @Test
  public void testRefreshRespectsNumSegments() {
    createTable();
    appendFragment(0, 10);
    createZonemapIndex();
    appendFragment(10, 20);
    appendFragment(20, 30);
    appendFragment(30, 40);
    appendFragment(40, 50);

    Row result = refresh("with (num_segments = 2)");
    Assertions.assertEquals(4L, result.getLong(0));
    Assertions.assertEquals(2L, result.getLong(1), "Four fragments should build as two segments");
    Assertions.assertEquals(5, coverage().size());
  }

  /**
   * The column to rebuild is resolved from the index's field id, not from remembered text, so a
   * nested field must round-trip back to a path the index builder accepts.
   */
  @Test
  public void testRefreshIndexOnNestedField() {
    spark.sql(
        String.format(
            "create table %s (id int, payload struct<value: int>) using lance;", fullTable));
    spark.sql(String.format("insert into %s values (1, named_struct('value', 10))", fullTable));
    spark.sql(
        String.format(
            "alter table %s create index idx_nested using btree (payload.value)", fullTable));
    spark.sql(String.format("insert into %s values (2, named_struct('value', 20))", fullTable));

    Row result =
        spark
            .sql(String.format("alter table %s refresh index idx_nested", fullTable))
            .collectAsList()
            .get(0);

    Assertions.assertEquals(1L, result.getLong(0), "The appended fragment should be indexed");
    Assertions.assertEquals("idx_nested", result.getString(2));
    Assertions.assertEquals(
        1L,
        spark.sql(String.format("select * from %s where payload.value = 20", fullTable)).count(),
        "The refreshed nested index must answer a lookup on the new fragment");
  }

  /** Index options are forwarded to the segment build. */
  @Test
  public void testRefreshForwardsIndexOptions() {
    createTable();
    appendFragment(0, 10);
    createZonemapIndex();
    appendFragment(10, 20);

    Row result = refresh("with (rows_per_zone = 2048)");
    Assertions.assertEquals(1L, result.getLong(0));
    Assertions.assertEquals(2, coverage().size());
  }

  @Test
  public void testRefreshUnknownIndexFails() {
    createTable();
    appendFragment(0, 10);

    Exception error =
        Assertions.assertThrows(
            Exception.class,
            () -> spark.sql(String.format("alter table %s refresh index missing_idx", fullTable)));
    Assertions.assertTrue(
        causeChain(error).contains("does not exist"),
        "Expected an actionable message, got: " + causeChain(error));
  }

  @Test
  public void testRefreshSystemIndexFails() {
    createTable();
    appendFragment(0, 10);

    Exception error =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    String.format("alter table %s refresh index __lance_frag_reuse", fullTable)));
    Assertions.assertTrue(
        causeChain(error).contains("system index"),
        "Expected a system-index rejection, got: " + causeChain(error));
  }

  /** Options that only apply to a full rebuild are rejected rather than silently ignored. */
  @ParameterizedTest
  @ValueSource(strings = {"train = false", "build_mode = 'range'", "rows_per_range = 100"})
  public void testRefreshRejectsFullBuildOnlyOptions(String option) {
    createTable();
    appendFragment(0, 10);
    createZonemapIndex();
    appendFragment(10, 20);

    Exception error =
        Assertions.assertThrows(Exception.class, () -> refresh(String.format("with (%s)", option)));
    Assertions.assertTrue(
        causeChain(error).contains("not supported for REFRESH INDEX"),
        "Expected a rejection naming the option, got: " + causeChain(error));
  }

  private void createTable() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
  }

  /** Appends one fragment holding ids in [startInclusive, endExclusive). */
  private void appendFragment(int startInclusive, int endExclusive) {
    List<Row> rows =
        IntStream.range(startInclusive, endExclusive)
            .boxed()
            .map(i -> RowFactory.create(i, String.format("text_%d", i)))
            .collect(Collectors.toList());
    try {
      // coalesce(1) keeps each append to a single fragment, so fragment counts stay predictable.
      spark.createDataFrame(rows, SCHEMA).coalesce(1).writeTo(fullTable).append();
    } catch (NoSuchTableException e) {
      throw new IllegalStateException("Test table was not created: " + fullTable, e);
    }
  }

  private void createZonemapIndex() {
    spark.sql(String.format("alter table %s create index idx_id using zonemap (id)", fullTable));
  }

  private Row refresh(String withClause) {
    return spark
        .sql(String.format("alter table %s refresh index idx_id %s", fullTable, withClause))
        .collectAsList()
        .get(0);
  }

  /** Fragment ids covered by every segment of the test index. */
  private Set<Integer> coverage() {
    Set<Integer> covered = new HashSet<>();
    for (Index segment : segments()) {
      covered.addAll(segment.fragments().orElse(Collections.emptyList()));
    }
    return covered;
  }

  private Set<UUID> segmentUuids() {
    return segments().stream().map(Index::uuid).collect(Collectors.toSet());
  }

  private List<Index> segments() {
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      return dataset.getIndexes().stream()
          .filter(index -> "idx_id".equals(index.name()))
          .collect(Collectors.toList());
    }
  }

  private long datasetVersion() {
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      return dataset.version();
    }
  }

  private static String causeChain(Throwable error) {
    StringBuilder messages = new StringBuilder();
    for (Throwable current = error; current != null; current = current.getCause()) {
      messages.append(current.getMessage()).append(" | ");
    }
    return messages.toString();
  }
}
