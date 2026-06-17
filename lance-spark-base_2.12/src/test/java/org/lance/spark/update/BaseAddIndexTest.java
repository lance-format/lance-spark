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
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.index.IndexType;
import org.lance.index.OptimizeOptions;
import org.lance.spark.utils.FieldPathUtils;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Base test for distributed CREATE INDEX. */
public abstract class BaseAddIndexTest {
  protected String catalogName = "lance_test";
  protected String tableName = "create_index_test";
  protected String fullTable = catalogName + ".default." + tableName;

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
            .appName("lance-create-index-test")
            .master("local[10]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "create_index_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
    this.tableDir =
        FileSystems.getDefault().getPath(testRoot, this.tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  private void prepareDataset() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    // First insert to create initial fragments
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
    // Second insert to ensure multiple fragments
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  private void prepareNestedDataset() {
    spark.sql(
        String.format(
            "create table %s ("
                + "id int, "
                + "left_payload struct<value:int>, "
                + "right_payload struct<value:int>, "
                + "dot_payload struct<`literal.dot`:int>, "
                + "special_payload struct<`user-id`:int, `display name`:int>"
                + ") using lance;",
            fullTable));
    spark.sql(
        String.format(
            "insert into %s values "
                + "(1, named_struct('value', 10), named_struct('value', 100), "
                + "named_struct('literal.dot', 1000), "
                + "named_struct('user-id', 10000, 'display name', 10001)), "
                + "(2, named_struct('value', 20), named_struct('value', 200), "
                + "named_struct('literal.dot', 2000), "
                + "named_struct('user-id', 20000, 'display name', 20001))",
            fullTable));
  }

  @Test
  public void testCreateIndexDistributed() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format("alter table %s create index test_index using btree (id)", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index", indexName);

    // Verify query using the indexed field
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=5", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(5, r.getInt(0));
    Assertions.assertEquals("text_5", r.getString(1));

    // Check index is created successfully
    checkIndex("test_index");
  }

  @Test
  public void testRepeatedCreateIndex() {
    prepareDataset();

    Dataset<Row> result1 =
        spark.sql(
            String.format(
                "alter table %s create index test_index_repeat using btree (id)", fullTable));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result1.schema().toString());
    Row row1 = result1.collectAsList().get(0);
    long fragmentsIndexed1 = row1.getLong(0);
    String indexName1 = row1.getString(1);
    Assertions.assertTrue(fragmentsIndexed1 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_repeat", indexName1);

    // Check index is created successfully
    checkIndex("test_index_repeat");

    Dataset<Row> result2 =
        spark.sql(
            String.format(
                "alter table %s create index test_index_repeat using btree (id)", fullTable));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result2.schema().toString());
    Row row2 = result2.collectAsList().get(0);
    long fragmentsIndexed2 = row2.getLong(0);
    String indexName2 = row2.getString(1);
    Assertions.assertTrue(fragmentsIndexed2 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_repeat", indexName2);

    // Check index is created successfully
    checkIndex("test_index_repeat");
  }

  @Test
  public void testCreateBTreeIndexWithZoneSize() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_btree_param using btree (id) with (zone_size=2048)",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_btree_param", indexName);

    checkIndex("test_index_btree_param");

    // Verify query using the indexed field with zone_size parameter
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=15", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(15, r.getInt(0));
    Assertions.assertEquals("text_15", r.getString(1));
  }

  @Test
  public void testCreateZonemapIndex() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_zonemap using zonemap (id) with (rows_per_zone=4)",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row2 = result.collectAsList().get(0);
    long fragmentsIndexed2 = row2.getLong(0);
    String indexName2 = row2.getString(1);

    Assertions.assertTrue(fragmentsIndexed2 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_zonemap", indexName2);

    checkIndex("test_index_zonemap");

    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = lanceDataset.getFragments().size();
      int expectedSegmentCount = Math.min(fragmentCount, spark.sparkContext().defaultParallelism());
      List<Index> zonemapSegments =
          lanceDataset.getIndexes().stream()
              .filter(index -> "test_index_zonemap".equals(index.name()))
              .collect(Collectors.toList());
      int coveredFragments =
          zonemapSegments.stream()
              .map(index -> index.fragments().orElse(Collections.emptyList()).size())
              .mapToInt(Integer::intValue)
              .sum();

      Assertions.assertEquals(
          expectedSegmentCount,
          zonemapSegments.size(),
          "Expected distributed zonemap build to batch fragments into bounded segment count");
      Assertions.assertTrue(
          zonemapSegments.stream()
              .allMatch(
                  index -> index.fragments().isPresent() && !index.fragments().get().isEmpty()),
          "Expected each zonemap segment to cover at least one fragment");
      Assertions.assertEquals(
          fragmentCount,
          coveredFragments,
          "Expected zonemap segments to cover all fragments exactly once");
    } finally {
      lanceDataset.close();
    }

    Dataset<Row> query2 = spark.sql(String.format("select * from %s where id=15", fullTable));
    Assertions.assertEquals(1L, query2.count());
    Row r2 = query2.collectAsList().get(0);
    Assertions.assertEquals(15, r2.getInt(0));
    Assertions.assertEquals("text_15", r2.getString(1));
  }

  @Test
  public void testCreateZonemapIndexWithNumSegments() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_zonemap_segments using zonemap (id) with (num_segments = 3)",
                fullTable));

    Row row2 = result.collectAsList().get(0);
    long fragmentsIndexed2 = row2.getLong(0);
    String indexName2 = row2.getString(1);

    Assertions.assertTrue(fragmentsIndexed2 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_zonemap_segments", indexName2);

    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = lanceDataset.getFragments().size();
      int expectedSegmentCount = Math.min(fragmentCount, 3);
      List<Index> segments =
          lanceDataset.getIndexes().stream()
              .filter(index -> "test_index_zonemap_segments".equals(index.name()))
              .collect(Collectors.toList());

      Assertions.assertEquals(
          expectedSegmentCount,
          segments.size(),
          "Expected num_segments=3 to produce exactly 3 segments (or fewer if fragment count < 3)");

      int coveredFragments =
          segments.stream()
              .map(index -> index.fragments().orElse(Collections.emptyList()).size())
              .mapToInt(Integer::intValue)
              .sum();
      Assertions.assertEquals(
          fragmentCount,
          coveredFragments,
          "Expected committed segments to cover all fragments exactly once");
    } finally {
      lanceDataset.close();
    }
  }

  @Test
  public void testRepeatedCreateZonemapIndexReplacesExistingSegments() {
    prepareDataset();

    String sql =
        String.format(
            "alter table %s create index test_index_zonemap_repeat using zonemap (id) with (rows_per_zone=4)",
            fullTable);

    spark.sql(sql);

    // Capture segment UUIDs after first run
    org.lance.Dataset ds1 = org.lance.Dataset.open().uri(tableDir).build();
    Set<UUID> firstRunUuids;
    try {
      firstRunUuids =
          ds1.getIndexes().stream()
              .filter(index -> "test_index_zonemap_repeat".equals(index.name()))
              .map(Index::uuid)
              .collect(Collectors.toSet());
    } finally {
      ds1.close();
    }
    Assertions.assertFalse(firstRunUuids.isEmpty(), "First run should produce segments");

    spark.sql(sql);

    // Verify segments were replaced (new UUIDs), not duplicated
    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = lanceDataset.getFragments().size();
      int expectedSegmentCount = Math.min(fragmentCount, spark.sparkContext().defaultParallelism());
      List<Index> zonemapSegments =
          lanceDataset.getIndexes().stream()
              .filter(index -> "test_index_zonemap_repeat".equals(index.name()))
              .collect(Collectors.toList());
      Set<UUID> secondRunUuids =
          zonemapSegments.stream().map(Index::uuid).collect(Collectors.toSet());
      int coveredFragments =
          zonemapSegments.stream()
              .map(index -> index.fragments().orElse(Collections.emptyList()).size())
              .mapToInt(Integer::intValue)
              .sum();

      Assertions.assertEquals(
          expectedSegmentCount,
          zonemapSegments.size(),
          "Expected recreated zonemap index to replace existing batched segments instead of duplicating them");
      Assertions.assertEquals(
          fragmentCount,
          coveredFragments,
          "Expected recreated zonemap segments to cover all fragments exactly once");
      Assertions.assertTrue(
          Collections.disjoint(firstRunUuids, secondRunUuids),
          "Expected second run to produce fresh segment UUIDs, not reuse first run's");
    } finally {
      lanceDataset.close();
    }
  }

  @Test
  public void testZonemapRejectsMultipleColumns() {
    prepareDataset();
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark.sql(
                String.format(
                    "alter table %s create index idx_multi using zonemap (id, text)", fullTable)));
  }

  /**
   * Deferred (train=false) ZONEMAP commits a single empty driver-side index (not distributed
   * segments); optimizeIndices then populates it.
   */
  @Test
  public void testCreateZonemapIndexDeferred() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_zonemap_deferred using zonemap (id) "
                    + "with (train=false, rows_per_zone=4)",
                fullTable));

    // Deferred create processes no fragments.
    Row row = result.collectAsList().get(0);
    Assertions.assertEquals(0L, row.getLong(0), "Deferred create should index zero fragments");
    Assertions.assertEquals("test_index_zonemap_deferred", row.getString(1));

    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = lanceDataset.getFragments().size();
      Assertions.assertTrue(fragmentCount >= 2, "Expected multiple fragments");

      List<Index> deferred =
          lanceDataset.getIndexes().stream()
              .filter(index -> "test_index_zonemap_deferred".equals(index.name()))
              .collect(Collectors.toList());

      // A single empty index is committed on the driver (not distributed segments).
      Assertions.assertEquals(
          1, deferred.size(), "Deferred zonemap should commit a single empty index");
      Assertions.assertEquals(IndexType.ZONEMAP, deferred.get(0).indexType());
      Assertions.assertEquals(
          0,
          deferred.get(0).fragments().orElse(Collections.emptyList()).size(),
          "Deferred zonemap should cover no fragments before OPTIMIZE");

      // Query still returns correct results: the predicate falls back to a full scan
      // because the index covers nothing yet.
      Dataset<Row> beforeOptimize =
          spark.sql(String.format("select * from %s where id=15", fullTable));
      Assertions.assertEquals(1L, beforeOptimize.count());
      Assertions.assertEquals("text_15", beforeOptimize.collectAsList().get(0).getString(1));

      // Populate the deferred index from the unindexed fragments.
      lanceDataset.optimizeIndices(OptimizeOptions.builder().build());
    } finally {
      lanceDataset.close();
    }

    org.lance.Dataset optimized = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = optimized.getFragments().size();
      List<Index> populated =
          optimized.getIndexes().stream()
              .filter(index -> "test_index_zonemap_deferred".equals(index.name()))
              .collect(Collectors.toList());
      int coveredFragments =
          populated.stream()
              .map(index -> index.fragments().orElse(Collections.emptyList()).size())
              .mapToInt(Integer::intValue)
              .sum();
      Assertions.assertEquals(
          fragmentCount,
          coveredFragments,
          "Expected OPTIMIZE to populate the deferred zonemap over all fragments");
    } finally {
      optimized.close();
    }

    // Query remains correct after the index is populated.
    Dataset<Row> afterOptimize =
        spark.sql(String.format("select * from %s where id=15", fullTable));
    Assertions.assertEquals(1L, afterOptimize.count());
    Assertions.assertEquals("text_15", afterOptimize.collectAsList().get(0).getString(1));
  }

  /**
   * A deferred ZONEMAP can be populated by re-running CREATE INDEX (eager): the distributed segment
   * build replaces the empty index and covers all fragments.
   */
  @Test
  public void testDeferredZonemapPopulatedByEagerRecreate() {
    prepareDataset();

    spark.sql(
        String.format(
            "alter table %s create index idx_dz using zonemap (id) with (train=false)", fullTable));

    // Re-run eagerly with the same name: distributed build over all fragments.
    spark.sql(String.format("alter table %s create index idx_dz using zonemap (id)", fullTable));

    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = lanceDataset.getFragments().size();
      List<Index> segments =
          lanceDataset.getIndexes().stream()
              .filter(index -> "idx_dz".equals(index.name()))
              .collect(Collectors.toList());

      // Distributed build emits more than one segment, and the empty deferred index is gone.
      Assertions.assertTrue(
          segments.size() > 1,
          "Expected eager recreate to build distributed segments, got " + segments.size());
      Assertions.assertTrue(
          segments.stream().allMatch(s -> !s.fragments().orElse(Collections.emptyList()).isEmpty()),
          "Expected no empty (deferred) segment to survive the eager recreate");
      int coveredFragments =
          segments.stream()
              .map(index -> index.fragments().orElse(Collections.emptyList()).size())
              .mapToInt(Integer::intValue)
              .sum();
      Assertions.assertEquals(
          fragmentCount, coveredFragments, "Expected recreated zonemap to cover all fragments");
    } finally {
      lanceDataset.close();
    }
  }

  /** num_segments is meaningless for a deferred build (no segmented build happens). */
  @Test
  public void testZonemapDeferredRejectsNumSegments() {
    prepareDataset();
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark.sql(
                String.format(
                    "alter table %s create index idx_defer_seg using zonemap (id) "
                        + "with (train=false, num_segments=3)",
                    fullTable)));
  }

  @Test
  public void testNumSegmentsRejectedForBtree() {
    prepareDataset();
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark.sql(
                String.format(
                    "alter table %s create index idx_btree_seg using btree (id) with (num_segments=3)",
                    fullTable)));
  }

  @Test
  public void testNumSegmentsRejectsZeroAndNegative() {
    prepareDataset();
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark.sql(
                String.format(
                    "alter table %s create index idx_zero using zonemap (id) with (num_segments=0)",
                    fullTable)));
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark.sql(
                String.format(
                    "alter table %s create index idx_neg using zonemap (id) with (num_segments=-1)",
                    fullTable)));
  }

  @Test
  public void testCreateBTreeIndexWithRangeMode() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_btree_param using btree (id) with (zone_size=2048, build_mode='range')",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_btree_param", indexName);

    checkIndex("test_index_btree_param");

    // Verify query using the indexed field with zone_size parameter
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=15", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(15, r.getInt(0));
    Assertions.assertEquals("text_15", r.getString(1));
  }

  @Test
  public void testRepeatedCreateBTreeRangeIndex() {
    prepareDataset();

    String sql =
        String.format(
            "alter table %s create index test_range_repeat using btree (id) with (build_mode='range')",
            fullTable);

    spark.sql(sql);
    checkIndex("test_range_repeat");

    int fragmentCount;
    Set<UUID> firstRunUuids;
    org.lance.Dataset ds1 = org.lance.Dataset.open().uri(tableDir).build();
    try {
      fragmentCount = ds1.getFragments().size();
      firstRunUuids =
          ds1.getIndexes().stream()
              .filter(index -> "test_range_repeat".equals(index.name()))
              .map(Index::uuid)
              .collect(Collectors.toSet());
    } finally {
      ds1.close();
    }
    Assertions.assertEquals(
        fragmentCount,
        firstRunUuids.size(),
        "Expected one disjoint range segment per fragment on first create");

    // Re-create with the same name: exercises replace(false) on the segment builds plus
    // atomic replacement at commit time. The old segments must be replaced, not duplicated.
    spark.sql(sql);
    checkIndex("test_range_repeat");

    org.lance.Dataset ds2 = org.lance.Dataset.open().uri(tableDir).build();
    try {
      List<Index> segments =
          ds2.getIndexes().stream()
              .filter(index -> "test_range_repeat".equals(index.name()))
              .collect(Collectors.toList());
      Set<UUID> secondRunUuids = segments.stream().map(Index::uuid).collect(Collectors.toSet());
      Assertions.assertEquals(
          fragmentCount,
          segments.size(),
          "Expected recreated range index to replace existing segments instead of duplicating them");
      Assertions.assertTrue(
          Collections.disjoint(firstRunUuids, secondRunUuids),
          "Expected the recreated range index to produce fresh segment UUIDs");
    } finally {
      ds2.close();
    }

    // Index must remain queryable after the replacement.
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=15", fullTable));
    Assertions.assertEquals(1L, query.count());
    Assertions.assertEquals("text_15", query.collectAsList().get(0).getString(1));
  }

  @Test
  public void testCreateBTreeIndexWithRowsPerRange() {
    prepareDataset();
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_btree_param using btree (id) "
                    + "with (zone_size=2048, build_mode='range', rows_per_range=2)",
                fullTable));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());
    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);
    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_btree_param", indexName);
    checkIndex("test_index_btree_param");
    // Verify query using the indexed field with zone_size parameter
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=15", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(15, r.getInt(0));
    Assertions.assertEquals("text_15", r.getString(1));
  }

  @Test
  public void testCreateBTreeIndexWithFragmentMode() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_index_btree_fragment using btree (id) with (build_mode='fragment')",
                fullTable));

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_index_btree_fragment", indexName);

    checkIndex("test_index_btree_fragment");
  }

  @Test
  public void testCreateBTreeIndexOnNestedFieldsUsesLeafFieldIds() {
    prepareNestedDataset();

    spark.sql(
        String.format(
            "alter table %s create index idx_left_value using btree (left_payload.value)",
            fullTable));
    spark.sql(
        String.format(
            "alter table %s create index idx_right_value using btree (right_payload.value)",
            fullTable));

    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int leftFieldId = fieldId(lanceDataset, "left_payload.value");
      int rightFieldId = fieldId(lanceDataset, "right_payload.value");

      Assertions.assertEquals(
          Collections.singletonList(leftFieldId), checkIndex("idx_left_value").fields());
      Assertions.assertEquals(
          Collections.singletonList(rightFieldId), checkIndex("idx_right_value").fields());
      Assertions.assertNotEquals(
          leftFieldId,
          rightFieldId,
          "Same leaf names under different parents must resolve to different field ids");
    } finally {
      lanceDataset.close();
    }
  }

  @Test
  public void testCreateBTreeIndexOnNestedLiteralDotFieldShowsCanonicalPath() {
    prepareNestedDataset();

    spark.sql(
        String.format(
            "alter table %s create index idx_literal_dot using btree (dot_payload.`literal.dot`)",
            fullTable));

    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int literalDotFieldId = fieldId(lanceDataset, "dot_payload.`literal.dot`");
      Assertions.assertEquals(
          Collections.singletonList(literalDotFieldId), checkIndex("idx_literal_dot").fields());
    } finally {
      lanceDataset.close();
    }

    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Row row =
        rows.stream()
            .filter(r -> "idx_literal_dot".equals(r.getString(0)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("SHOW INDEXES did not return idx_literal_dot"));
    Assertions.assertEquals(Collections.singletonList("dot_payload.`literal.dot`"), row.getList(1));
  }

  @Test
  public void testCreateBTreeIndexOnNestedSpecialCharacterFieldShowsCanonicalPath() {
    prepareNestedDataset();

    spark.sql(
        String.format(
            "alter table %s create index idx_user_id using btree (special_payload.`user-id`)",
            fullTable));

    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int userIdFieldId = fieldId(lanceDataset, "special_payload.`user-id`");
      Assertions.assertEquals(
          Collections.singletonList(userIdFieldId), checkIndex("idx_user_id").fields());
    } finally {
      lanceDataset.close();
    }

    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Row row =
        rows.stream()
            .filter(r -> "idx_user_id".equals(r.getString(0)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("SHOW INDEXES did not return idx_user_id"));
    Assertions.assertEquals(Collections.singletonList("special_payload.`user-id`"), row.getList(1));
  }

  @Test
  public void testCreateBTreeIndexWithUnrecognizedBuildMode() {
    prepareDataset();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "alter table %s create index test_index_bad_mode using btree (id) with (build_mode='invalid')",
                            fullTable))
                    .collect());

    Assertions.assertTrue(
        exception.getMessage().contains("Unrecognized build_mode"),
        "Expected error message to mention unrecognized build_mode, got: "
            + exception.getMessage());
  }

  @Test
  public void testCreateIndexFailureLeavesTableUsable() {
    prepareDataset();

    // An FTS index on a non-text column (id is int) resolves the column fine but fails inside
    // AddIndexExec.run()'s FTS path, after the driver-side dataset is opened — the path the
    // close-on-failure fix guards. We assert the publicly observable contract: the failed index
    // is not committed, the table stays queryable, and a later valid CREATE INDEX succeeds. (On
    // POSIX a leaked read handle would not surface, so this guards reusability/cleanliness, not
    // handle-closure directly.)
    Exception failure =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "alter table %s create index bad_idx using fts (id)", fullTable))
                    .collect());

    // The failure must come from the distributed index-build job, which runs only after the
    // driver-side dataset is opened. A SparkException in the cause chain indicates a job failure;
    // earlier failures (analysis, driver-side validation) surface unwrapped and would mean this
    // test no longer exercises the close-on-failure path it exists to guard.
    boolean fromBuildJob = false;
    for (Throwable t = failure; t != null && !fromBuildJob; t = t.getCause()) {
      fromBuildJob = t instanceof SparkException;
    }
    Assertions.assertTrue(
        fromBuildJob, "Expected a build-job failure after dataset open, got: " + failure);

    // The table is still fully queryable after the failed CREATE INDEX. collectAsList (not
    // count, which is answered from manifest metadata) forces a real scan of the data pages.
    Dataset<Row> all = spark.sql(String.format("select * from %s", fullTable));
    Assertions.assertEquals(20, all.collectAsList().size());

    // Nothing was committed to the manifest: the table should have no index at all. (A check
    // for the failed name alone would miss unnamed segment entries.)
    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      Assertions.assertTrue(
          lanceDataset.getIndexes().isEmpty(),
          "Failed CREATE INDEX should not commit any index to the manifest, found: "
              + lanceDataset.getIndexes());
    } finally {
      lanceDataset.close();
    }

    // The dataset is reusable: a subsequent valid CREATE INDEX still succeeds.
    spark.sql(String.format("alter table %s create index good_idx using btree (id)", fullTable));
    checkIndex("good_idx");
  }

  @Test
  public void testCreateFtsIndex() {
    prepareDataset();

    // FTS requires all InvertedIndexDetails fields to be specified
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_index using fts (text) with ("
                    + "base_tokenizer='simple', "
                    + "language='English', "
                    + "max_token_length=40, "
                    + "lower_case=true, "
                    + "stem=false, "
                    + "remove_stop_words=false, "
                    + "ascii_folding=false, "
                    + "with_position=true"
                    + ")",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    // Verify distributed execution across multiple fragments
    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_index", indexName);

    // Check index is created successfully
    checkFtsIndex("test_fts_index");

    // Verify query using the text column
    Dataset<Row> query =
        spark.sql(String.format("select * from %s where text='text_5'", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(5, r.getInt(0));
    Assertions.assertEquals("text_5", r.getString(1));
  }

  @Test
  public void testCreateFtsIndexWithStemming() {
    prepareDataset();

    // Test with stemming enabled
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_stem using fts (text) with ("
                    + "base_tokenizer='simple', "
                    + "language='English', "
                    + "max_token_length=40, "
                    + "lower_case=true, "
                    + "stem=true, "
                    + "remove_stop_words=false, "
                    + "ascii_folding=false, "
                    + "with_position=true"
                    + ")",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_stem", indexName);

    checkFtsIndex("test_fts_stem");
  }

  @Test
  public void testRepeatedCreateFtsIndex() {
    prepareDataset();

    String ftsOptions =
        "base_tokenizer='simple', "
            + "language='English', "
            + "max_token_length=40, "
            + "lower_case=true, "
            + "stem=false, "
            + "remove_stop_words=false, "
            + "ascii_folding=false, "
            + "with_position=true";

    // First FTS index creation
    Dataset<Row> result1 =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_repeat using fts (text) with (%s)",
                fullTable, ftsOptions));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result1.schema().toString());
    Row row1 = result1.collectAsList().get(0);
    long fragmentsIndexed1 = row1.getLong(0);
    String indexName1 = row1.getString(1);
    Assertions.assertTrue(fragmentsIndexed1 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_repeat", indexName1);

    // Check index is created successfully
    checkFtsIndex("test_fts_repeat");

    // Second FTS index creation with same name (should replace)
    Dataset<Row> result2 =
        spark.sql(
            String.format(
                "alter table %s create index test_fts_repeat using fts (text) with (%s)",
                fullTable, ftsOptions));
    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result2.schema().toString());
    Row row2 = result2.collectAsList().get(0);
    long fragmentsIndexed2 = row2.getLong(0);
    String indexName2 = row2.getString(1);
    Assertions.assertTrue(fragmentsIndexed2 >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_fts_repeat", indexName2);

    // Check index still exists after replacement
    checkFtsIndex("test_fts_repeat");
  }

  @Test
  public void testDropIndex() {
    prepareDataset();

    // Create an index first
    spark.sql(
        String.format("alter table %s create index test_drop_idx using btree (id)", fullTable));
    checkIndex("test_drop_idx");

    // Drop the index
    Dataset<Row> result =
        spark.sql(String.format("alter table %s drop index test_drop_idx", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(index_name,StringType,true),StructField(status,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    Assertions.assertEquals("test_drop_idx", row.getString(0));
    Assertions.assertEquals("dropped", row.getString(1));

    // Verify index no longer exists
    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      List<Index> indexList = lanceDataset.getIndexes();
      Set<String> indexNames = indexList.stream().map(Index::name).collect(Collectors.toSet());
      Assertions.assertFalse(
          indexNames.contains("test_drop_idx"), "Index should have been dropped");
    } finally {
      lanceDataset.close();
    }
  }

  @Test
  public void testDropIndexThenRecreate() {
    prepareDataset();

    // Create, drop, then recreate
    spark.sql(
        String.format("alter table %s create index test_recreate_idx using btree (id)", fullTable));
    checkIndex("test_recreate_idx");

    spark.sql(String.format("alter table %s drop index test_recreate_idx", fullTable));

    spark.sql(
        String.format("alter table %s create index test_recreate_idx using btree (id)", fullTable));
    checkIndex("test_recreate_idx");

    // Verify query still works
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=5", fullTable));
    Assertions.assertEquals(1L, query.count());
  }

  @Test
  public void testBTreeIndexHasIndexDetails() {
    prepareDataset();
    spark.sql(
        String.format("alter table %s create index idx_details_btree using btree (id)", fullTable));
    verifyIndexDetails("idx_details_btree", "BTREE");
  }

  @Test
  public void testRangeBTreeIndexHasIndexDetails() {
    prepareDataset();
    spark.sql(
        String.format(
            "alter table %s create index idx_details_range using btree (id) with (build_mode='range')",
            fullTable));
    verifyIndexDetails("idx_details_range", "BTREE");
  }

  @Test
  public void testFtsIndexHasIndexDetails() {
    prepareDataset();
    spark.sql(
        String.format(
            "alter table %s create index idx_details_fts using fts (text) with ("
                + "base_tokenizer='simple', "
                + "language='English', "
                + "max_token_length=40, "
                + "lower_case=true, "
                + "stem=false, "
                + "remove_stop_words=false, "
                + "ascii_folding=false, "
                + "with_position=true"
                + ")",
            fullTable));
    verifyIndexDetails("idx_details_fts", "INVERTED");
  }

  /** Checks index_details is populated and both describeIndices overloads work. */
  private void verifyIndexDetails(String indexName, String expectedIndexType) {
    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      List<Index> indexList = lanceDataset.getIndexes();
      Index index =
          indexList.stream()
              .filter(i -> indexName.equals(i.name()))
              .findFirst()
              .orElseThrow(
                  () -> new AssertionError("Index '" + indexName + "' not found in dataset"));
      Assertions.assertTrue(
          index.indexDetails().isPresent(),
          "index_details should be populated for index '" + indexName + "'");
      Assertions.assertTrue(
          index.indexDetails().get().length > 0,
          "index_details should not be empty for index '" + indexName + "'");
      Assertions.assertEquals(
          IndexType.valueOf(expectedIndexType.toUpperCase()),
          index.indexType(),
          "Index type mismatch for '" + indexName + "'");
      if (index.indexType() == IndexType.INVERTED) {
        Assertions.assertTrue(index.indexVersion() > 0, "FTS index version should be positive");
        if ("2".equals(System.getenv("LANCE_FTS_FORMAT_VERSION"))) {
          Assertions.assertEquals(2, index.indexVersion());
        }
      }

      // criteria-based overload
      IndexCriteria criteria = new IndexCriteria.Builder().build();
      List<IndexDescription> descriptions = lanceDataset.describeIndices(criteria);
      Assertions.assertFalse(
          descriptions.isEmpty(), "describeIndices(criteria) should return at least one index");
      IndexDescription desc =
          descriptions.stream()
              .filter(d -> indexName.equals(d.getName()))
              .findFirst()
              .orElseThrow(
                  () -> new AssertionError("Index description for '" + indexName + "' not found"));
      Assertions.assertEquals(
          expectedIndexType.toUpperCase(),
          desc.getIndexType().toUpperCase(),
          "Index type mismatch for '" + indexName + "'");

      // no-arg overload
      List<IndexDescription> noArgDescriptions = lanceDataset.describeIndices();
      Assertions.assertFalse(
          noArgDescriptions.isEmpty(), "describeIndices() no-arg should succeed");
      Assertions.assertTrue(
          noArgDescriptions.stream().anyMatch(d -> indexName.equals(d.getName())),
          "describeIndices() no-arg should contain index '" + indexName + "'");
    } finally {
      lanceDataset.close();
    }
  }

  private Index checkIndex(String indexName) {
    // Check index is created successfully
    org.lance.Dataset lanceDataset = org.lance.Dataset.open().uri(tableDir).build();
    try {
      List<Index> indexList = lanceDataset.getIndexes();
      Assertions.assertTrue(indexList.size() >= 1);
      Set<String> indexNames = indexList.stream().map(Index::name).collect(Collectors.toSet());
      Assertions.assertTrue(indexNames.contains(indexName));
      Index index =
          indexList.stream()
              .filter(i -> indexName.equals(i.name()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("Index not found: " + indexName));
      Assertions.assertTrue(index.indexDetails().isPresent(), "Index details should be present");
      Assertions.assertTrue(
          index.indexDetails().get().length > 0, "Index details should not be empty");
      return index;
    } finally {
      lanceDataset.close();
    }
  }

  private void checkFtsIndex(String indexName) {
    Index index = checkIndex(indexName);
    Assertions.assertEquals(IndexType.INVERTED, index.indexType());
    Assertions.assertTrue(index.indexVersion() > 0, "FTS index version should be positive");
    if ("2".equals(System.getenv("LANCE_FTS_FORMAT_VERSION"))) {
      Assertions.assertEquals(2, index.indexVersion());
    }
  }

  private int fieldId(org.lance.Dataset dataset, String path) {
    return FieldPathUtils.resolveLeafField(dataset.getLanceSchema(), path).getId();
  }
}
