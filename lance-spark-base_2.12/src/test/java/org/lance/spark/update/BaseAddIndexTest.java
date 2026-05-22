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
import org.lance.index.scalar.ZoneStats;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Base test for distributed CREATE INDEX. */
public abstract class BaseAddIndexTest {
  private static final String ID_COLUMN = "id";

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

  // ----- USING zonemap method ---------------------------------------------

  @Test
  public void testCreateZonemapIndex() {
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index test_idx_zonemap using zonemap (id)", fullTable));

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);
    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("test_idx_zonemap", indexName);

    Index idx = checkIndex("test_idx_zonemap");
    Assertions.assertEquals(IndexType.ZONEMAP, idx.indexType());

    // Strict per-fragment coverage: every indexed fragment must contribute
    // at least one zone in getZonemapStats. A miss would mean the build path
    // raced on a shared zonemap.lance and only one fragment's data survived.
    Set<Integer> fragmentIdsWithStats =
        zonemapStats(ID_COLUMN).stream().map(ZoneStats::getFragmentId).collect(Collectors.toSet());
    Assertions.assertEquals(
        (int) fragmentsIndexed,
        fragmentIdsWithStats.size(),
        "Every indexed fragment must have at least one zone in getZonemapStats");
  }

  @Test
  public void testCreateZonemapOnNonExistentColumn() {
    prepareDataset();
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "alter table %s create index idx_missing using zonemap (does_not_exist)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        ex.getMessage().contains("Cannot find index column"),
        "Expected error message to mention missing column, got: " + ex.getMessage());
  }

  @Test
  public void testRepeatedCreateZonemap() {
    // Companion to testRepeatedCreateIndex (BTree). Re-running CREATE INDEX with the same
    // name must produce one logical index — the second invocation's `removedIndices` filter
    // is supposed to clean up the first run's per-fragment segments before the new commit
    // lands. A regression where removal didn't fire would produce 2 × N segments and the
    // getZonemapStats listing would silently double-count.
    prepareDataset();

    // First run.
    Dataset<Row> result1 =
        spark.sql(
            String.format(
                "alter table %s create index idx_zm_repeat using zonemap (id)", fullTable));
    long fragmentsIndexed1 = result1.collectAsList().get(0).getLong(0);
    Assertions.assertTrue(
        fragmentsIndexed1 >= 2, "First run: expected at least 2 fragments indexed");

    List<Index> segmentsAfterFirst = indexesByName("idx_zm_repeat");
    Assertions.assertEquals(
        (int) fragmentsIndexed1,
        segmentsAfterFirst.size(),
        "First run: one segment per indexed fragment");

    // Second run with same name — must overwrite, not accumulate.
    Dataset<Row> result2 =
        spark.sql(
            String.format(
                "alter table %s create index idx_zm_repeat using zonemap (id)", fullTable));
    long fragmentsIndexed2 = result2.collectAsList().get(0).getLong(0);
    Assertions.assertEquals(
        fragmentsIndexed1, fragmentsIndexed2, "Second run must index the same number of fragments");

    List<Index> segmentsAfterSecond = indexesByName("idx_zm_repeat");
    Assertions.assertEquals(
        (int) fragmentsIndexed2,
        segmentsAfterSecond.size(),
        "Second run must NOT accumulate segments — old run's segments must be cleared by "
            + "removedIndices. Got "
            + segmentsAfterSecond.size()
            + " segments after replace, expected "
            + fragmentsIndexed2);

    // After replace, the segment set should have entirely new UUIDs — the old segments are
    // gone and the new ones came from this run's fresh per-task createIndex calls.
    Set<UUID> firstRunUuids =
        segmentsAfterFirst.stream().map(Index::uuid).collect(Collectors.toSet());
    Set<UUID> secondRunUuids =
        segmentsAfterSecond.stream().map(Index::uuid).collect(Collectors.toSet());
    Assertions.assertTrue(
        java.util.Collections.disjoint(firstRunUuids, secondRunUuids),
        "Re-created segments must have entirely new UUIDs; got overlap between "
            + firstRunUuids
            + " and "
            + secondRunUuids);
  }

  @Test
  public void testCreateZonemapOnStringColumn() {
    prepareDataset();
    spark
        .sql(
            String.format(
                "alter table %s create index idx_text_zonemap using zonemap (text)", fullTable))
        .collect();
    Index idx = checkIndex("idx_text_zonemap");
    Assertions.assertEquals(IndexType.ZONEMAP, idx.indexType());

    // Verify the string codec actually round-trips: every zone's min/max must be a non-null
    // String. A bug in the codec would give us null bounds or non-String types here.
    List<ZoneStats> stats = zonemapStats("text");
    Assertions.assertFalse(stats.isEmpty(), "Zonemap stats should be present for string column");
    for (ZoneStats z : stats) {
      Assertions.assertNotNull(z.getMin(), "Zone min for string column should be non-null");
      Assertions.assertNotNull(z.getMax(), "Zone max for string column should be non-null");
      Assertions.assertTrue(
          z.getMin() instanceof String, "Zone min should be String, got " + z.getMin().getClass());
      Assertions.assertTrue(
          z.getMax() instanceof String, "Zone max should be String, got " + z.getMax().getClass());
    }
  }

  @Test
  public void testZonemapDistributedCommitShape() {
    // Locks the multi-segment commit invariants that the distributed-build implementation
    // depends on: one IndexMetadata per fragment, every segment has a distinct UUID, the
    // segment fragment-bitmaps cover exactly the indexed fragment set. Regressions to a
    // shared-UUID single-segment shape (the old race) would fail every one of these.
    prepareDataset();
    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index idx_zm_shape using zonemap (id)", fullTable));
    long fragmentsIndexed = result.collectAsList().get(0).getLong(0);

    List<Index> segments = indexesByName("idx_zm_shape");
    Assertions.assertEquals(
        (int) fragmentsIndexed,
        segments.size(),
        "Expect exactly one IndexMetadata segment per indexed fragment");

    long distinctUuids = segments.stream().map(Index::uuid).distinct().count();
    Assertions.assertEquals(
        (long) segments.size(),
        distinctUuids,
        "Every segment must have a distinct UUID — the per-task UUID invariant prevents the "
            + "shared-zonemap.lance write race");

    // Discover the ground-truth set of fragment ids actually present in the dataset. The
    // build path's promise is that it indexes every existing fragment exactly once; comparing
    // against this set catches both kinds of permutation bug:
    //   (a) a task returning a fragment id NOT in the input set (e.g. some encoding artefact
    //       in the result struct silently flipped the id)
    //   (b) a task duplicating a fragment id another task already claimed
    // The previous formulation only asserted size-equality, which let (a) slip through when
    // the duplication and dropout happened to balance out.
    Set<Integer> expectedFragments = new HashSet<>();
    try (org.lance.Dataset lds = openLance()) {
      for (org.lance.Fragment f : lds.getFragments()) {
        expectedFragments.add(f.getId());
      }
    }
    Assertions.assertEquals(
        (int) fragmentsIndexed,
        expectedFragments.size(),
        "Sanity: fragmentsIndexed return value must match the dataset's current fragment count");
    // The "multi-segment" invariants this test pins are vacuously satisfied for N=1.
    // Assert the fixture produced enough fragments to exercise the distributed-commit
    // path it claims to verify — without this, a future change to prepareDataset() (e.g.
    // a Spark version that consolidates VALUES tuples into one fragment) would silently
    // turn this test into a single-segment regression check.
    Assertions.assertTrue(
        expectedFragments.size() >= 2,
        "Fixture must produce >= 2 fragments to exercise multi-segment commit invariants; "
            + "got "
            + expectedFragments.size());

    // Each segment must cover exactly one fragment, that fragment must belong to the
    // ground-truth set, and no two segments may claim the same fragment.
    Set<Integer> coveredFragments = new HashSet<>();
    Set<List<Integer>> seenFieldLists = new HashSet<>();
    Set<Integer> seenIndexVersions = new HashSet<>();
    for (Index segment : segments) {
      Assertions.assertEquals(IndexType.ZONEMAP, segment.indexType());
      Assertions.assertTrue(
          segment.fragments().isPresent(), "Every committed segment must carry a fragment list");
      List<Integer> segFragments = segment.fragments().get();
      Assertions.assertEquals(
          1,
          segFragments.size(),
          "Each segment must cover exactly one fragment, got " + segFragments);
      int segFragId = segFragments.get(0);
      Assertions.assertTrue(
          expectedFragments.contains(segFragId),
          "Segment claims fragment "
              + segFragId
              + " which is NOT in the dataset's actual "
              + "fragment set "
              + expectedFragments
              + " — task→fragment-id permutation bug?");
      Assertions.assertTrue(
          coveredFragments.add(segFragId),
          "Fragment " + segFragId + " is covered by more than one segment");

      // Every segment must point to the same field-id list — the driver computed it once and
      // populated it on every Index entry. A regression that left the field list empty, or
      // sized for the wrong number of columns, or diverging across segments would still
      // satisfy uuid/fragment/type assertions otherwise. We assert size == 1 because this
      // test indexes a single column ("id"). We do NOT assert the exact field-id value
      // because that would require reading the Lance schema's internal field-id metadata,
      // which isn't cleanly exposed through the test surface; the per-segment uniformity
      // check below catches the "wrong but uniform id" case as long as it'd be wrong
      // consistently (which is what a real regression would produce).
      Assertions.assertEquals(
          1,
          segment.fields().size(),
          "Indexing one column ('id') must produce a field-id list of length 1; got "
              + segment.fields());
      seenFieldLists.add(segment.fields());

      // Index version must be plumbed consistently from per-task createIndex results — all
      // segments under one name must report the same version. We do NOT assert
      // indexVersion > 0 because ZONEMAP_INDEX_VERSION is currently 0 (legitimate); the
      // cross-segment-uniformity check is what catches a regression where per-task plumbing
      // drifted (one task's r.indexVersion lost in serialisation, defaulted to int 0 while
      // others carried a non-zero value).
      seenIndexVersions.add(segment.indexVersion());
    }
    Assertions.assertEquals(
        1,
        seenIndexVersions.size(),
        "All segments under one name must report the same indexVersion; got " + seenIndexVersions);
    Assertions.assertEquals(
        expectedFragments,
        coveredFragments,
        "Set of segment fragment-ids must equal the dataset's actual fragment set");
    Assertions.assertEquals(
        1,
        seenFieldLists.size(),
        "All segments under the same name must share one field-id list, got " + seenFieldLists);
  }

  @Test
  public void testCreateZonemapWithZoneSize() throws Exception {
    // Verifies that `with (rows_per_zone=N)` actually reaches lance-core and changes the
    // on-disk zone layout — not just that the SQL parses. The previous smoke-only version
    // would still pass if a refactor silently dropped the parameter before
    // IndexUtils.toJson / ScalarIndexParams.create reached the JNI.
    //
    // IMPORTANT: ZONEMAP's parameter name is `rows_per_zone`, NOT `zone_size`. BTree uses
    // `zone_size` (for its own range-partitioning), and the param names are independent on
    // each side of the JSON boundary — `lance-core/scalar/zonemap.rs` only deserialises
    // `rows_per_zone`. A `with (zone_size=N)` clause on a ZONEMAP index is silently ignored
    // by lance-core's serde default-on-unknown-key behaviour.
    //
    // Strategy: build a multi-row single fragment by writing via DataFrame.coalesce(1) so the
    // entire row set lands in one fragment. Spark's SQL INSERT VALUES path with master
    // `local[10]` partitions each VALUES tuple into its own task and produces N single-row
    // fragments, which can't differentiate "rows_per_zone honored" from "rows_per_zone
    // ignored".
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 12)
        .selectExpr("cast(id as int) as id", "cast(concat('t_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();

    spark
        .sql(
            String.format(
                "alter table %s create index idx_zonemap_small_zone using zonemap (id) with (rows_per_zone=4)",
                fullTable))
        .collect();
    Index idx = checkIndex("idx_zonemap_small_zone");
    Assertions.assertEquals(IndexType.ZONEMAP, idx.indexType());

    int fragmentCount;
    int totalZoneCount;
    try (org.lance.Dataset lds = openLance()) {
      fragmentCount = lds.getFragments().size();
      totalZoneCount = lds.getZonemapStats("id").size();
    }
    // Pin fragmentCount==1 explicitly: the rows-per-zone arithmetic below assumes a single
    // 12-row fragment. If a future Spark/Lance change splits the coalesce(1) partition into
    // multiple fragments, the totalZoneCount would scale and the assertion below would still
    // pass (per-fragment count × fragmentCount could match by accident). Asserting up front
    // makes that drift loud.
    Assertions.assertEquals(
        1,
        fragmentCount,
        "Test fixture expects coalesce(1) to produce exactly one 12-row fragment");

    // With coalesce(1) + 12 rows, we get one fragment of 12 rows; rows_per_zone=4 yields
    // ceil(12/4) = 3 zones. A regression that ignored rows_per_zone and defaulted to 8192
    // would produce 1 zone.
    int expectedZonesPerFragment = (12 + 3) / 4; // ceil(12 / 4) = 3
    Assertions.assertEquals(
        fragmentCount * expectedZonesPerFragment,
        totalZoneCount,
        "with (rows_per_zone=4) on a 12-row coalesced fragment must produce 3 zones — got "
            + totalZoneCount
            + " across "
            + fragmentCount
            + " fragments");

    // Per-zone length verification: the first two zones must be full (length=4) and the
    // trailing zone gets the remaining 4 rows (also length=4 in this aligned case).
    // Without this check, a regression that produced 3 zones at the wrong sizes (e.g.
    // {1, 1, 10} from a different chunking heuristic) would still satisfy the
    // totalZoneCount == 3 assertion.
    try (org.lance.Dataset lds = openLance()) {
      List<ZoneStats> stats = lds.getZonemapStats("id");
      for (ZoneStats z : stats) {
        Assertions.assertEquals(
            4,
            z.getZoneLength(),
            "Every zone must have length == rows_per_zone=4; got "
                + z.getZoneLength()
                + " at zone_start="
                + z.getZoneStart());
      }
    }
  }

  @Test
  public void testTwoCoexistingZonemapIndexes() throws Exception {
    // Two differently-named ZONEMAP indexes on different columns of the same dataset must
    // commit with disjoint UUIDs and not interfere with each other. The per-task UUID
    // strategy in `runZonemapDistributed` should make this trivially safe (each createIndex
    // call generates its own UUID, so no shared-path race) but the test pins the cross-name
    // isolation that the read path (load_indices_by_name groups by name) depends on.
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 20)
        .selectExpr("cast(id as int) as id", "cast(concat('t_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();
    spark.sql(String.format("alter table %s create index idx_zm_id using zonemap (id)", fullTable));
    spark.sql(
        String.format("alter table %s create index idx_zm_text using zonemap (text)", fullTable));

    List<Index> idSegments = indexesByName("idx_zm_id");
    List<Index> textSegments = indexesByName("idx_zm_text");
    Assertions.assertFalse(idSegments.isEmpty(), "id index must commit");
    Assertions.assertFalse(textSegments.isEmpty(), "text index must commit");

    Set<UUID> idUuids = idSegments.stream().map(Index::uuid).collect(Collectors.toSet());
    Set<UUID> textUuids = textSegments.stream().map(Index::uuid).collect(Collectors.toSet());
    Assertions.assertTrue(
        java.util.Collections.disjoint(idUuids, textUuids),
        "Per-task UUIDs must be globally unique across differently-named indexes; got "
            + "overlap: id="
            + idUuids
            + ", text="
            + textUuids);

    // Both indexes' getZonemapStats must work after committing both — a regression where
    // index-name-grouping leaked across names would mix stats between columns.
    try (org.lance.Dataset lds = openLance()) {
      List<ZoneStats> idStats = lds.getZonemapStats("id");
      List<ZoneStats> textStats = lds.getZonemapStats("text");
      Assertions.assertFalse(idStats.isEmpty(), "id stats must be readable");
      Assertions.assertFalse(textStats.isEmpty(), "text stats must be readable");
      // Sanity: id stats must have Number min/max (Int32 column), text stats must have
      // String min/max — a cross-leak would produce mismatched types.
      for (ZoneStats z : idStats) {
        Assertions.assertTrue(
            z.getMin() instanceof Number,
            "id zone min must be Number; got "
                + (z.getMin() == null ? "null" : z.getMin().getClass()));
      }
      for (ZoneStats z : textStats) {
        Assertions.assertTrue(
            z.getMin() instanceof String,
            "text zone min must be String; got "
                + (z.getMin() == null ? "null" : z.getMin().getClass()));
      }
    }
  }

  @Test
  public void testCreateZonemapOnNullableStringColumn() throws Exception {
    // Companion to testCreateZonemapOnStringColumn: the prior test asserts non-null min/max,
    // but uses a non-null fixture. This test injects NULL values and verifies the zonemap
    // round-trips through the all-NULL-zone codec path:
    //   - Zones spanning a row range with no NULLs: non-null String min/max, nullCount=0.
    //   - Zones spanning all-NULL rows: null min/max, nullCount=zoneLength.
    // Without this test, a regression that made the string codec crash on NULLs would not
    // surface here.
    //
    // DataFrame.coalesce(1) is required because SQL INSERT VALUES under `local[10]` produces
    // one fragment per VALUES tuple, defeating the "5 NULL rows align with one zone" plan.
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    // 10 rows: rows 0..4 have text, rows 5..9 are NULL. Use rows_per_zone=5 so the zones
    // align with the NULL boundary. coalesce(1) groups them all into one fragment.
    // (ZONEMAP uses `rows_per_zone`, NOT `zone_size` — the latter is BTree's parameter
    // name. Mixing them up is the exact silent-drop bug class this test is sensitive to.)
    spark
        .range(0, 10)
        .selectExpr(
            "cast(id as int) as id",
            "case when id < 5 then cast(concat('s_', id) as string) else cast(null as string) end as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();
    spark
        .sql(
            String.format(
                "alter table %s create index idx_zm_null_text using zonemap (text) with (rows_per_zone=5)",
                fullTable))
        .collect();
    checkIndex("idx_zm_null_text");

    try (org.lance.Dataset lds = openLance()) {
      // Pin the fragmentation: the test logic depends on the 5-NULL block aligning with one
      // rows_per_zone=5 zone. If Spark ever splits the coalesce(1) partition into multiple
      // fragments, the alignment would break and the all-NULL-zone assertion below could pass
      // by accident for the wrong reason.
      Assertions.assertEquals(
          1,
          lds.getFragments().size(),
          "Test fixture expects coalesce(1) to produce exactly one fragment");

      List<org.lance.index.scalar.ZoneStats> stats = lds.getZonemapStats("text");
      Assertions.assertFalse(stats.isEmpty(), "Expected at least one zone for the indexed column");

      boolean sawAllNullZone = false;
      boolean sawNonNullZone = false;
      for (org.lance.index.scalar.ZoneStats z : stats) {
        if (z.getNullCount() == z.getZoneLength()) {
          // All-NULL zone: min/max must be null; this is the codec's contract.
          Assertions.assertNull(z.getMin(), "All-NULL zone must have null min");
          Assertions.assertNull(z.getMax(), "All-NULL zone must have null max");
          sawAllNullZone = true;
        } else if (z.getNullCount() == 0) {
          // No-NULL zone: min/max must be present non-null Strings.
          Assertions.assertNotNull(z.getMin(), "Non-NULL zone must have a min");
          Assertions.assertNotNull(z.getMax(), "Non-NULL zone must have a max");
          Assertions.assertTrue(z.getMin() instanceof String, "min must be String");
          Assertions.assertTrue(z.getMax() instanceof String, "max must be String");
          sawNonNullZone = true;
        } else {
          // Mixed-null zone (some nulls, some non-nulls). The fixture is designed so the
          // 5-NULL block aligns with one rows_per_zone=5 zone — every zone should be either
          // all-NULL or no-NULL. A mixed-null zone means the alignment broke (Spark
          // partitioning drift, row ordering change, or codec regression), and the
          // sawAllNullZone/sawNonNullZone flags would otherwise both stay false and the test
          // would pass vacuously. Fail loudly with the zone shape.
          Assertions.fail(
              String.format(
                  "Unexpected mixed-null zone: nullCount=%d, zoneLength=%d, "
                      + "fragmentId=%d, zoneStart=%d — test fixture expects zone boundaries to "
                      + "align with the 5-NULL/5-non-NULL split, but didn't",
                  z.getNullCount(), z.getZoneLength(), z.getFragmentId(), z.getZoneStart()));
        }
      }
      Assertions.assertTrue(sawAllNullZone, "Test fixture must produce at least one all-NULL zone");
      Assertions.assertTrue(sawNonNullZone, "Test fixture must produce at least one non-NULL zone");
    }
  }

  @Test
  public void testCreateZonemapOnSingleFragmentTable() throws Exception {
    // The smallest non-trivial case for distributed-multi-segment: a one-fragment table must
    // still commit cleanly with exactly one segment under the index name. Catches a regression
    // where the runZonemapDistributed code path mishandled the N=1 degenerate edge (e.g. a
    // refactor that special-cased "more than one task" and skipped the commit on N=1).
    //
    // We must use DataFrame.coalesce(1) rather than SQL INSERT VALUES because the latter
    // partitions per VALUES tuple under `local[10]` and produces N single-row fragments.
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id", "cast(concat('t_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index idx_zm_single using zonemap (id)", fullTable));
    long fragmentsIndexed = result.collectAsList().get(0).getLong(0);
    Assertions.assertEquals(1L, fragmentsIndexed, "Expected one fragment indexed for N=1 case");

    List<Index> segments = indexesByName("idx_zm_single");
    Assertions.assertEquals(
        1, segments.size(), "N=1 fragment dataset must commit exactly one segment");
    Index segment = segments.get(0);
    Assertions.assertEquals(
        1,
        segment.fragments().orElseThrow().size(),
        "The single segment must cover exactly one fragment");
  }

  @Test
  public void testZonemapConsolidatedCommitShape() {
    // With spark.lance.zonemap.consolidate.enabled=true the build path must produce ONE
    // consolidated
    // IndexMetadata segment covering the full fragment set, instead of one segment per
    // fragment. This is the consumer-side integration of lance-core's
    // computeZonemapBatch + writeZonemapIndexFromBatches APIs (added in 7.0.0-beta.7):
    // executors return per-fragment Arrow batches, the driver merges them into a single
    // <uuid>/zonemap.lance file under one IndexMetadata entry.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    prepareDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index idx_zm_consolidated using zonemap (id)", fullTable));
    long fragmentsIndexed = result.collectAsList().get(0).getLong(0);

    Set<Integer> expectedFragments = new HashSet<>();
    try (org.lance.Dataset lds = openLance()) {
      for (org.lance.Fragment f : lds.getFragments()) {
        expectedFragments.add(f.getId());
      }
    }
    Assertions.assertEquals(
        expectedFragments.size(),
        (int) fragmentsIndexed,
        "fragmentsIndexed return value must match the dataset's current fragment count");
    // Without >=2 fragments the "single segment vs N segments" shape is indistinguishable
    // from the distributed path's N=1 degenerate case.
    Assertions.assertTrue(
        expectedFragments.size() >= 2,
        "Fixture must produce >= 2 fragments to verify consolidation; got "
            + expectedFragments.size());

    List<Index> segments = indexesByName("idx_zm_consolidated");
    Assertions.assertEquals(
        1,
        segments.size(),
        "Consolidated path must commit exactly ONE IndexMetadata segment regardless of "
            + "fragment count; got "
            + segments.size());

    Index segment = segments.get(0);
    Assertions.assertEquals(IndexType.ZONEMAP, segment.indexType());
    Assertions.assertTrue(
        segment.fragments().isPresent(), "Consolidated segment must carry a fragment list");
    Set<Integer> segmentFragments = new HashSet<>(segment.fragments().get());
    Assertions.assertEquals(
        expectedFragments,
        segmentFragments,
        "Consolidated segment's fragment list must equal the dataset's fragment set");
    Assertions.assertEquals(
        1,
        segment.fields().size(),
        "Indexing one column ('id') must produce a field-id list of length 1; got "
            + segment.fields());

    // The consolidated zonemap.lance must be readable end-to-end: every indexed fragment must
    // appear in getZonemapStats. A regression that wrote an empty or partial batch list
    // would still satisfy the IndexMetadata shape but fail this read-back coverage check.
    Set<Integer> coveredFragments =
        zonemapStats(ID_COLUMN).stream().map(ZoneStats::getFragmentId).collect(Collectors.toSet());
    Assertions.assertEquals(
        expectedFragments,
        coveredFragments,
        "getZonemapStats(id) must cover every indexed fragment from the consolidated segment");

    // Pin datasetVersion / indexVersion wire-through. A regression that defaulted these to 0
    // on the consolidated Index.builder (e.g. .datasetVersion(0L) instead of capturing the
    // live version, or .indexVersion(0) instead of writtenIndex.indexVersion()) passes every
    // other consolidated test because no other assertion touches these fields. The distributed
    // test only asserts cross-segment uniformity, which is vacuous on 1 segment.
    long postCommitVersion;
    try (org.lance.Dataset lds = openLance()) {
      postCommitVersion = lds.version();
    }
    Assertions.assertTrue(
        segment.datasetVersion() > 0L,
        "Consolidated segment must record a non-zero datasetVersion; got "
            + segment.datasetVersion());
    Assertions.assertTrue(
        segment.datasetVersion() <= postCommitVersion,
        "Consolidated segment's datasetVersion ("
            + segment.datasetVersion()
            + ") must not exceed the live dataset version ("
            + postCommitVersion
            + ") — recorded version came from the wrong manifest");
  }

  @Test
  public void testZonemapConsolidatedRepeatedCreate() {
    // Replace-semantics for the consolidated path: re-running CREATE INDEX under the same name
    // must mark the previous segment removed in `withRemovedIndices` and commit a fresh single
    // segment. A regression that dropped the removedIndices wiring would leave the first run's
    // IndexMetadata entry behind alongside the new one, doubling the manifest footprint.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    prepareDataset();

    spark
        .sql(
            String.format(
                "alter table %s create index idx_zm_consolidated_repeat using zonemap (id)",
                fullTable))
        .collect();
    List<Index> firstRun = indexesByName("idx_zm_consolidated_repeat");
    Assertions.assertEquals(1, firstRun.size(), "First run must commit one segment");
    UUID firstUuid = firstRun.get(0).uuid();

    spark
        .sql(
            String.format(
                "alter table %s create index idx_zm_consolidated_repeat using zonemap (id)",
                fullTable))
        .collect();
    List<Index> secondRun = indexesByName("idx_zm_consolidated_repeat");
    Assertions.assertEquals(
        1,
        secondRun.size(),
        "Second run under the same name must replace, not append — got "
            + secondRun.size()
            + " segments");
    Assertions.assertNotEquals(
        firstUuid,
        secondRun.get(0).uuid(),
        "Replace must produce a fresh UUID; got the same UUID across both runs, meaning the "
            + "consolidated write reused state from the previous run");

    // Verify the FIRST run's UUID is gone from the entire manifest (not just same-name).
    // A regression that dropped withRemovedIndices would leave the old IndexMetadata entry
    // alongside the new one — `indexesByName` filters by name and would not catch a
    // different-named orphan. Walking the full index list does.
    try (org.lance.Dataset lds = openLance()) {
      Set<UUID> liveUuids = lds.getIndexes().stream().map(Index::uuid).collect(Collectors.toSet());
      Assertions.assertFalse(
          liveUuids.contains(firstUuid),
          "First-run UUID must NOT remain in the live manifest after replace; "
              + "withRemovedIndices wiring is broken");
    }

    // Post-replace coverage: the new segment must cover the full dataset fragment set, not a
    // truncated subset. A regression where the consolidated write trimmed fragments silently
    // (e.g. dropped the last task's contribution) would still satisfy size==1 + fresh UUID.
    Set<Integer> expectedFragments = new HashSet<>();
    try (org.lance.Dataset lds = openLance()) {
      for (org.lance.Fragment f : lds.getFragments()) {
        expectedFragments.add(f.getId());
      }
    }
    Set<Integer> segmentFragments = new HashSet<>(secondRun.get(0).fragments().orElseThrow());
    Assertions.assertEquals(
        expectedFragments,
        segmentFragments,
        "Post-replace segment must cover the dataset's full fragment set; got "
            + segmentFragments
            + " vs expected "
            + expectedFragments);

    // Read-back: the manifest entry's fragment list is built from the dispatcher's view,
    // which is tautological vs. what we just asserted. The independent ground truth is what
    // getZonemapStats actually returns — that's the read path's view of the on-disk
    // consolidated zonemap.lance. A regression where the manifest claims coverage the file
    // doesn't have would diverge here.
    Set<Integer> coveredFragments =
        zonemapStats(ID_COLUMN).stream().map(ZoneStats::getFragmentId).collect(Collectors.toSet());
    Assertions.assertEquals(
        expectedFragments,
        coveredFragments,
        "getZonemapStats coverage must match the dispatched fragment set after replace");
  }

  @Test
  public void testZonemapConsolidatedReplacesDistributed() {
    // Cross-path interop: a distributed-built zonemap (N segments) replaced by a consolidated
    // build under the same index name must commit exactly one segment AND mark all N old
    // segments as removed. A regression in `removedIndices` filtering by name would leave
    // some-or-all of the N old segments stranded as duplicate-named entries in the manifest.
    prepareDataset();
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "false");
    spark.sql(
        String.format(
            "alter table %s create index idx_zm_crosspath using zonemap (id)", fullTable));
    List<Index> distributedSegments = indexesByName("idx_zm_crosspath");
    Assertions.assertTrue(
        distributedSegments.size() >= 2,
        "Pre-condition: distributed build must produce multiple segments (fixture-dependent)");

    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(
        String.format(
            "alter table %s create index idx_zm_crosspath using zonemap (id)", fullTable));

    List<Index> afterConsolidate = indexesByName("idx_zm_crosspath");
    Assertions.assertEquals(
        1,
        afterConsolidate.size(),
        "Consolidated re-create must collapse the N distributed segments to one; got "
            + afterConsolidate.size());
    Set<UUID> oldUuids = distributedSegments.stream().map(Index::uuid).collect(Collectors.toSet());
    Assertions.assertFalse(
        oldUuids.contains(afterConsolidate.get(0).uuid()),
        "Replacement segment must have a fresh UUID, disjoint from the N distributed UUIDs");

    // Post-replace coverage: the consolidated segment must cover the union of fragments the
    // N distributed segments covered. A regression that committed a single-segment index over
    // a strict subset (truncated parallelize result, dropped task, etc.) satisfies size==1
    // and "fresh UUID" but breaks pruning silently.
    Set<Integer> expectedFragments = new HashSet<>();
    try (org.lance.Dataset lds = openLance()) {
      for (org.lance.Fragment f : lds.getFragments()) {
        expectedFragments.add(f.getId());
      }
    }
    Set<Integer> segmentFragments =
        new HashSet<>(afterConsolidate.get(0).fragments().orElseThrow());
    Assertions.assertEquals(
        expectedFragments,
        segmentFragments,
        "Consolidated replacement must cover every fragment the N distributed segments did");

    // The full manifest must drop ALL N old UUIDs, not just same-name ones. A regression that
    // filtered removedIndices incorrectly would leave orphans of a different name in place.
    try (org.lance.Dataset lds = openLance()) {
      Set<UUID> liveUuids = lds.getIndexes().stream().map(Index::uuid).collect(Collectors.toSet());
      Assertions.assertTrue(
          java.util.Collections.disjoint(liveUuids, oldUuids),
          "Distributed-build UUIDs must be fully removed from the manifest after consolidated "
              + "replace; got intersection: "
              + (liveUuids.stream().filter(oldUuids::contains).collect(Collectors.toSet())));
    }

    // Post-replace zonemap stats must be readable and cover the full fragment set — proves the
    // consolidated zonemap.lance is intact AND that the manifest's index→segment pointer is
    // wired up correctly (a stale pointer from the dropped segments would fail to load).
    Set<Integer> coveredFragments =
        zonemapStats(ID_COLUMN).stream().map(ZoneStats::getFragmentId).collect(Collectors.toSet());
    Assertions.assertEquals(
        expectedFragments,
        coveredFragments,
        "getZonemapStats must cover every fragment post-replace");
  }

  @Test
  public void testZonemapDistributedReplacesConsolidated() {
    // Reverse-interop direction: consolidated → distributed. Toggling the flag back off after
    // a consolidated build and re-creating the same name must remove the consolidated segment
    // and commit N distributed segments. A regression where the distributed path's
    // removedIndices filter missed the single consolidated entry (e.g. asserted N>0 entries
    // to remove) would leave the consolidated segment orphaned in the manifest.
    prepareDataset();
    // Fixture pin: prepareDataset() must produce >=2 fragments for this test to assert
    // anything meaningful about the distributed "multiple segments" shape. A future change
    // to the fixture (e.g. Spark version consolidating INSERT VALUES tuples) would turn this
    // test into a tautology where N=1 satisfies both the "consolidated" pre-condition and
    // the "distributed >= 2" post-condition.
    try (org.lance.Dataset lds = openLance()) {
      Assertions.assertTrue(
          lds.getFragments().size() >= 2,
          "Fixture must produce >= 2 fragments to distinguish consolidated (1 segment) from "
              + "distributed (N segments); got "
              + lds.getFragments().size());
    }
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(
        String.format("alter table %s create index idx_zm_reverse using zonemap (id)", fullTable));
    List<Index> consolidatedSegments = indexesByName("idx_zm_reverse");
    Assertions.assertEquals(
        1,
        consolidatedSegments.size(),
        "Pre-condition: consolidated build must produce exactly one segment");
    UUID consolidatedUuid = consolidatedSegments.get(0).uuid();

    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "false");
    spark.sql(
        String.format("alter table %s create index idx_zm_reverse using zonemap (id)", fullTable));

    List<Index> afterDistributed = indexesByName("idx_zm_reverse");
    Assertions.assertTrue(
        afterDistributed.size() >= 2,
        "Distributed replacement must produce multiple segments (one per fragment); got "
            + afterDistributed.size());
    Set<UUID> newUuids = afterDistributed.stream().map(Index::uuid).collect(Collectors.toSet());
    Assertions.assertFalse(
        newUuids.contains(consolidatedUuid),
        "Consolidated segment's UUID must NOT appear in the post-replace distributed segment "
            + "set");

    try (org.lance.Dataset lds = openLance()) {
      Set<UUID> liveUuids = lds.getIndexes().stream().map(Index::uuid).collect(Collectors.toSet());
      Assertions.assertFalse(
          liveUuids.contains(consolidatedUuid),
          "Consolidated UUID must be fully removed from the manifest after distributed "
              + "replace");
    }
  }

  @Test
  public void testZonemapConsolidateFlagDefaultsToDistributed() {
    // Pin the default behavior: without setting the flag, ZONEMAP CREATE INDEX must route
    // through the distributed path. A regression that flipped the default would silently
    // change every existing pipeline's commit footprint, and every other consolidated test
    // would still pass (they explicitly set the flag).
    Assertions.assertFalse(
        spark.conf().contains("spark.lance.zonemap.consolidate.enabled"),
        "Sanity: this test assumes the flag is unset on entry; AfterEach teardown is wrong "
            + "if this fails");
    prepareDataset();
    spark.sql(
        String.format("alter table %s create index idx_zm_default using zonemap (id)", fullTable));
    List<Index> segments = indexesByName("idx_zm_default");
    int fragmentCount;
    try (org.lance.Dataset lds = openLance()) {
      fragmentCount = lds.getFragments().size();
    }
    // Tight pin (mirror the explicit-false test): distributed produces one segment per
    // fragment. A regression where the default path produced some-but-not-N segments would
    // pass `>= 2` silently.
    Assertions.assertEquals(
        fragmentCount,
        segments.size(),
        "Default (flag unset) must produce one segment per fragment; got "
            + segments.size()
            + " segments over "
            + fragmentCount
            + " fragments");
  }

  @Test
  public void testZonemapConsolidateFlagFalseRoutesToDistributed() {
    // Representative negative case: explicit "false" must produce the distributed multi-
    // segment shape. Combined with testZonemapConsolidateFlagCaseInsensitive (positive "TRUE")
    // and testZonemapConsolidateFlagDefaultsToDistributed (unset), this triangulates the
    // parse semantics: only case-insensitive "true" (with trim) flips the path.
    prepareDataset();
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "false");
    spark.sql(
        String.format("alter table %s create index idx_zm_false using zonemap (id)", fullTable));
    List<Index> segments = indexesByName("idx_zm_false");
    // Tight pin: distributed produces ONE segment per fragment. Asserting equality (not
    // size >= 2) catches a regression where consolidated misroutes to a state that produces
    // some-but-not-N segments (e.g. 2 segments from a 20-fragment dataset).
    int fragmentCount;
    try (org.lance.Dataset lds = openLance()) {
      fragmentCount = lds.getFragments().size();
    }
    Assertions.assertEquals(
        fragmentCount,
        segments.size(),
        "spark.lance.zonemap.consolidate.enabled=false must route to distributed (one segment per "
            + "fragment); got "
            + segments.size()
            + " segments over "
            + fragmentCount
            + " fragments");
  }

  @Test
  public void testZonemapConsolidateFlagCaseInsensitive() {
    // The R1 fix switched parsing from `_.toBoolean` to `equalsIgnoreCase("true")` + trim.
    // Pin the lenient cases that R1 added support for: mixed case, leading/trailing whitespace,
    // and the canonical "true". A regression that reverted to strict `_.toBoolean` would fail
    // "TRUE" and " true "; one that broke trim() would fail " true ".
    prepareDataset();
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "  TRUE  ");
    spark.sql(
        String.format("alter table %s create index idx_zm_flag_ci using zonemap (id)", fullTable));
    List<Index> segments = indexesByName("idx_zm_flag_ci");
    Assertions.assertEquals(
        1,
        segments.size(),
        "Flag value '  TRUE  ' must route to the consolidated path (1 segment); got "
            + segments.size());
  }

  @Test
  public void testZonemapConsolidatedOnStringColumn() throws Exception {
    // String columns exercise the UTF-8 min/max codec end-to-end through Arrow IPC. Numeric
    // min/max in the existing test would not catch a regression where the string codec
    // misencodes empty validity buffers or mishandles variable-length byte payloads on the
    // ArrowStreamWriter → ArrowStreamReader round-trip.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 12)
        .selectExpr("cast(id as int) as id", "cast(concat('s_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();
    spark
        .sql(
            String.format(
                "alter table %s create index idx_zm_c_text using zonemap (text)", fullTable))
        .collect();
    List<Index> segments = indexesByName("idx_zm_c_text");
    Assertions.assertEquals(1, segments.size(), "Consolidated string index must be one segment");
    // Coverage check: a regression committing 1 segment over zero fragments satisfies size==1.
    Set<Integer> expectedFragments = new HashSet<>();
    try (org.lance.Dataset lds = openLance()) {
      for (org.lance.Fragment f : lds.getFragments()) {
        expectedFragments.add(f.getId());
      }
    }
    Set<Integer> segmentFragments = new HashSet<>(segments.get(0).fragments().orElseThrow());
    Assertions.assertEquals(
        expectedFragments,
        segmentFragments,
        "String consolidated segment must cover the dataset's full fragment set");
    List<ZoneStats> stats = zonemapStats("text");
    Assertions.assertFalse(stats.isEmpty(), "Zonemap stats for string column must be present");
    for (ZoneStats z : stats) {
      // Skip all-NULL zones if any; for this fixture there should be none.
      Assertions.assertTrue(
          z.getMin() instanceof String,
          "text zone min must be String; got "
              + (z.getMin() == null ? "null" : z.getMin().getClass()));
      Assertions.assertTrue(
          z.getMax() instanceof String,
          "text zone max must be String; got "
              + (z.getMax() == null ? "null" : z.getMax().getClass()));
    }
  }

  @Test
  public void testZonemapConsolidatedOnNullableStringColumn() throws Exception {
    // Verifies that the Arrow IPC round-trip preserves all-NULL zone semantics for the
    // consolidated path. A regression that mishandled empty/all-NULL validity buffers through
    // ArrowStreamWriter would either crash or produce non-null min/max for NULL data, neither
    // of which the testZonemapConsolidatedCommitShape numeric test would catch.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 10)
        .selectExpr(
            "cast(id as int) as id",
            "case when id < 5 then cast(concat('s_', id) as string) "
                + "else cast(null as string) end as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();
    spark
        .sql(
            String.format(
                "alter table %s create index idx_zm_c_null_text using zonemap (text) "
                    + "with (rows_per_zone=5)",
                fullTable))
        .collect();
    List<Index> segments = indexesByName("idx_zm_c_null_text");
    Assertions.assertEquals(1, segments.size(), "Single consolidated segment expected");

    List<ZoneStats> stats = zonemapStats("text");
    boolean sawAllNullZone = false;
    boolean sawNonNullZone = false;
    for (ZoneStats z : stats) {
      if (z.getNullCount() == z.getZoneLength()) {
        Assertions.assertNull(z.getMin(), "All-NULL zone must have null min");
        Assertions.assertNull(z.getMax(), "All-NULL zone must have null max");
        sawAllNullZone = true;
      } else if (z.getNullCount() == 0) {
        Assertions.assertTrue(z.getMin() instanceof String, "Non-NULL zone min must be String");
        Assertions.assertTrue(z.getMax() instanceof String, "Non-NULL zone max must be String");
        sawNonNullZone = true;
      } else {
        Assertions.fail(
            String.format(
                "Unexpected mixed-null zone: nullCount=%d, zoneLength=%d, "
                    + "fragmentId=%d, zoneStart=%d — fixture expects boundaries to align with "
                    + "the 5-NULL/5-non-NULL split",
                z.getNullCount(), z.getZoneLength(), z.getFragmentId(), z.getZoneStart()));
      }
    }
    Assertions.assertTrue(sawAllNullZone, "Fixture must produce at least one all-NULL zone");
    Assertions.assertTrue(sawNonNullZone, "Fixture must produce at least one non-NULL zone");
  }

  @Test
  public void testZonemapConsolidatedWithRowsPerZone() throws Exception {
    // The argsJson value the driver passes to writeZonemapIndexFromBatches MUST match what the
    // executors pass to computeZonemapBatch — a regression that re-serialized args at the
    // dispatcher boundary could diverge and produce a file whose recorded `rows_per_zone`
    // differs from the actual zone layout. This test pins the threading by counting zones.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 12)
        .selectExpr("cast(id as int) as id", "cast(concat('t_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();
    spark
        .sql(
            String.format(
                "alter table %s create index idx_zm_c_rpz using zonemap (id) "
                    + "with (rows_per_zone=4)",
                fullTable))
        .collect();
    List<Index> segments = indexesByName("idx_zm_c_rpz");
    Assertions.assertEquals(1, segments.size(), "Single consolidated segment expected");

    try (org.lance.Dataset lds = openLance()) {
      Assertions.assertEquals(
          1, lds.getFragments().size(), "Fixture expects coalesce(1) to produce one fragment");
      List<ZoneStats> stats = lds.getZonemapStats("id");
      Assertions.assertEquals(
          3,
          stats.size(),
          "rows_per_zone=4 over a 12-row fragment must produce 3 zones; got "
              + stats.size()
              + " — argsJson threading from driver to writeZonemapIndexFromBatches likely "
              + "broken");
      // Sort by zone_start so we can assert per-zone min/max correspond to the right id range.
      // Without this, a regression that aliased one fragment's stats array across all zones
      // (e.g. argsJson reused stale state in writeZonemapIndexFromBatches) produces 3 zones
      // of length 4 with IDENTICAL min/max — every count/length assertion still passes.
      List<ZoneStats> sorted =
          stats.stream()
              .sorted((a, b) -> Long.compare(a.getZoneStart(), b.getZoneStart()))
              .collect(Collectors.toList());
      for (int i = 0; i < 3; i++) {
        ZoneStats z = sorted.get(i);
        Assertions.assertEquals(
            4,
            z.getZoneLength(),
            "Zone " + i + " must have length == rows_per_zone=4; got " + z.getZoneLength());
        // ids in [i*4, i*4+4): zone 0 → ids {0,1,2,3} min=0 max=3, etc.
        int expectedMin = i * 4;
        int expectedMax = i * 4 + 3;
        Number actualMin = (Number) z.getMin();
        Number actualMax = (Number) z.getMax();
        Assertions.assertEquals(
            expectedMin,
            actualMin.intValue(),
            "Zone "
                + i
                + " (zone_start="
                + z.getZoneStart()
                + ") min must be "
                + expectedMin
                + " (the first id in ["
                + expectedMin
                + ","
                + (expectedMax + 1)
                + "));"
                + " got "
                + actualMin
                + " — stale-stats aliasing across zones");
        Assertions.assertEquals(
            expectedMax,
            actualMax.intValue(),
            "Zone "
                + i
                + " (zone_start="
                + z.getZoneStart()
                + ") max must be "
                + expectedMax
                + "; got "
                + actualMax);
      }
    }
  }

  @Test
  public void testZonemapConsolidatedOnSingleFragmentTable() throws Exception {
    // Degenerate N=1 case for the consolidated path. The dispatcher already guards `isEmpty`,
    // but N=1 has no test coverage and a regression that special-cased "multiple fragments"
    // could silently break this path.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id", "cast(concat('t_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create index idx_zm_c_single using zonemap (id)", fullTable));
    long fragmentsIndexed = result.collectAsList().get(0).getLong(0);
    Assertions.assertEquals(1L, fragmentsIndexed, "Expected one fragment indexed for N=1 case");

    List<Index> segments = indexesByName("idx_zm_c_single");
    Assertions.assertEquals(
        1, segments.size(), "Consolidated path on N=1 must commit exactly one segment");
    Assertions.assertEquals(
        1,
        segments.get(0).fragments().orElseThrow().size(),
        "Segment must cover exactly one fragment");

    // Read-back via getZonemapStats: confirms the on-disk consolidated zonemap.lance is
    // wired up correctly on the degenerate N=1 path. Without this, a regression that wrote
    // an empty file but committed a structurally-valid IndexMetadata entry passes the
    // segment-count / fragment-count assertions above silently.
    Set<Integer> coveredFragments =
        zonemapStats(ID_COLUMN).stream().map(ZoneStats::getFragmentId).collect(Collectors.toSet());
    Assertions.assertEquals(
        new HashSet<>(segments.get(0).fragments().orElseThrow()),
        coveredFragments,
        "getZonemapStats on N=1 consolidated must match the segment's fragment list");
  }

  @Test
  public void testTwoCoexistingConsolidatedZonemapIndexes() throws Exception {
    // Two differently-named consolidated indexes on different columns of the same dataset must
    // commit with disjoint UUIDs. Each indexes a different column → writeZonemapIndexFromBatches
    // is invoked twice with different arguments, exercising the cross-name isolation that the
    // read path's load_indices_by_name grouping depends on.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark
        .range(0, 20)
        .selectExpr("cast(id as int) as id", "cast(concat('t_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();
    spark.sql(
        String.format("alter table %s create index idx_zm_c_id using zonemap (id)", fullTable));
    spark.sql(
        String.format("alter table %s create index idx_zm_c_text using zonemap (text)", fullTable));

    List<Index> idSegments = indexesByName("idx_zm_c_id");
    List<Index> textSegments = indexesByName("idx_zm_c_text");
    Assertions.assertEquals(1, idSegments.size(), "id consolidated index must be one segment");
    Assertions.assertEquals(1, textSegments.size(), "text consolidated index must be one segment");
    Assertions.assertNotEquals(
        idSegments.get(0).uuid(),
        textSegments.get(0).uuid(),
        "Two consolidated indexes on different columns must have disjoint UUIDs");

    // Both names must survive in a single manifest snapshot taken AFTER both commits.
    // `indexesByName` re-opens the dataset on each call; calling it twice could mask a
    // regression where the second commit dropped the first index from the manifest, because
    // each lookup walks the live (post-second-commit) state independently. Snapshotting once
    // ensures we're asserting about a single consistent view of the manifest.
    try (org.lance.Dataset lds = openLance()) {
      Set<String> liveNames =
          lds.getIndexes().stream().map(Index::name).collect(Collectors.toSet());
      Assertions.assertTrue(
          liveNames.contains("idx_zm_c_id"),
          "id index must remain in manifest after text-index commit; live names: " + liveNames);
      Assertions.assertTrue(
          liveNames.contains("idx_zm_c_text"),
          "text index must remain in manifest after both commits; live names: " + liveNames);
    }

    // Stats for both columns must be readable independently after both commits.
    try (org.lance.Dataset lds = openLance()) {
      List<ZoneStats> idStats = lds.getZonemapStats("id");
      List<ZoneStats> textStats = lds.getZonemapStats("text");
      Assertions.assertFalse(idStats.isEmpty(), "id zonemap stats must be readable");
      Assertions.assertFalse(textStats.isEmpty(), "text zonemap stats must be readable");
      for (ZoneStats z : idStats) {
        Assertions.assertTrue(
            z.getMin() instanceof Number,
            "id zone min must be Number; got "
                + (z.getMin() == null ? "null" : z.getMin().getClass()));
      }
      for (ZoneStats z : textStats) {
        Assertions.assertTrue(
            z.getMin() instanceof String,
            "text zone min must be String; got "
                + (z.getMin() == null ? "null" : z.getMin().getClass()));
      }
    }
  }

  @Test
  public void testZonemapConsolidatedOnLongColumn() throws Exception {
    // The Int32 codec path used by the other consolidated tests is NOT representative of
    // the production target — at sf=100 store_sales the indexed column is `ss_sold_date_sk`,
    // a BIGINT. Int and Long round-trip through different Arrow IPC paths
    // (IntVector vs BigIntVector); a regression that mishandled BigIntVector through
    // ArrowStreamWriter/Reader (e.g. byte-order or buffer-length) would not be caught by the
    // Int-column tests.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    spark.sql(String.format("create table %s (id bigint, text string) using lance;", fullTable));
    spark
        .range(0L, 12L)
        .selectExpr("id as id", "cast(concat('t_', id) as string) as text")
        .coalesce(1)
        .writeTo(fullTable)
        .append();

    spark
        .sql(
            String.format(
                "alter table %s create index idx_zm_c_long using zonemap (id)", fullTable))
        .collect();
    List<Index> segments = indexesByName("idx_zm_c_long");
    Assertions.assertEquals(1, segments.size(), "Consolidated bigint index must be one segment");
    // Coverage check: a regression that committed 1 segment over zero fragments would still
    // satisfy size==1. Verify the segment's fragments() set matches the dataset's.
    Set<Integer> expectedFragments = new HashSet<>();
    try (org.lance.Dataset lds = openLance()) {
      for (org.lance.Fragment f : lds.getFragments()) {
        expectedFragments.add(f.getId());
      }
    }
    Set<Integer> segmentFragments = new HashSet<>(segments.get(0).fragments().orElseThrow());
    Assertions.assertEquals(
        expectedFragments,
        segmentFragments,
        "Bigint consolidated segment must cover the dataset's full fragment set");

    List<ZoneStats> stats = zonemapStats(ID_COLUMN);
    Assertions.assertFalse(stats.isEmpty(), "Zonemap stats for bigint column must be present");
    for (ZoneStats z : stats) {
      Assertions.assertTrue(
          z.getMin() instanceof Number,
          "bigint zone min must be Number; got "
              + (z.getMin() == null ? "null" : z.getMin().getClass()));
      long min = ((Number) z.getMin()).longValue();
      long max = ((Number) z.getMax()).longValue();
      Assertions.assertTrue(
          min >= 0L && max <= 11L,
          "bigint zone min/max must be inside the [0, 11] id range; got [" + min + "," + max + "]");
    }
  }

  @Test
  public void testZonemapConsolidatedIndexIsQueryable() {
    // End-to-end read-back: confirm a SELECT against the indexed column returns correct rows
    // after a consolidated CREATE INDEX. Manifest entry + zonemap stats round-trip prove the
    // index is structurally sound, but not that it's actually USABLE by the read path's
    // pruning/scan logic. A regression that left the manifest-to-file pointer wrong (or the
    // file in an unreadable state post-write) would pass every other consolidated test but
    // break query execution. Mirrors testCreateIndexDistributed's read-back check.
    spark.conf().set("spark.lance.zonemap.consolidate.enabled", "true");
    prepareDataset();
    spark.sql(
        String.format(
            "alter table %s create index idx_zm_c_queryable using zonemap (id)", fullTable));

    Dataset<Row> result = spark.sql(String.format("select * from %s where id = 5", fullTable));
    Assertions.assertEquals(
        1L, result.count(), "SELECT WHERE id=5 must return exactly one row post-CREATE-INDEX");
    Row row = result.collectAsList().get(0);
    Assertions.assertEquals(5, row.getInt(0), "Returned row's id must be 5");
    Assertions.assertEquals("text_5", row.getString(1), "Returned row's text must be 'text_5'");
  }

  /** Open the test table as a Lance dataset; caller is responsible for closing. */
  private org.lance.Dataset openLance() {
    return org.lance.Dataset.open().uri(tableDir).build();
  }

  private List<ZoneStats> zonemapStats(String column) {
    org.lance.Dataset ds = openLance();
    try {
      return ds.getZonemapStats(column);
    } finally {
      ds.close();
    }
  }

  private List<Index> indexesByName(String name) {
    org.lance.Dataset ds = openLance();
    try {
      return ds.getIndexes().stream()
          .filter(i -> name.equals(i.name()))
          .collect(Collectors.toList());
    } finally {
      ds.close();
    }
  }
}
