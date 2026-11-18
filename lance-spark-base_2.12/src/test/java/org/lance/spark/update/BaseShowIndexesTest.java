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
import org.lance.index.DistanceType;
import org.lance.index.Index;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.index.vector.VectorIndexParams;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Base test for SHOW INDEXES command. */
public abstract class BaseShowIndexesTest {
  protected String catalogName = "lance_test";
  protected String tableName = "show_indexes_test";
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
            .appName("lance-show-indexes-test")
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
    this.tableName = "show_indexes_test_" + UUID.randomUUID().toString().replace("-", "");
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
  public void testShowIndexesUsesMetadataWithoutOpeningScalarIndexFiles() throws IOException {
    prepareDataset();
    int fragmentCount;
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      fragmentCount = dataset.getFragments().size();
      for (String type : List.of("BTREE", "BITMAP", "ZONEMAP", "INVERTED", "NGRAM")) {
        String field = type.equals("INVERTED") || type.equals("NGRAM") ? "text" : "id";
        dataset.createIndex(
            List.of(field),
            IndexType.SCALAR,
            Optional.of(type.toLowerCase(Locale.ROOT)),
            IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create(type)).build(),
            false);
      }
    }
    long version;
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      version = dataset.version();
    }
    Files.move(Path.of(tableDir, "_indices"), Path.of(tableDir, "_hidden_indices"));

    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Assertions.assertEquals(5, rows.size());
    for (Row row : rows) {
      Assertions.assertEquals(row.getString(0), row.getString(2));
      Assertions.assertEquals(fragmentCount, row.getLong(3));
      Assertions.assertEquals(20L, row.getLong(4));
      Assertions.assertEquals(0L, row.getLong(5));
      Assertions.assertEquals(0L, row.getLong(6));
      Assertions.assertEquals(100.0, row.getDouble(7));
      Assertions.assertEquals(1L, row.getLong(8));
      Assertions.assertTrue(row.getLong(9) > 0L);
    }
    Assertions.assertEquals(
        rows, spark.sql(String.format("show indexes from %s", fullTable)).collectAsList());
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      Assertions.assertEquals(version, dataset.version());
    }
  }

  @Test
  public void testShowIndexesUsesMetadataAcrossPartialVectorSegments() throws IOException {
    spark.sql(
        String.format(
            "create table %s (id int, vector array<float>) using lance "
                + "tblproperties ('vector.arrow.fixed-size-list.size' = '4')",
            fullTable));
    for (int batch = 0; batch < 2; batch++) {
      spark.sql(
          String.format(
              "insert into %s select cast(id as int), "
                  + "transform(sequence(1, 4), element -> cast(id + element as float)) "
                  + "from range(%d, %d, 1, 1)",
              fullTable, batch * 32, (batch + 1) * 32));
    }
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      IndexParams params =
          IndexParams.builder()
              .setVectorIndexParams(VectorIndexParams.ivfFlat(1, DistanceType.L2))
              .build();
      List<Index> segments = new ArrayList<>();
      for (Fragment fragment : dataset.getFragments()) {
        segments.add(
            dataset.createIndex(
                IndexOptions.builder(List.of("vector"), IndexType.IVF_FLAT, params)
                    .withIndexName("vector_idx")
                    .withFragmentIds(List.of(fragment.getId()))
                    .build()));
      }
      dataset.commitExistingIndexSegments("vector_idx", "vector", segments);
      dataset.delete("id = 0");
    }
    spark.sql(String.format("insert into %s values (64, array(1F, 2F, 3F, 4F))", fullTable));
    Files.move(Path.of(tableDir, "_indices"), Path.of(tableDir, "_hidden_indices"));

    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Assertions.assertEquals(1, rows.size());
    Row row = rows.get(0);
    Assertions.assertEquals("vector_idx", row.getString(0));
    Assertions.assertEquals(List.of("vector"), row.getList(1));
    Assertions.assertEquals("ivf_flat", row.getString(2));
    Assertions.assertEquals(2L, row.getLong(3));
    Assertions.assertEquals(63L, row.getLong(4));
    Assertions.assertEquals(1L, row.getLong(5));
    Assertions.assertEquals(1L, row.getLong(6));
    Assertions.assertEquals(98.43, row.getDouble(7));
    Assertions.assertEquals(2L, row.getLong(8));
    Assertions.assertTrue(row.getLong(9) > 0L);
  }

  @Test
  public void testShowIndexesPreservesEmptyIndexValues() {
    spark.sql(String.format("create table %s (id int) using lance", fullTable));
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      dataset.createIndex(
          List.of("id"),
          IndexType.BTREE,
          Optional.of("empty_index"),
          IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("BTREE")).build(),
          false);
    }

    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Assertions.assertEquals(1, rows.size());
    Row row = rows.get(0);
    Assertions.assertEquals("btree", row.getString(2));
    for (int position = 3; position <= 6; position++) {
      Assertions.assertEquals(0L, row.getLong(position));
    }
    Assertions.assertTrue(row.isNullAt(7));
    Assertions.assertEquals(1L, row.getLong(8));
    Assertions.assertTrue(row.getLong(9) > 0L);
  }

  @Test
  public void testShowIndexesPreservesLegacyMigration() throws Exception {
    copyLegacyDataset();
    long version;
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      version = dataset.version();
      Assertions.assertEquals(544L, dataset.describeIndices().get(0).getRowsIndexed());
    }

    if ("false".equalsIgnoreCase(System.getenv("LANCE_AUTO_MIGRATION"))) {
      Exception error =
          Assertions.assertThrows(
              Exception.class,
              () -> spark.sql(String.format("show indexes from %s", fullTable)).collectAsList());
      Assertions.assertTrue(error.toString().contains("Overlap in indexed fragments"));
    } else {
      List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
      Assertions.assertEquals(1, rows.size());
      Row row = rows.get(0);
      Assertions.assertEquals("ivf_pq", row.getString(2));
      Assertions.assertEquals(288L, row.getLong(4));
      Assertions.assertEquals(0L, row.getLong(6));
      Assertions.assertEquals(100.0, row.getDouble(7));
      Assertions.assertEquals(2L, row.getLong(8));
      Assertions.assertTrue(row.isNullAt(9));
      List<Row> migratedRows =
          spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
      Assertions.assertEquals(288L, migratedRows.get(0).getLong(4));
      Assertions.assertEquals(0L, migratedRows.get(0).getLong(6));
      Assertions.assertTrue(migratedRows.get(0).getLong(9) > 0L);
      Assertions.assertEquals(
          migratedRows,
          spark.sql(String.format("show indexes from %s", fullTable)).collectAsList());
    }
    try (org.lance.Dataset latest =
        Utils.openDatasetBuilder(LanceSparkReadOptions.from(tableDir)).build()) {
      Assertions.assertEquals(
          "false".equalsIgnoreCase(System.getenv("LANCE_AUTO_MIGRATION")) ? version : version + 1,
          latest.version());
    }
  }

  @Test
  public void testShowIndexesPreservesMissingLegacyIndexBehavior() throws Exception {
    copyLegacyDataset();
    Files.move(Path.of(tableDir, "_indices"), Path.of(tableDir, "_hidden_indices"));
    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Assertions.assertTrue(rows.isEmpty());
  }

  private void copyLegacyDataset() throws Exception {
    Path fixture =
        Path.of(
            getClass()
                .getResource("/legacy_index_fragment_bitmap/bad_index_fragment_bitmap")
                .toURI());
    Path destination = Path.of(tableDir);
    try (Stream<Path> paths = Files.walk(fixture)) {
      for (Path source : paths.collect(Collectors.toList())) {
        Path target = destination.resolve(fixture.relativize(source));
        if (Files.isDirectory(source)) {
          Files.createDirectories(target);
        } else {
          Files.copy(source, target);
        }
      }
    }
  }

  @Test
  public void testShowIndexes() {
    prepareDataset();

    // Create a B-tree index on id
    spark.sql(String.format("alter table %s create index test_index using btree (id)", fullTable));

    Dataset<Row> result = spark.sql(String.format("show indexes from %s", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(name,StringType,true),StructField(fields,ArrayType(StringType,true),true),StructField(index_type,StringType,true),StructField(num_indexed_fragments,LongType,true),StructField(num_indexed_rows,LongType,true),StructField(num_unindexed_fragments,LongType,true),StructField(num_unindexed_rows,LongType,true),StructField(indexed_percent,DoubleType,true),StructField(num_segments,LongType,true),StructField(size_bytes,LongType,true))",
        result.schema().toString());

    List<Row> rows = result.collectAsList();
    Assertions.assertFalse(rows.isEmpty(), "Expected at least one index row");

    Row row = rows.get(0);

    // name should match created index
    Assertions.assertEquals("test_index", row.getString(0));

    // fields should contain column name "id"
    @SuppressWarnings("unchecked")
    List<String> fieldNames = row.getList(1);
    Assertions.assertTrue(fieldNames.contains("id"), "fields should contain column name 'id'");

    // index_type should be btree
    Assertions.assertEquals("btree", row.getString(2));

    // num_indexed_fragments should be at least 1
    long numIndexedFragments = row.getLong(3);
    Assertions.assertTrue(numIndexedFragments >= 1L, "num_indexed_fragments should be at least 1");

    // num_indexed_rows should be at least 1
    long numIndexedRows = row.getLong(4);
    Assertions.assertTrue(numIndexedRows >= 1L, "num_indexed_rows should be at least 1");

    Assertions.assertEquals(100.0d, row.getDouble(7), 1e-9, "indexed_percent should be 100");
    Assertions.assertTrue(row.getLong(8) >= 1L, "num_segments should be at least 1");
    Assertions.assertTrue(row.getLong(9) > 0L, "size_bytes should be positive");
  }

  @Test
  public void testShowIndexesAggregatesAcrossSegments() {
    spark.sql(String.format("create table %s (id int, name string) using lance", fullTable));
    for (int batch = 0; batch < 4; batch++) {
      spark.sql(
          String.format(
              "insert into %s values (%d, 'n%d'), (%d, 'n%d')",
              fullTable, batch * 2, batch * 2, batch * 2 + 1, batch * 2 + 1));
    }
    spark.sql(
        String.format(
            "alter table %s create index test_index using btree (id) with (num_segments = 2)",
            fullTable));

    long expectedSize = 0L;
    int segmentCount;
    try (org.lance.Dataset dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(tableDir).build())
            .build()) {
      List<Index> segments =
          dataset.getIndexes().stream()
              .filter(index -> "test_index".equals(index.name()))
              .collect(Collectors.toList());
      segmentCount = segments.size();
      for (Index segment : segments) {
        expectedSize += segment.getSizeBytes().orElse(0L);
      }
    }
    Assertions.assertEquals(
        2, segmentCount, "num_segments = 2 should have produced two physical segments");

    Row row = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList().get(0);

    Assertions.assertEquals(2L, row.getLong(8), "num_segments must count every physical segment");
    Assertions.assertEquals(
        expectedSize,
        row.getLong(9),
        "size_bytes must sum every segment's files, not just the first");
    Assertions.assertEquals(
        100.0d, row.getDouble(7), 1e-9, "a freshly built index covers every row");
  }

  @Test
  public void testShowIndexesReportsNullPercentForEmptyTable() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark.sql(String.format("alter table %s create index test_index using btree (id)", fullTable));

    Row row = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList().get(0);

    Assertions.assertTrue(row.isNullAt(7), "indexed_percent should be null for an empty table");
    Assertions.assertEquals(0L, row.getLong(4), "no rows can be indexed");
    Assertions.assertEquals(0L, row.getLong(6), "no rows can be unindexed");
  }

  @Test
  public void testShowIndexesFiltersMemWalIndex() {
    spark.sql(
        String.format(
            "create table %s (id int, region string) using lance "
                + "partitioned by (bucket(4, region))",
            fullTable));
    spark.sql(
        String.format(
            "insert into %s values (1, 'east'), (2, 'west'), (3, 'north'), (4, 'south')",
            fullTable));

    List<Row> systemOnly =
        spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();
    Assertions.assertTrue(systemOnly.isEmpty(), "MemWAL must not appear in SHOW INDEXES");

    spark.sql(String.format("alter table %s create index test_index using btree (id)", fullTable));
    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();

    Assertions.assertEquals(1, rows.size());
    Row row = rows.get(0);
    Assertions.assertEquals("test_index", row.getString(0));
    Assertions.assertEquals("btree", row.getString(2));
    Assertions.assertTrue(row.getLong(3) >= 1L);
    Assertions.assertTrue(row.getLong(4) >= 1L);
  }

  @Test
  public void testShowIndexesFiltersFragmentReuseIndex() {
    prepareDataset();

    // Use one index segment over all fragments so compaction rewrites indexed data. Distributed
    // CREATE INDEX produces per-task segments that the compaction planner keeps in separate bins.
    IndexParams indexParams =
        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("BTREE")).build();
    try (var dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(tableDir).build())
            .build()) {
      dataset.createIndex(
          IndexOptions.builder(List.of("id"), IndexType.BTREE, indexParams)
              .replace(true)
              .train(true)
              .withIndexName("test_index")
              .build());
    }

    spark.sql(
        String.format(
            "optimize %s with (target_rows_per_fragment=20000, defer_index_remap=true)",
            fullTable));

    try (var dataset =
        Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(tableDir).build())
            .build()) {
      Assertions.assertTrue(
          dataset.getIndexes().stream()
              .anyMatch(index -> "__lance_frag_reuse".equalsIgnoreCase(index.name())),
          "OPTIMIZE with deferred index remapping must create a fragment-reuse system index");
    }

    List<Row> rows = spark.sql(String.format("show indexes from %s", fullTable)).collectAsList();

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("test_index", rows.get(0).getString(0));
    Assertions.assertEquals("btree", rows.get(0).getString(2));
  }
}
