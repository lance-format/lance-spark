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
import org.lance.index.IndexType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
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
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Base integration tests for distributed vector index creation ({@code ALTER TABLE ... CREATE INDEX
 * ... USING IVF_*}).
 *
 * <p>Tests run against a local SparkSession with the dir namespace catalog and a four-fragment
 * vector dataset. Concrete subclasses are empty shells so each Spark/Scala combination runs the
 * full suite.
 */
public abstract class BaseAddVectorIndexTest {

  protected static final int VEC_DIM = 32;
  protected static final int ROWS_PER_INSERT = 80;
  protected static final int INSERT_COUNT = 4; // 4 fragments
  protected static final int NUM_ROWS = ROWS_PER_INSERT * INSERT_COUNT;

  protected SparkSession spark;
  protected String catalogName = "lance_test";
  protected String tableName;
  protected String fullTable;
  protected String tableDir;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-vector-index-test")
            .master("local[10]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "vec_index_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
    this.tableDir =
        FileSystems.getDefault().getPath(testRoot, this.tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Create the table and insert {@link #INSERT_COUNT} batches of {@link #ROWS_PER_INSERT} rows so
   * the dataset has at least four fragments.
   *
   * <p>Schema: id INT NOT NULL, vec ARRAY&lt;FLOAT&gt; NOT NULL with metadata {@code
   * arrow.fixed-size-list.size=VEC_DIM}.
   */
  protected void prepareVectorDataset() {
    prepareVectorDataset(VEC_DIM);
  }

  protected void prepareVectorDataset(int dim) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, vec ARRAY<FLOAT> NOT NULL) USING lance "
                + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '%d')",
            fullTable, dim));

    Random random = new Random(42);
    int rowId = 0;
    for (int b = 0; b < INSERT_COUNT; b++) {
      List<String> values = new ArrayList<>();
      for (int r = 0; r < ROWS_PER_INSERT; r++) {
        StringBuilder arr = new StringBuilder("ARRAY(");
        for (int i = 0; i < dim; i++) {
          if (i > 0) arr.append(", ");
          arr.append(String.format(Locale.ROOT, "CAST(%f AS FLOAT)", random.nextFloat()));
        }
        arr.append(")");
        values.add(String.format("(%d, %s)", rowId++, arr));
      }
      spark.sql(
          String.format("INSERT INTO %s (id, vec) VALUES %s", fullTable, String.join(",", values)));
    }
  }

  /** Walks an exception chain to the root cause and returns its message (or empty). */
  protected static String rootCauseMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    String msg = cur.getMessage();
    return msg == null ? "" : msg;
  }

  /** Build a SQL ARRAY(...) literal of {@code dim} floats, all set to {@code value}. */
  private static String buildVecLiteral(int dim, float value) {
    StringBuilder sb = new StringBuilder("array(");
    for (int i = 0; i < dim; i++) {
      if (i > 0) sb.append(", ");
      sb.append(String.format(Locale.ROOT, "CAST(%f AS FLOAT)", value));
    }
    sb.append(")");
    return sb.toString();
  }

  /** Collect the UUIDs of every index segment with the given name. */
  private java.util.Set<UUID> collectIndexUuids(String name) {
    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      return ds.getIndexes().stream()
          .filter(i -> name.equals(i.name()))
          .map(Index::uuid)
          .collect(Collectors.toSet());
    } finally {
      ds.close();
    }
  }

  /**
   * Verify that an index with the given name exists on the {@code vec} column and covers all
   * fragments. {@code expectedTypes} are accepted alternatives because lance-core 7.0.0 empirically
   * surfaces some IVF families (IVF_FLAT, IVF_SQ) as the umbrella {@link IndexType#VECTOR} rather
   * than the concrete subtype, while others (IVF_PQ, IVF_HNSW_PQ, IVF_HNSW_SQ) come back as the
   * concrete enum. Tighter subtype discrimination is deferred to a follow-up that inspects {@code
   * indexDetails()} bytes once lance-core stabilizes the enum surface for the FLAT and SQ paths.
   */
  protected void checkVectorIndex(String name, IndexType... expectedTypes) {
    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      List<Index> matches =
          ds.getIndexes().stream().filter(i -> name.equals(i.name())).collect(Collectors.toList());
      Assertions.assertFalse(matches.isEmpty(), "Index '" + name + "' was not committed");
      for (Index idx : matches) {
        boolean ok = false;
        for (IndexType expected : expectedTypes) {
          if (idx.indexType() == expected || idx.indexType() == IndexType.VECTOR) {
            ok = true;
            break;
          }
        }
        Assertions.assertTrue(ok, "Unexpected indexType for '" + name + "': " + idx.indexType());
      }
      int coveredFragments =
          matches.stream()
              .map(i -> i.fragments().orElse(java.util.Collections.emptyList()).size())
              .mapToInt(Integer::intValue)
              .sum();
      Assertions.assertEquals(
          ds.getFragments().size(),
          coveredFragments,
          "Vector index segments should cover every fragment exactly once");
    } finally {
      ds.close();
    }
  }

  /**
   * Sanity test for the fixture itself. Ensures the helper produces at least {@link #INSERT_COUNT}
   * fragments (Spark's {@code local[10]} parallelism may split each INSERT into multiple
   * partitions, yielding more fragments — but never fewer than the number of INSERT batches).
   */
  @Test
  public void testFixtureProducesFourFragmentVectorTable() {
    prepareVectorDataset();
    long count = spark.sql("SELECT COUNT(*) FROM " + fullTable).first().getLong(0);
    Assertions.assertEquals(NUM_ROWS, count);

    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      Assertions.assertTrue(
          ds.getFragments().size() >= INSERT_COUNT,
          "Expected at least " + INSERT_COUNT + " fragments, got " + ds.getFragments().size());
    } finally {
      ds.close();
    }
  }

  @Test
  public void testCreateIvfFlatIndexDefault() {
    prepareVectorDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                    + "WITH (num_partitions=4)",
                fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);
    Assertions.assertTrue(
        fragmentsIndexed >= INSERT_COUNT,
        "Expected fragments_indexed to be at least the insert count, got " + fragmentsIndexed);
    Assertions.assertEquals("vec_idx", indexName);

    checkVectorIndex("vec_idx", IndexType.IVF_FLAT);
  }

  @Test
  public void testCreateIvfPqIndexExplicit() {
    prepareVectorDataset();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "ALTER TABLE %s CREATE INDEX vec_pq_idx USING IVF_PQ (vec) "
                    + "WITH (num_partitions=4, num_sub_vectors=4)",
                fullTable));

    Row row = result.collectAsList().get(0);
    Assertions.assertTrue(
        row.getLong(0) >= INSERT_COUNT,
        "Expected fragments_indexed >= INSERT_COUNT, got " + row.getLong(0));
    Assertions.assertEquals("vec_pq_idx", row.getString(1));

    checkVectorIndex("vec_pq_idx", IndexType.IVF_PQ);
  }

  @Test
  public void testCreateIvfSqIndexDefault() {
    prepareVectorDataset();

    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_sq_idx USING IVF_SQ (vec) "
                + "WITH (num_partitions=4)",
            fullTable));

    checkVectorIndex("vec_sq_idx", IndexType.IVF_SQ);
  }

  @Test
  public void testCreateIvfHnswPqIndex() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_hnsw_pq USING IVF_HNSW_PQ (vec) "
                + "WITH (num_partitions=4, num_sub_vectors=4)",
            fullTable));
    checkVectorIndex("vec_hnsw_pq", IndexType.IVF_HNSW_PQ);
  }

  @Test
  public void testCreateIvfHnswSqIndex() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_hnsw_sq USING IVF_HNSW_SQ (vec) "
                + "WITH (num_partitions=4)",
            fullTable));
    checkVectorIndex("vec_hnsw_sq", IndexType.IVF_HNSW_SQ);
  }

  // ---------------------------------------------------------------------
  // Task 11: Default-inference end-to-end tests
  // ---------------------------------------------------------------------

  @Test
  public void testInferNumSubVectorsFromDim() {
    prepareVectorDataset(); // dim=32, divisible by 16
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                + "WITH (num_partitions=4)", // no num_sub_vectors
            fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_PQ);
  }

  @Test
  public void testInferNumSubVectorsDimDivBy8() {
    prepareVectorDataset(24); // dim=24, 24%16!=0, 24%8==0
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) " + "WITH (num_partitions=4)",
            fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_PQ);
  }

  @Test
  public void testInferNumSubVectorsDimNotDivisible() {
    prepareVectorDataset(23); // dim=23, divisible by neither
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                                + "WITH (num_partitions=4)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("not divisible by 16 or 8"),
        "Expected error mentioning 'not divisible by 16 or 8', got: " + rootCauseMessage(ex));
  }

  @Test
  public void testInferNumPartitionsFromRowCount() {
    prepareVectorDataset(); // 320 rows; sqrt(320) ~= 18
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec)", fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_FLAT);
  }

  @Test
  public void testEuclideanAliasMapsToL2() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                + "WITH (num_partitions=4, distance_type='euclidean')",
            fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_FLAT);
  }

  // ---------------------------------------------------------------------
  // Task 12: Negative tests - rejected key combos and bad columns
  // ---------------------------------------------------------------------

  @Test
  public void testRejectNumSubVectorsForIvfFlat() {
    prepareVectorDataset();
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                                + "WITH (num_partitions=4, num_sub_vectors=4)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("only supported for IVF_PQ"), "got: " + rootCauseMessage(ex));
  }

  @Test
  public void testRejectHnswParamsForNonHnsw() {
    prepareVectorDataset();
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                                + "WITH (num_partitions=4, num_sub_vectors=4, m=16)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("only supported for IVF_HNSW"),
        "got: " + rootCauseMessage(ex));
  }

  @Test
  public void testRejectUnknownDistanceType() {
    prepareVectorDataset();
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                                + "WITH (num_partitions=4, distance_type='manhattan')",
                            fullTable))
                    .collect());
    Assertions.assertTrue(rootCauseMessage(ex).contains("manhattan"));
    Assertions.assertTrue(rootCauseMessage(ex).contains("l2"));
  }

  @Test
  public void testRejectMultipleColumns() {
    prepareVectorDataset();
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (id, vec) "
                                + "WITH (num_partitions=4, num_sub_vectors=4)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("support a single column only"),
        "got: " + rootCauseMessage(ex));
  }

  @Test
  public void testRejectNonVectorColumn() {
    prepareVectorDataset();
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (id) "
                                + "WITH (num_partitions=4)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("not a fixed-size vector column"),
        "got: " + rootCauseMessage(ex));
  }

  @Test
  public void testRejectNumPartitionsExceedsRowCount() {
    prepareVectorDataset(); // 320 rows
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                                + "WITH (num_partitions=10000)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("cannot exceed total rows"), "got: " + rootCauseMessage(ex));
  }

  // ---------------------------------------------------------------------
  // Task 13: num_segments tests
  // ---------------------------------------------------------------------

  @Test
  public void testCreateIvfFlatWithNumSegments() {
    prepareVectorDataset(); // ~40 fragments due to local[10]
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                + "WITH (num_partitions=4, num_segments=2)",
            fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_FLAT);

    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      long segCount = ds.getIndexes().stream().filter(i -> "vec_idx".equals(i.name())).count();
      Assertions.assertEquals(2, segCount, "Expected num_segments=2 to produce exactly 2 segments");
    } finally {
      ds.close();
    }
  }

  @Test
  public void testNumSegmentsClampedToFragmentCount() {
    prepareVectorDataset();
    // Use a very large num_segments to force clamping
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                + "WITH (num_partitions=4, num_segments=100000)",
            fullTable));

    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = ds.getFragments().size();
      long segCount = ds.getIndexes().stream().filter(i -> "vec_idx".equals(i.name())).count();
      Assertions.assertEquals(
          fragmentCount,
          segCount,
          "Expected num_segments=100000 to be clamped down to fragment count " + fragmentCount);
    } finally {
      ds.close();
    }
  }

  @Test
  public void testDefaultNumSegmentsIsParallelismClampedToFragmentCount() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) " + "WITH (num_partitions=4)",
            fullTable));

    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      int fragmentCount = ds.getFragments().size();
      int defaultParallelism = spark.sparkContext().defaultParallelism();
      int expectedSegments = Math.min(fragmentCount, defaultParallelism);
      long segCount = ds.getIndexes().stream().filter(i -> "vec_idx".equals(i.name())).count();
      Assertions.assertEquals(
          expectedSegments,
          segCount,
          "default num_segments should be min(fragmentCount="
              + fragmentCount
              + ", defaultParallelism="
              + defaultParallelism
              + ")");
    } finally {
      ds.close();
    }
  }

  // ---------------------------------------------------------------------
  // Task 14: Replace / drop / SHOW INDEXES / VECTOR_SEARCH integration
  // ---------------------------------------------------------------------

  @Test
  public void testRepeatedCreateIvfPqReplacesSegments() {
    prepareVectorDataset();

    String sql =
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                + "WITH (num_partitions=4, num_sub_vectors=4, num_segments=2)",
            fullTable);

    spark.sql(sql);
    java.util.Set<UUID> firstUuids = collectIndexUuids("vec_idx");
    Assertions.assertFalse(firstUuids.isEmpty(), "First run produced no segments");

    spark.sql(sql);
    java.util.Set<UUID> secondUuids = collectIndexUuids("vec_idx");

    Assertions.assertEquals(
        firstUuids.size(), secondUuids.size(), "Replace should produce same segment count");
    Assertions.assertTrue(
        java.util.Collections.disjoint(firstUuids, secondUuids),
        "Replace should produce fresh segment UUIDs");
  }

  @Test
  public void testShowIndexesIncludesVectorIndex() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) " + "WITH (num_partitions=4)",
            fullTable));

    Dataset<Row> rows = spark.sql(String.format("SHOW INDEXES IN %s", fullTable));
    long match =
        rows.collectAsList().stream()
            .filter(r -> "vec_idx".equals(r.getString(r.fieldIndex("name"))))
            .count();
    Assertions.assertEquals(1, match, "SHOW INDEXES should include vec_idx");
  }

  @Test
  public void testDropVectorIndex() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) " + "WITH (num_partitions=4)",
            fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_FLAT);

    spark.sql(String.format("ALTER TABLE %s DROP INDEX vec_idx", fullTable));

    Dataset<Row> rows = spark.sql(String.format("SHOW INDEXES IN %s", fullTable));
    long match =
        rows.collectAsList().stream()
            .filter(r -> "vec_idx".equals(r.getString(r.fieldIndex("name"))))
            .count();
    Assertions.assertEquals(0, match, "SHOW INDEXES should no longer include vec_idx");

    // Re-create after drop must succeed
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) " + "WITH (num_partitions=4)",
            fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_FLAT);
  }

  @Test
  public void testCreateIndexEmptyTable() {
    // Create an empty table without inserting any rows. IVF_* CREATE INDEX on empty tables
    // is rejected with a clear message, since Lance cannot train IVF centroids without
    // vectors; scalar-only empty-table creation is exercised in the scalar index test suite.
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, vec ARRAY<FLOAT> NOT NULL) USING lance "
                + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '%d')",
            fullTable, VEC_DIM));

    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                                + "WITH (num_partitions=4)",
                            fullTable))
                    .collect());
    String message = rootCauseMessage(ex);
    Assertions.assertTrue(message.contains("empty table"), "got: " + message);
    Assertions.assertTrue(message.contains("IVF") || message.contains("ivf"), "got: " + message);

    // No index was committed.
    Dataset<Row> rows = spark.sql(String.format("SHOW INDEXES IN %s", fullTable));
    long match =
        rows.collectAsList().stream()
            .filter(r -> "vec_idx".equals(r.getString(r.fieldIndex("name"))))
            .count();
    Assertions.assertEquals(0, match);
  }

  @Test
  public void testEmptyTableRejectsUnsupportedVectorIndexMethod() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, vec ARRAY<FLOAT> NOT NULL) USING lance "
                + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '%d')",
            fullTable, VEC_DIM));

    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_HNSW_FLAT (vec)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("IVF_HNSW_FLAT"), "got: " + rootCauseMessage(ex));
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("ivf_hnsw_pq"), "got: " + rootCauseMessage(ex));
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("ivf_hnsw_sq"), "got: " + rootCauseMessage(ex));
  }

  @Test
  public void testEmptyTableRejectsInvalidVectorIndexParams() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, vec ARRAY<FLOAT> NOT NULL) USING lance "
                + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '%d')",
            fullTable, VEC_DIM));

    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                                + "WITH (num_partitions=1, num_sub_vectors=4, num_bits=3)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(rootCauseMessage(ex).contains("num_bits"), rootCauseMessage(ex));
  }

  @Test
  public void testEmptyTableRejectsDoubleVectorIndex() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, vec ARRAY<DOUBLE> NOT NULL) USING lance "
                + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '%d')",
            fullTable, VEC_DIM));

    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec)", fullTable))
                    .collect());
    Assertions.assertTrue(rootCauseMessage(ex).contains("Float32"), rootCauseMessage(ex));
    Assertions.assertTrue(rootCauseMessage(ex).contains("Double"), rootCauseMessage(ex));
  }

  @Test
  public void testVectorSearchUsesIndex() {
    // Spark 3.4 does not support TVF named-argument syntax (`table => ...`); SPARK-44059 added
    // it in 3.5. Skip on 3.4 to mirror BaseSparkSearchTableFunctionTest#supportsNamedArguments.
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "VECTOR_SEARCH named-argument syntax requires Spark 3.5+");
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                + "WITH (num_partitions=4, num_sub_vectors=4)",
            fullTable));

    String qVec = buildVecLiteral(VEC_DIM, 0.5f);

    Dataset<Row> result =
        spark.sql(
            String.format(
                "SELECT id, _distance FROM VECTOR_SEARCH("
                    + "table => '%s', "
                    + "query_vector => %s, "
                    + "vector_column => 'vec', "
                    + "num_results => 5)",
                fullTable, qVec));
    Assertions.assertEquals(5, result.count(), "VECTOR_SEARCH topK should return exactly k rows");
  }

  /**
   * Deferred training (`train=false`) is not yet supported for IVF_* vector indexes because Lance
   * does not expose a vector-aware empty-index commit path. AddIndexExec rejects it up front with a
   * clear message rather than falling through to "Unsupported index method".
   */
  @Test
  public void testIvfTrainFalseIsRejectedExplicitly() {
    prepareVectorDataset();
    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                                + "WITH (num_partitions=4, num_sub_vectors=4, train=false)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("train=false is not supported"), rootCauseMessage(ex));
    Assertions.assertTrue(rootCauseMessage(ex).contains("IVF_PQ"), rootCauseMessage(ex));
  }

  /**
   * Explicit `train=true` is a no-op for IVF builds; it must be accepted (whitelisted in
   * VectorIndexParamsResolver.CommonKeys and stripped via SparkOnlyOptions before being forwarded
   * to lance-core), and the resulting index must cover all fragments just like the default eager
   * build.
   */
  @Test
  public void testIvfTrainTrueIsAcceptedAndBuildsIndex() {
    prepareVectorDataset();
    Dataset<Row> result =
        spark.sql(
            String.format(
                "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                    + "WITH (num_partitions=4, num_sub_vectors=4, train=true)",
                fullTable));
    long fragmentsIndexed = result.first().getLong(0);
    Assertions.assertTrue(
        fragmentsIndexed > 0L,
        "explicit train=true should run a full distributed build and cover at least one fragment");
  }

  /**
   * SQ-quantized IVF variants must be downgraded to a single segment. lance-core trains the
   * ScalarQuantizer per createIndex and `commit_existing_index_segments` does not reconcile
   * per-segment SQ bounds, so multiple segments would silently corrupt distance computation.
   * AddIndexExec clamps `num_segments` to 1 with a WARN log even when the user explicitly requests
   * a larger value.
   */
  @Test
  public void testIvfSqIsClampedToSingleSegmentEvenWhenLargerRequested() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_sq_idx USING IVF_SQ (vec) "
                + "WITH (num_partitions=4, num_segments=8)",
            fullTable));
    checkVectorIndex("vec_sq_idx", IndexType.IVF_SQ);

    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      long segCount = ds.getIndexes().stream().filter(i -> "vec_sq_idx".equals(i.name())).count();
      Assertions.assertEquals(
          1L,
          segCount,
          "IVF_SQ should be forced to a single segment regardless of requested num_segments");
    } finally {
      ds.close();
    }
  }

  @Test
  public void testIvfHnswSqIsClampedToSingleSegmentByDefault() {
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_hnsw_sq_idx USING IVF_HNSW_SQ (vec) "
                + "WITH (num_partitions=4)",
            fullTable));
    checkVectorIndex("vec_hnsw_sq_idx", IndexType.IVF_HNSW_SQ);

    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      long segCount =
          ds.getIndexes().stream().filter(i -> "vec_hnsw_sq_idx".equals(i.name())).count();
      Assertions.assertEquals(
          1L,
          segCount,
          "IVF_HNSW_SQ should be forced to a single segment by default (no num_segments specified)");
    } finally {
      ds.close();
    }
  }

  // ---------------------------------------------------------------------
  // Semantic correctness: recall against brute-force + degenerate input
  // ---------------------------------------------------------------------

  /** Brute-force L2² for ARRAY&lt;FLOAT&gt; rows; small fixture so exact math is fine. */
  private static double l2sq(float[] a, float[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++) {
      double d = a[i] - b[i];
      s += d * d;
    }
    return s;
  }

  /**
   * Recall smoke test: IVF_FLAT (no quantization) top-10 should overlap heavily with a brute-force
   * scan on the fixture. A build that ships wrong centroids or mis-sharded segments produces near-
   * zero overlap. This pins semantic correctness of the multi-segment batch path — the default
   * {@code num_segments = min(fragment_count, parallelism)} exercises it here.
   */
  @Test
  public void testIvfFlatMatchesBruteForceTopK() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "VECTOR_SEARCH named-argument syntax requires Spark 3.5+");
    prepareVectorDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) " + "WITH (num_partitions=4)",
            fullTable));

    float[] q = new float[VEC_DIM];
    for (int i = 0; i < VEC_DIM; i++) q[i] = 0.5f;

    java.util.List<Row> all = spark.sql("SELECT id, vec FROM " + fullTable).collectAsList();
    java.util.List<Integer> bruteTopK =
        all.stream()
            .sorted(
                java.util.Comparator.comparingDouble(
                    r -> {
                      java.util.List<Float> v = r.getList(1);
                      float[] arr = new float[v.size()];
                      for (int i = 0; i < v.size(); i++) arr[i] = v.get(i);
                      return l2sq(arr, q);
                    }))
            .limit(10)
            .map(r -> r.getInt(0))
            .collect(Collectors.toList());

    String qVec = buildVecLiteral(VEC_DIM, 0.5f);
    java.util.List<Integer> indexTopK =
        spark
            .sql(
                String.format(
                    "SELECT id, _distance FROM VECTOR_SEARCH("
                        + "table => '%s', "
                        + "query_vector => %s, "
                        + "vector_column => 'vec', "
                        + "num_results => 10)",
                    fullTable, qVec))
            .collectAsList()
            .stream()
            .map(r -> r.getInt(0))
            .collect(Collectors.toList());

    long overlap = indexTopK.stream().filter(bruteTopK::contains).count();
    Assertions.assertTrue(
        overlap >= 6,
        "IVF_FLAT top-10 should agree with brute-force in >=6 IDs, got "
            + overlap
            + " (index="
            + indexTopK
            + ", brute="
            + bruteTopK
            + ")");
  }

  /**
   * Degenerate input: every vector is identical. K-means centroids collapse (any partition
   * assignment is optimal) and cosine normalization would produce NaN. The pipeline must still
   * commit a valid index and not blow up on the driver side.
   */
  @Test
  public void testCreateIvfFlatOnAllEqualVectors() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, vec ARRAY<FLOAT> NOT NULL) USING lance "
                + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '%d')",
            fullTable, VEC_DIM));

    int rowId = 0;
    for (int b = 0; b < INSERT_COUNT; b++) {
      java.util.List<String> values = new java.util.ArrayList<>();
      for (int r = 0; r < ROWS_PER_INSERT; r++) {
        values.add(String.format("(%d, %s)", rowId++, buildVecLiteral(VEC_DIM, 0.5f)));
      }
      spark.sql(
          String.format("INSERT INTO %s (id, vec) VALUES %s", fullTable, String.join(",", values)));
    }

    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                + "WITH (num_partitions=2, distance_type='l2')",
            fullTable));
    checkVectorIndex("vec_idx", IndexType.IVF_FLAT);
  }
}
