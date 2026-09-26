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
package org.lance.spark.search;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.DistanceType;
import org.lance.index.Index;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.vector.IvfBuildParams;
import org.lance.index.vector.VectorIndexParams;
import org.lance.index.vector.VectorTrainer;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.search.LanceSearchQuery.SearchType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the indexed-unit path of distributed VECTOR_SEARCH: the planner must emit one Spark task
 * per vector index segment (plus one per unindexed fragment) and merge the per-unit top-k into a
 * globally correct result.
 *
 * <p>lance-spark has no {@code CREATE INDEX ... USING IVF_*} DDL yet (blocked on #605), so the
 * segments are built directly through the Lance Java API, the same way lance's own {@code
 * VectorIndexTest#testCreateIvfFlatIndexDistributively} does it.
 */
public abstract class BaseSparkDistributedVectorSearchIndexTest {
  private static final String CATALOG_NAME = "lance_dist_idx";
  private static final String INDEX_NAME = "vec_idx";
  private static final String VECTOR_COLUMN = "vector";
  private static final int DIM = 8;
  private static final int ROWS_PER_INSERT = 64;
  private static final int NUM_PARTITIONS = 2;

  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-distributed-vector-search-index-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  // ---------------------------------------------------------------- tests

  @Test
  void allFragmentsIndexedPlansOneUnitPerSegment() throws Exception {
    String table = createTable("idx_all", 4);
    List<Integer> fragments = fragmentIds(table);
    List<Index> segments = buildSegmentPerFragment(table, fragments);
    assertEquals(fragments.size(), segments.size());

    InputPartition[] parts = planPartitions(table, 10, null, null);
    assertEquals(
        fragments.size(), parts.length, "one task per index segment, no fallback tasks expected");
    for (InputPartition p : parts) {
      LanceDistributedSearchInputPartition sp = (LanceDistributedSearchInputPartition) p;
      assertEquals(1, sp.getIndexSegments().size(), "each unit scans exactly one segment");
      assertTrue(sp.getFragmentIds().isEmpty());
    }

    assertTopKMatchesReference(table, 10);
  }

  @Test
  void mixedIndexedAndUnindexedFragmentsCoverEveryRow() throws Exception {
    String table = createTable("idx_mixed", 4);
    List<Integer> fragments = fragmentIds(table);
    assertTrue(fragments.size() >= 3, "need at least three fragments, got " + fragments);
    List<Integer> indexed = fragments.subList(0, fragments.size() - 1);
    List<Integer> unindexed = fragments.subList(fragments.size() - 1, fragments.size());
    buildSegmentPerFragment(table, indexed);

    InputPartition[] parts = planPartitions(table, 10, null, null);
    long indexedUnits =
        java.util.Arrays.stream(parts)
            .map(LanceDistributedSearchInputPartition.class::cast)
            .filter(p -> !p.getIndexSegments().isEmpty())
            .count();
    long fallbackUnits = parts.length - indexedUnits;
    assertEquals(indexed.size(), indexedUnits, "one unit per segment");
    assertEquals(unindexed.size(), fallbackUnits, "one fallback unit per unindexed fragment");

    assertTopKMatchesReference(table, 10);
  }

  @Test
  void fastSearchSkipsUnindexedFragments() throws Exception {
    String table = createTable("idx_fast", 4);
    List<Integer> fragments = fragmentIds(table);
    List<Integer> indexed = fragments.subList(0, fragments.size() - 1);
    buildSegmentPerFragment(table, indexed);

    InputPartition[] parts = planPartitions(table, 10, Boolean.TRUE, null);
    assertEquals(indexed.size(), parts.length, "fast_search must not plan fallback units");
    for (InputPartition p : parts) {
      assertTrue(((LanceDistributedSearchInputPartition) p).getFragmentIds().isEmpty());
    }
  }

  @Test
  void wholeTableIndexIsActuallyUsedByThePlanner() throws Exception {
    String table = createTable("idx_whole", 4);
    // A regular, non-segmented index over the whole table - what a normal CREATE INDEX produces.
    try (Dataset ds = openDataset(table)) {
      ds.createIndex(
          IndexOptions.builder(
                  Collections.singletonList(VECTOR_COLUMN), IndexType.IVF_FLAT, indexParams(ds))
              .withIndexName(INDEX_NAME)
              .replace(true)
              .build());
      assertTrue(ds.listIndexes().contains(INDEX_NAME));
    }
    spark.sql("REFRESH TABLE " + table);

    InputPartition[] parts = planPartitions(table, 10, null, null);
    List<LanceDistributedSearchInputPartition> units =
        java.util.Arrays.stream(parts)
            .map(LanceDistributedSearchInputPartition.class::cast)
            .collect(Collectors.toList());
    long indexedUnits = units.stream().filter(u -> !u.getIndexSegments().isEmpty()).count();
    assertFalse(
        indexedUnits == 0,
        "a whole-table vector index was silently ignored: the planner degraded to "
            + units.size()
            + " flat-KNN fallback units");

    assertTopKMatchesReference(table, 10);
  }

  @Test
  void indexedSearchWithOffsetMatchesReference() throws Exception {
    String table = createTable("idx_offset", 4);
    buildSegmentPerFragment(table, fragmentIds(table));

    String sql = vectorSearchSql(table, 5, ", offset => 3");
    List<Row> distributed = collect(sql, true);
    List<Row> reference = collect(sql, false);
    assertEquals(ids(reference), ids(distributed), "offset results must match the reference path");
  }

  @Test
  void indexedSearchWithPrefilterMatchesReference() throws Exception {
    String table = createTable("idx_prefilter", 4);
    buildSegmentPerFragment(table, fragmentIds(table));

    String sql = vectorSearchSql(table, 5, ", filter => 'id % 2 = 0', prefilter => true");
    List<Integer> distributed = ids(collect(sql, true));
    List<Integer> reference = ids(collect(sql, false));
    assertEquals(
        java.util.Arrays.asList(0, 2, 4, 6, 8), distributed, "prefilter must yield the true top-k");
    assertEquals(reference, distributed, "prefilter results must match the reference path");
  }

  @Test
  void indexedSearchAppliesFilterBeforeTopK() throws Exception {
    String table = createTable("idx_filter", 4);
    buildSegmentPerFragment(table, fragmentIds(table));

    String sql = vectorSearchSql(table, 5, ", filter => 'id % 2 = 0'");
    assertEquals(
        java.util.Arrays.asList(0, 2, 4, 6, 8),
        ids(collect(sql, true)),
        "a filtered distributed search must return the true filtered top-k");
  }

  @Test
  void indexedAndUnindexedTablesAgreeOnFilteredSearch() throws Exception {
    // Same data, same query, same filter - the only difference is whether a vector index
    // exists. Indexed units and fallback units must not disagree about filter semantics.
    String indexed = createTable("idx_semantics_indexed", 4);
    buildSegmentPerFragment(indexed, fragmentIds(indexed));
    String unindexed = createTable("idx_semantics_plain", 4);

    String filterArg = ", filter => 'id % 2 = 0'";
    List<Integer> indexedRows = ids(collect(vectorSearchSql(indexed, 5, filterArg), true));
    List<Integer> unindexedRows = ids(collect(vectorSearchSql(unindexed, 5, filterArg), true));
    assertEquals(java.util.Arrays.asList(0, 2, 4, 6, 8), unindexedRows, "unindexed table");
    assertEquals(unindexedRows, indexedRows, "an index must not change which rows match");
  }

  @Test
  void cosineDistanceOrderingMatchesReference() {
    assertDistanceTypeOrdering("cosine");
  }

  @Test
  void dotDistanceOrderingMatchesReference() {
    assertDistanceTypeOrdering("dot");
  }

  /**
   * Six single-row fragments whose vectors fan out by 15 degrees from the query direction, so the
   * correct ranking is id 0, 1, 2, ... for every distance type. Verifies the merged global sort on
   * {@code _distance} is not ordered the wrong way for cosine/dot.
   */
  private void assertDistanceTypeOrdering(String distanceType) {
    String table = CATALOG_NAME + ".default.dir_" + distanceType;
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT NOT NULL, "
            + VECTOR_COLUMN
            + " ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '"
            + DIM
            + "')");
    for (int i = 0; i < 6; i++) {
      double angle = Math.toRadians(15.0 * i);
      List<String> components = new ArrayList<>();
      components.add(String.valueOf((float) Math.cos(angle)));
      components.add(String.valueOf((float) Math.sin(angle)));
      for (int d = 2; d < DIM; d++) {
        components.add("0.0");
      }
      spark.sql(
          "INSERT INTO "
              + table
              + " VALUES ("
              + i
              + ", array("
              + String.join(", ", components)
              + "))");
    }

    List<String> unit = new ArrayList<>();
    unit.add("1.0");
    for (int d = 1; d < DIM; d++) {
      unit.add("0.0");
    }
    String sql =
        "SELECT id FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array("
            + String.join(", ", unit)
            + "), k => 3, distance_type => '"
            + distanceType
            + "')";
    List<Integer> distributed = ids(collect(sql, true));
    List<Integer> reference = ids(collect(sql, false));
    assertEquals(
        java.util.Arrays.asList(0, 1, 2),
        distributed,
        distanceType + ": merged order must put the closest vectors first");
    assertEquals(reference, distributed, distanceType + ": must match the reference path");
  }

  // ---------------------------------------------------------------- helpers

  private String createTable(String name, int inserts) {
    String fullName = CATALOG_NAME + ".default." + name;
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, "
            + VECTOR_COLUMN
            + " ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '"
            + DIM
            + "')");
    for (int i = 0; i < inserts; i++) {
      int start = i * ROWS_PER_INSERT;
      spark.sql(
          "INSERT INTO "
              + fullName
              + " SELECT CAST(id AS INT), "
              + "transform(sequence(1, "
              + DIM
              + "), x -> CAST(id AS FLOAT)) FROM range("
              + start
              + ", "
              + (start + ROWS_PER_INSERT)
              + ", 1, 1)");
    }
    return fullName;
  }

  /** Query vector sits at the origin, so the top-k by L2 is simply ids 0, 1, 2, ... */
  private String queryVectorSql() {
    List<String> zeros = new ArrayList<>();
    for (int i = 0; i < DIM; i++) {
      zeros.add("0.0");
    }
    return "array(" + String.join(", ", zeros) + ")";
  }

  private List<Integer> fragmentIds(String table) throws Exception {
    try (Dataset ds = openDataset(table)) {
      List<Integer> ids = new ArrayList<>();
      for (Fragment f : ds.getFragments()) {
        ids.add(f.getId());
      }
      Collections.sort(ids);
      return ids;
    }
  }

  private Dataset openDataset(String table) throws Exception {
    return Dataset.open().allocator(LanceRuntime.allocator()).uri(datasetUriOf(table)).build();
  }

  private String datasetUriOf(String table) throws Exception {
    return lanceTable(table).readOptions().getDatasetUri();
  }

  private LanceDataset lanceTable(String fullName) throws Exception {
    String name = fullName.substring(fullName.lastIndexOf('.') + 1);
    return (LanceDataset)
        ((TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG_NAME))
            .loadTable(Identifier.of(new String[] {"default"}, name));
  }

  private IndexParams indexParams(Dataset ds) {
    IvfBuildParams trainParams =
        new IvfBuildParams.Builder().setNumPartitions(NUM_PARTITIONS).setMaxIters(2).build();
    float[] centroids = VectorTrainer.trainIvfCentroids(ds, VECTOR_COLUMN, trainParams);
    IvfBuildParams ivfParams =
        new IvfBuildParams.Builder()
            .setNumPartitions(NUM_PARTITIONS)
            .setMaxIters(2)
            .setCentroids(centroids)
            .build();
    return IndexParams.builder()
        .setVectorIndexParams(
            new VectorIndexParams.Builder(ivfParams).setDistanceType(DistanceType.L2).build())
        .build();
  }

  /** Builds one index segment per given fragment and commits them as a single named index. */
  private List<Index> buildSegmentPerFragment(String table, List<Integer> fragments)
      throws Exception {
    try (Dataset ds = openDataset(table)) {
      IndexParams params = indexParams(ds);
      List<Index> segments = new ArrayList<>();
      for (Integer fragmentId : fragments) {
        segments.add(
            ds.createIndex(
                IndexOptions.builder(
                        Collections.singletonList(VECTOR_COLUMN), IndexType.IVF_FLAT, params)
                    .withIndexName(INDEX_NAME)
                    .withFragmentIds(Collections.singletonList(fragmentId))
                    .build()));
      }
      List<Index> committed = ds.commitExistingIndexSegments(INDEX_NAME, VECTOR_COLUMN, segments);
      assertTrue(ds.listIndexes().contains(INDEX_NAME), "index must be visible after commit");
      spark.sql("REFRESH TABLE " + table);
      return committed;
    }
  }

  /** Plans the distributed scan on the driver, without running a Spark job. */
  private InputPartition[] planPartitions(String table, int k, Boolean fastSearch, String filter)
      throws Exception {
    LanceDataset lanceTable = lanceTable(table);
    List<Float> queryVector = new ArrayList<>();
    for (int i = 0; i < DIM; i++) {
      queryVector.add(0.0f);
    }
    LanceSearchQuery query =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(lanceTable.readOptions().getTableId())
            .namespaceImpl(lanceTable.getNamespaceImpl())
            .namespaceProperties(lanceTable.getNamespaceProperties())
            .readOptions(lanceTable.readOptions())
            .initialStorageOptions(lanceTable.getInitialStorageOptions())
            .outputColumns(Collections.singletonList("id"))
            .vector(queryVector)
            .topK(k)
            .vectorColumn(VECTOR_COLUMN)
            .nprobes(NUM_PARTITIONS)
            .filter(filter)
            .fastSearch(fastSearch)
            .build();
    return new LanceDistributedSearchScan(lanceTable.schema(), query).planInputPartitions();
  }

  private void assertTopKMatchesReference(String table, int k) {
    String sql = vectorSearchSql(table, k, "");
    List<Integer> distributed = ids(collect(sql, true));
    List<Integer> reference = ids(collect(sql, false));
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < k; i++) {
      expected.add(i);
    }
    assertEquals(expected, reference, "sanity: reference path must return the k smallest ids");
    assertEquals(expected, distributed, "distributed path must return the k smallest ids");
  }

  private String vectorSearchSql(String table, int k, String extraNamedArgs) {
    return "SELECT id FROM VECTOR_SEARCH(table => '"
        + table
        + "', query_vector => "
        + queryVectorSql()
        + ", k => "
        + k
        + ", nprobes => "
        + NUM_PARTITIONS
        + extraNamedArgs
        + ")";
  }

  private List<Row> collect(String sql, boolean distributed) {
    spark.conf().set("spark.sql.lance.search.distributed.enabled", String.valueOf(distributed));
    return spark.sql(sql).collectAsList();
  }

  private List<Integer> ids(List<Row> rows) {
    return rows.stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
  }
}
