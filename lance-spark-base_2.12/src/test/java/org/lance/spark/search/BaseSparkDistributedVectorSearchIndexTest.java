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
import org.lance.index.vector.PQBuildParams;
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
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  void bypassVectorIndexPlansOnlyFallbackFragments() throws Exception {
    String table = createTable("idx_bypass", 4);
    List<Integer> fragments = fragmentIds(table);
    buildSegmentPerFragment(table, fragments);

    InputPartition[] parts = planPartitions(table, 10, null, null, null, true);
    assertEquals(fragments.size(), parts.length);
    for (InputPartition part : parts) {
      LanceDistributedSearchInputPartition searchPart = (LanceDistributedSearchInputPartition) part;
      assertTrue(searchPart.getIndexSegments().isEmpty());
      assertEquals(1, searchPart.getFragmentIds().size());
    }

    String sql = vectorSearchSql(table, 5, ", bypass_vector_index => true");
    assertEquals(java.util.Arrays.asList(0, 1, 2, 3, 4), ids(collect(sql, true)));
  }

  @Test
  void bypassVectorIndexConflictsWithFastSearch() throws Exception {
    String table = createTable("idx_bypass_fast", 1);
    String sql = vectorSearchSql(table, 5, ", bypass_vector_index => true, fast_search => true");
    for (boolean distributed : new boolean[] {true, false}) {
      Exception error = assertThrows(Exception.class, () -> collect(sql, distributed));
      assertTrue(rootMessage(error).contains("cannot both be true"), rootMessage(error));
    }
  }

  @Test
  void explicitMetricMismatchFallsBackInsteadOfForcingIndex() throws Exception {
    String table = createTable("idx_metric_mismatch", 4);
    List<Integer> fragments = fragmentIds(table);
    buildSegmentPerFragment(table, fragments, DistanceType.Cosine);

    InputPartition[] parts = planPartitions(table, 10, null, null, "l2", false);
    assertEquals(fragments.size(), parts.length);
    for (InputPartition part : parts) {
      assertTrue(((LanceDistributedSearchInputPartition) part).getIndexSegments().isEmpty());
    }

    Exception error =
        assertThrows(Exception.class, () -> planPartitions(table, 10, true, null, "l2", false));
    assertTrue(rootMessage(error).contains("fast_search cannot use"), rootMessage(error));
  }

  @Test
  void omittedMetricUsesCosineForIndexedAndFallbackUnits() throws Exception {
    String table = createAngularTable("idx_implicit_cosine");
    List<Integer> fragments = fragmentIds(table);
    buildSegmentPerFragment(table, fragments.subList(0, fragments.size() - 1), DistanceType.Cosine);

    InputPartition[] parts = planPartitions(table, 3, null, null);
    for (InputPartition part : parts) {
      assertEquals(
          "cosine",
          ((LanceDistributedSearchInputPartition) part).getQuery().getDistanceType(),
          "the resolved index metric must be sent to indexed and fallback tasks");
    }

    String sql =
        "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), "
            + "k => 3, nprobes => "
            + NUM_PARTITIONS
            + ")";
    List<Row> reference = collect(sql, false);
    List<Row> distributed = collect(sql, true);
    assertEquals(ids(reference), ids(distributed));
    assertEquals(192, distributed.get(0).getInt(0), "nearest row is in the fallback fragment");
    for (int i = 0; i < reference.size(); i++) {
      assertEquals(reference.get(i).getFloat(1), distributed.get(i).getFloat(1), 1e-3f);
    }
  }

  @Test
  void plannedPartitionsReadTheVersionTheyWerePlannedAgainst() throws Exception {
    String table = createTable("idx_snapshot", 2);
    long plannedVersion;
    try (Dataset ds = openDataset(table)) {
      plannedVersion = ds.version();
    }

    InputPartition[] parts = planPartitionsForExecution(table, 3);
    assertTrue(parts.length > 0);
    for (InputPartition part : parts) {
      assertEquals(plannedVersion, pinnedVersionOf(part));
    }

    // DELETE, not INSERT: a new fragment would not be in the planned partition list at all, so a
    // worker that wrongly opened the latest version could still pass. Deleting changes the
    // visibility of a fragment these partitions already reference.
    spark.sql("DELETE FROM " + table + " WHERE id = 0");
    try (Dataset ds = openDataset(table)) {
      assertTrue(ds.version() > plannedVersion, "the delete must produce a new version");
    }
    assertFalse(
        ids(collect(vectorSearchSql(table, 3, ""), true)).contains(0),
        "a freshly planned search must not see the deleted row");

    // Round-trip through Java serialization the way Spark ships a partition to an executor, then
    // execute the pre-delete partitions: they must still read the pinned version.
    List<Integer> idsFromPlannedPartitions = new ArrayList<>();
    LanceDistributedSearchPartitionReaderFactory factory =
        new LanceDistributedSearchPartitionReaderFactory();
    for (InputPartition part : parts) {
      InputPartition shipped = roundTrip(part);
      assertEquals(plannedVersion, pinnedVersionOf(shipped), "the pinned ref must survive");
      try (PartitionReader<ColumnarBatch> reader = factory.createColumnarReader(shipped)) {
        while (reader.next()) {
          ColumnarBatch batch = reader.get();
          for (int row = 0; row < batch.numRows(); row++) {
            idsFromPlannedPartitions.add(batch.column(0).getInt(row));
          }
        }
      }
    }
    assertTrue(
        idsFromPlannedPartitions.contains(0),
        "the planned partitions pin the pre-delete version, so they still see id 0; got "
            + idsFromPlannedPartitions);
  }

  /**
   * Plans partitions that can be executed directly through the reader factory: no {@code columns}
   * projection, so the scan returns every field the partition schema declares.
   */
  private InputPartition[] planPartitionsForExecution(String table, int k) throws Exception {
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
            .vector(queryVector)
            .topK(k)
            .vectorColumn(VECTOR_COLUMN)
            .nprobes(NUM_PARTITIONS)
            .build();
    return new LanceDistributedSearchScan(lanceTable.schema(), query).planInputPartitions();
  }

  private static long pinnedVersionOf(InputPartition part) {
    return ((LanceDistributedSearchInputPartition) part)
        .getQuery()
        .getReadOptions()
        .getRef()
        .getVersionNumber()
        .get();
  }

  /** Mimics Spark shipping an InputPartition from the driver to an executor. */
  private static InputPartition roundTrip(InputPartition part) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(part);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (InputPartition) in.readObject();
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
  void plannerPropagatesCandidateKToEveryUnit() throws Exception {
    // Narrow claim: the planner hands every unit the same candidate count. That the merge is then
    // correct is a separate property, covered by
    // distributedMergeKeepsTopKConcentratedInOneUnit.
    String table = createTable("idx_candidate_k", 3);
    List<Integer> fragments = fragmentIds(table);
    buildSegmentPerFragment(table, fragments.subList(0, fragments.size() - 1));

    InputPartition[] parts = planPartitions(table, 7, null, null, null, false);
    assertEquals(fragments.size(), parts.length, "two indexed units plus one flat unit");
    for (InputPartition part : parts) {
      assertEquals(
          7,
          ((LanceDistributedSearchInputPartition) part).getQuery().getK(),
          "every unit asks for exactly the requested candidate count");
    }
  }

  @Test
  void distributedMergeKeepsTopKConcentratedInOneUnit() throws Exception {
    // createTable lays ids out by fragment (0-63, 64-127, 128-191) with vector = [id]*DIM, and the
    // query sits at the origin, so the whole global top k lives in the first fragment's segment
    // and every other segment's nearest row is far away. A unit that returned fewer than k rows
    // would therefore be filled in from the distant segments and the result would visibly change.
    // nprobes covers all IVF partitions, so the comparison is exact rather than recall-dependent.
    String table = createTable("idx_concentrated", 3);
    buildSegmentPerFragment(table, fragmentIds(table));

    InputPartition[] parts = planPartitions(table, 5, null, null);
    assertEquals(3, parts.length, "one indexed unit per segment");

    String sql = vectorSearchSql(table, 5, "");
    List<Integer> distributed = ids(collect(sql, true));
    assertEquals(
        java.util.Arrays.asList(0, 1, 2, 3, 4),
        distributed,
        "the merge must keep the top k that one unit contributed");
    assertEquals(ids(collect(sql, false)), distributed);
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
  void explicitPrefilterFalseIsRejectedByDistributedSearch() throws Exception {
    String table = createTable("idx_postfilter", 4);
    buildSegmentPerFragment(table, fragmentIds(table));

    String sql = vectorSearchSql(table, 5, ", filter => 'id % 2 = 0', prefilter => false");
    List<Integer> reference = ids(collect(sql, false));
    assertFalse(reference.isEmpty(), "prefilter=false remains supported by namespace execution");

    // With a filter present the "pick your semantics" check fires first and names both options,
    // which is the more useful message of the two.
    Exception error = assertThrows(Exception.class, () -> collect(sql, true));
    assertTrue(rootMessage(error).contains("requires prefilter=true"), rootMessage(error));

    // Without a filter, prefilter=false has nothing to act on, but it is still rejected rather
    // than quietly ignored.
    String unfiltered = vectorSearchSql(table, 5, ", prefilter => false");
    Exception unfilteredError = assertThrows(Exception.class, () -> collect(unfiltered, true));
    assertTrue(
        rootMessage(unfilteredError)
            .contains("Distributed VECTOR_SEARCH does not support prefilter=false"),
        rootMessage(unfilteredError));
  }

  @Test
  void indexedAndUnindexedTablesAgreeOnFilteredSearch() throws Exception {
    // Same data, same query, same filter - the only difference is whether a vector index
    // exists. Indexed units and fallback units must not disagree about filter semantics.
    String indexed = createTable("idx_semantics_indexed", 4);
    buildSegmentPerFragment(indexed, fragmentIds(indexed));
    String unindexed = createTable("idx_semantics_plain", 4);

    String filterArg = ", filter => 'id % 2 = 0', prefilter => true";
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

  @Test
  void dotDistanceAliasesMatchReference() {
    assertDistanceTypeOrdering("ip");
    assertDistanceTypeOrdering("inner_product");
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

  @Test
  void mixedPlanWithAQuantizedIndexReportsComparableDistances() throws Exception {
    // A quantized index reports the quantized distance, a flat unit the exact one. Ranking the two
    // against each other in the global merge compares incomparable numbers, so the plan has to
    // re-score the indexed side first. One segment keeps the candidate set identical to what the
    // namespace path searches, which makes the comparison row-for-row.
    String table = createTable("idx_pq_mixed", 4);
    List<Integer> fragments = fragmentIds(table);
    buildPqSegment(table, fragments.subList(0, fragments.size() - 1));

    InputPartition[] parts = planPartitions(table, 10, null, null);
    long indexed =
        java.util.Arrays.stream(parts)
            .filter(p -> !((LanceDistributedSearchInputPartition) p).getIndexSegments().isEmpty())
            .count();
    assertEquals(1, indexed, "one indexed unit");
    assertEquals(parts.length - 1, 1, "and one flat unit for the uncovered fragment");

    String sql = vectorSearchSql(table, 10, "");
    List<Row> distributed = collect(sql, true);
    List<Row> reference = collect(sql, false);
    assertEquals(ids(reference), ids(distributed), "a mixed quantized plan must match namespace");
    for (int i = 0; i < reference.size(); i++) {
      assertEquals(
          reference.get(i).getFloat(1),
          distributed.get(i).getFloat(1),
          1e-3f,
          "row " + i + " must report the namespace distance, not a quantized one");
    }
  }

  @Test
  void twoIndexesOnOneColumnPickTheOneNamespaceWouldPick() throws Exception {
    // describeIndices sorts by index name, lance-core takes the first index on the column in
    // creation order. With two metrics on one column the two orders select different indexes, and
    // "nearest" then means something different on each path.
    String table = CATALOG_NAME + ".default.idx_two_metrics";
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT NOT NULL, "
            + VECTOR_COLUMN
            + " ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '"
            + DIM
            + "')");
    // Even ids: magnitude 10 at a tiny angle - excellent cosine, poor L2.
    // Odd ids: magnitude 1 at ~45 degrees - poor cosine, excellent L2.
    spark.sql(
        "INSERT INTO "
            + table
            + " SELECT CAST(id AS INT), array("
            + "CAST(CASE WHEN id % 2 = 0 THEN 10 * cos(radians(id * 0.5)) "
            + "ELSE 1 * cos(radians(45 + id * 0.5)) END AS FLOAT), "
            + "CAST(CASE WHEN id % 2 = 0 THEN 10 * sin(radians(id * 0.5)) "
            + "ELSE 1 * sin(radians(45 + id * 0.5)) END AS FLOAT), "
            + "0.0, 0.0, 0.0, 0.0, 0.0, 0.0) FROM range(0, 128, 1, 1)");

    // vec_l2 first in creation order; a_cos first by name.
    for (Object[] spec :
        new Object[][] {{"vec_l2", DistanceType.L2}, {"a_cos", DistanceType.Cosine}}) {
      try (Dataset ds = openDataset(table)) {
        ds.createIndex(
            IndexOptions.builder(
                    Collections.singletonList(VECTOR_COLUMN),
                    IndexType.IVF_FLAT,
                    indexParams(ds, (DistanceType) spec[1]))
                .withIndexName((String) spec[0])
                .build());
      }
    }
    spark.sql("REFRESH TABLE " + table);
    try (Dataset ds = openDataset(table)) {
      assertEquals(
          java.util.Arrays.asList("vec_l2", "a_cos"),
          ds.listIndexes(),
          "sanity: creation order must differ from name order for this to test anything");
    }

    String sql =
        "SELECT id FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), k => 6, nprobes => "
            + NUM_PARTITIONS
            + ")";
    List<Integer> reference = ids(collect(sql, false));
    List<Integer> distributed = ids(collect(sql, true));
    assertEquals(
        reference, distributed, "both paths must search through the index created first (vec_l2)");
    for (Integer id : distributed) {
      assertEquals(1, id % 2, "vec_l2 is the L2 index, whose neighbours here are the odd ids");
    }
  }

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

  /** One IVF_PQ segment over the given fragments; PQ needs a precomputed codebook per segment. */
  private void buildPqSegment(String table, List<Integer> fragments) throws Exception {
    try (Dataset ds = openDataset(table)) {
      IvfBuildParams trainParams =
          new IvfBuildParams.Builder().setNumPartitions(NUM_PARTITIONS).setMaxIters(2).build();
      float[] centroids = VectorTrainer.trainIvfCentroids(ds, VECTOR_COLUMN, trainParams);
      IvfBuildParams ivfParams =
          new IvfBuildParams.Builder()
              .setNumPartitions(NUM_PARTITIONS)
              .setMaxIters(2)
              .setCentroids(centroids)
              .build();
      PQBuildParams pqTrain =
          new PQBuildParams.Builder().setNumSubVectors(4).setNumBits(8).setMaxIters(2).build();
      PQBuildParams pqParams =
          new PQBuildParams.Builder()
              .setNumSubVectors(4)
              .setNumBits(8)
              .setMaxIters(2)
              .setCodebook(VectorTrainer.trainPqCodebook(ds, VECTOR_COLUMN, pqTrain))
              .build();
      IndexParams params =
          IndexParams.builder()
              .setVectorIndexParams(
                  new VectorIndexParams.Builder(ivfParams)
                      .setDistanceType(DistanceType.L2)
                      .setPqParams(pqParams)
                      .build())
              .build();
      List<Index> segments = new ArrayList<>();
      segments.add(
          ds.createIndex(
              IndexOptions.builder(
                      Collections.singletonList(VECTOR_COLUMN), IndexType.IVF_PQ, params)
                  .withIndexName(INDEX_NAME)
                  .withFragmentIds(fragments)
                  .build()));
      ds.commitExistingIndexSegments(INDEX_NAME, VECTOR_COLUMN, segments);
    }
    spark.sql("REFRESH TABLE " + table);
  }

  private String createAngularTable(String name) {
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
    for (int fragment = 0; fragment < 4; fragment++) {
      int start = fragment * ROWS_PER_INSERT;
      int angle = fragment == 3 ? 10 : 20 + fragment * 15;
      int scale = fragment == 3 ? 100 : 1;
      spark.sql(
          "INSERT INTO "
              + fullName
              + " SELECT CAST(id AS INT), array("
              + "CAST("
              + scale
              + " * cos(radians(CASE WHEN id = "
              + start
              + " THEN "
              + angle
              + " ELSE 80 END)) AS FLOAT), "
              + "CAST("
              + scale
              + " * sin(radians(CASE WHEN id = "
              + start
              + " THEN "
              + angle
              + " ELSE 80 END)) AS FLOAT), "
              + "0.0, 0.0, 0.0, 0.0, 0.0, 0.0) FROM range("
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
    return indexParams(ds, DistanceType.L2);
  }

  private IndexParams indexParams(Dataset ds, DistanceType distanceType) {
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
            new VectorIndexParams.Builder(ivfParams).setDistanceType(distanceType).build())
        .build();
  }

  /** Builds one index segment per given fragment and commits them as a single named index. */
  private List<Index> buildSegmentPerFragment(String table, List<Integer> fragments)
      throws Exception {
    return buildSegmentPerFragment(table, fragments, DistanceType.L2);
  }

  private List<Index> buildSegmentPerFragment(
      String table, List<Integer> fragments, DistanceType distanceType) throws Exception {
    try (Dataset ds = openDataset(table)) {
      IndexParams params = indexParams(ds, distanceType);
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
    return planPartitions(table, k, fastSearch, filter, null, false);
  }

  private InputPartition[] planPartitions(
      String table,
      int k,
      Boolean fastSearch,
      String filter,
      String distanceType,
      boolean bypassVectorIndex)
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
            .distanceType(distanceType)
            .filter(filter)
            .fastSearch(fastSearch)
            .bypassVectorIndex(bypassVectorIndex)
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
    return "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
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
    return rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList());
  }

  private String rootMessage(Throwable throwable) {
    Throwable root = throwable;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return String.valueOf(root.getMessage());
  }
}
